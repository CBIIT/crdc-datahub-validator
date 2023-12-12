#!/usr/bin/env python3

import pandas as pd
import json
import os
from botocore.exceptions import ClientError
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import DB, BATCH_TYPE_METADATA, DATA_COMMON_NAME, ERRORS, DB, \
    ERRORS, SQS_NAME, SCOPE, MODEL, SUBMISSION_ID, ERRORS, WARNINGS, STATUS_ERROR, \
    STATUS_WARNING, STATUS_PASSED, FILE_STATUS, UPDATED_AT
from common.utils import current_datetime_str, get_exception_msg, dump_dict_to_json
from common.model_store import ModelFactory
from data_loader import DataLoader
from common.error_messages import FAILED_VALIDATE_RECORDS

VISIBILITY_TIMEOUT = 20

def metadataValidate(configs, job_queue, mongo_dao):
    batches_processed = 0
    log = get_logger('Metadata Validation Service')
    try:
        model_store = ModelFactory(configs) 
        # dump models to json files
        # dump_dict_to_json([model[MODEL] for model in model_store.models], f"tmp/data_models_dump.json")
        dump_dict_to_json(model_store.models, f"tmp/data_models_dump.json")
    except Exception as e:
        log.debug(e)
        log.exception(f'Error occurred when initialize metadata validation service: {get_exception_msg()}')
        return 1
    validator = MetaDataValidator(configs, mongo_dao, model_store)

    #step 3: run validator as a service
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {configs[SQS_NAME]}, '
                            f'{batches_processed} batches have been processed so far')
            
            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                extender = None
                data = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    # Make sure job is in correct format
                    if data.get(SUBMISSION_ID) and data.get(SCOPE):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        scope = data[SUBMISSION_ID]
                        submissionID = data[SUBMISSION_ID]
                        result = validator.validate(submissionID, scope) 
                        if not result:
                            mongo_dao.set_submission_error(validator.submission, None, configs[DB])
                    else:
                        log.error(f'Invalid message: {data}!')

                    batches_processed +=1
                    
                except Exception as e:
                    log.debug(e)
                    log.critical(
                        f'Something wrong happened while processing file! Check debug log for details.')
                finally:
                    try:
                        msg.delete()
                    except Exception as e1:
                        log.debug(e1)
                        log.critical(
                        f'Something wrong happened while delete sqs message! Check debug log for details.')
                    if extender:
                        extender.stop()
                        extender = None
        except KeyboardInterrupt:
            log.info('Good bye!')
            return


""" Requirement for the ticket crdcdh-343
For files: read manifest file and validate local files’ sizes and md5s
For metadata: validate data folder contains TSV or TXT files
Compose a list of files to be updated and their sizes (metadata or files)
"""

class MetaDataValidator:
    
    def __init__(self, configs, mongo_dao, model_store):
        self.configs = configs
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('MetaData Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.submission = None
        self.datacommon = None

    def validate(self, submissionID, scope):
        #1. # get data common from submission
        submission = self.mongo_dao.get_submission(submissionID, self.configs[DB])
        if not submission:
            msg = f'Invalid submissionID, no submission found, {submissionID}!'
            self.log.error(msg)
            return False
        submission[ERRORS] = [] if not submission.get(ERRORS) else submission[ERRORS]
        if not submission.get(DATA_COMMON_NAME):
            msg = f'Invalid submission, no datacommon found, {submissionID}!'
            self.log.error(msg)
            error = {"title": "Invalid submission", "description": msg}
            submission[ERRORS] = submission[ERRORS] + [error]
            return False
        self.submission = submission
        self.datacommon = submission.get(DATA_COMMON_NAME)
        model = self.model_store.get_model_by_data_common(self.datacommon)
        #1. call mongo_dao to get dataRecords based on submissionID and scope
        dataRecords = self.mongo_dao.get_dataRecords(submissionID, scope, self.configs[DB])
        if not dataRecords or len(dataRecords) == 0:
            msg = f'No dataRecords found for the submission, {submissionID} at scope, {scope}!'
            self.log.error(msg)
            error = {"title": "Invalid submission", "description": msg}
            submission[ERRORS] = submission[ERRORS] + [error]
            return False
        #2. loop through all records and call validateNode
        updated_records = []
        try:
            for record in dataRecords:
                status, errors, warnings = self.validate_node(record, model)
                # todo set record with status, errors and warnings
                if errors and len(errors) > 0:
                    record[ERRORS] = record[ERRORS] + errors if record.get(ERRORS) else errors
                if warnings and len(warnings)> 0: 
                    record[WARNINGS] = record[WARNINGS] + warnings if record.get(WARNINGS) else warnings
                record[FILE_STATUS] = status
                record[UPDATED_AT] = current_datetime_str()
                updated_records.append(record)
        except Exception as e:
            self.log.debug(e)
            msg = f'Failed to validate dataRecords for the submission, {submissionID} at scope, {scope}!'
            self.log.exception(msg)
            error = {"title": "Failed to validate dataRecords", "description": msg}
            submission[ERRORS].append(error)
        #3. update data records based on record's _id
        result = self.mongo_dao.update_files(updated_records, self.configs[DB])
        if not result:
            #4. set errors in submission
            msg = f'Failed to update dataRecords for the submission, {submissionID} at scope, {scope}!'
            self.log.error(msg)
            error = {"title": "Failed to update dataRecords", "description": msg}
            submission[ERRORS].append(error)

        return result

    def validate_node(self, dataRecord, model):
        # set default return values
        result = STATUS_PASSED
        errors = []
        warnings = []

        # call validate_required_props
        result_required= self.validate_required_props(dataRecord, model)
        # call validate_prop_value
        result_prop_value = self.validate_prop_value(dataRecord, model)
        # call validate_relationship
        result_rel = self.validate_relationship(dataRecord, model)

        # concatenation of all errors
        errors = result_required.get(ERRORS, []) +  result_prop_value.get(ERRORS, []) + result_rel.get(ERRORS, [])
        # concatenation of all warnings
        warnings = result_required.get(WARNINGS, []) +  result_prop_value.get(WARNINGS, []) + result_rel.get(WARNINGS, [])
        # if there are any errors set the result to "Error"
        if len(errors) > 0:
            result = STATUS_ERROR
            return result, errors, warnings
        # if there are no errors but warnings,  set the result to "Warning"
        if len(warnings) > 0:
            result = STATUS_WARNING
            return result, errors, warnings
        #  if there are neither errors nor warnings, return default values
        return result, errors, warnings
    
    def validate_required_props(self, data_record, node_definition):
        result = {"result": STATUS_ERROR, ERRORS: [], WARNINGS: []}
        # check the correct format from the data_record
        if "nodeType" not in data_record.keys() or "props" not in data_record.keys() or len(data_record["props"].items()) == 0:
            result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, "data record is not correctly formatted."))
            return result

        # validation start
        nodes = node_definition[MODEL].get("nodes", {})
        node_type = data_record["nodeType"]
        # extract a node from the data record
        if node_type not in nodes.keys():
            result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"Required node '{node_type}' does not exist."))
            return result

        anode_definition = nodes[node_type]
        id_property_key = anode_definition["id_property"]
        id_property_value = data_record["props"].get(id_property_key, None)
        # check id property key and value are valid
        if not (id_property_key not in data_record["props"].keys()) and not (isinstance(id_property_value, str) and id_property_value.strip()):
            result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, "ID/Key property is missing or empty in the data-record."))

        for data_key, data_value in data_record["props"].items():
            anode_keys = anode_definition.keys()
            if "properties" not in anode_keys:
                result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, "data record is not correctly formatted."))
                continue

            if data_key not in anode_definition["properties"].keys():
                result[WARNINGS].append(create_error(FAILED_VALIDATE_RECORDS, f"data record key '{data_key}' does not exist in the node-definition."))
                continue

            # check missing required key and empty value
            if anode_definition["properties"][data_key]["required"]:
                if data_value is None or (isinstance(data_value, str) and not data_value.strip()):
                    result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"Required property '{data_key}' is missing or empty."))

        if len(result[WARNINGS]) > 0:
            result["result"] = STATUS_WARNING

        if len(result[ERRORS]) == 0 and len(result[WARNINGS]) == 0:
            result["result"] = STATUS_PASSED
        return result
    
    def validate_prop_value(self, dataRecord, model):
        # set default return values
        errors = []
        warnings = []
        result = STATUS_PASSED
        return {"result": result, ERRORS: errors, WARNINGS: warnings}
    
    def validate_relationship(self, dataRecord, model):
        # set default return values
        errors = []
        warnings = []
        result = STATUS_PASSED
        return {"result": result, ERRORS: errors, WARNINGS: warnings}


def create_error(title, msg):
    return {"title": title, "description": msg}

