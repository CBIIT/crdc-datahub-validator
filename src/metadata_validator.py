#!/usr/bin/env python3

import pandas as pd
import json
import os
from botocore.exceptions import ClientError
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import BATCH_STATUS, BATCH_TYPE_METADATA, DATA_COMMON_NAME, ERRORS, DB, \
    S3_DOWNLOAD_DIR, SQS_NAME, SCOPE, MODEL, SUBMISSION_ID, PASSED, ERROR
from common.utils import cleanup_s3_download_dir, get_exception_msg, dump_dict_to_json
from common.model_store import ModelFactory
from data_loader import DataLoader

VISIBILITY_TIMEOUT = 20

def metadataValidate(configs, job_queue, mongo_dao):
    batches_processed = 0
    log = get_logger('Metadata Validation Service')
    try:
        model_store = ModelFactory(configs) 
        # dump models to json
        dump_dict_to_json([model[MODEL] for model in model_store.models], f"tmp/data_models_dump.json")
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
                    except:
                        log.debug(e)
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
            submission[ERRORS] = submission[ERRORS].append(msg)
            return False
        self.submission = submission
        self.datacommon = submission.get(DATA_COMMON_NAME)
        #1. cal mongo_dao to get dataRecords based on submissionID and scope

        #2. loop through all records and call validateNode

        #3. update records
        
        return True

    
    def validateNode(self, dataRecord, model):
        
        return None

    def validate_required_props(self, data_record, node_definition):
        result = {"result": ERROR, "errors": [], "warnings": []}
        # check the correct format from the node_definition
        if "model" not in node_definition.keys() or "nodes" not in node_definition["model"].keys():
            result["errors"].append(f"node definition is not correctly formatted.")
            return result
        # check the correct format from the data_record
        if "nodeType" not in data_record.keys() or "rawData" not in data_record.keys() or len(data_record["rawData"].items()) == 0:
            result["errors"].append(f"data record is not correctly formatted.")
            return result

        # validation start
        nodes = node_definition["model"]["nodes"]
        node_type = data_record["nodeType"]
        # extract a node from the data record
        if node_type not in nodes.keys():
            result["errors"].append(f"Required node '{node_type}' does not exist.")
            return result

        anode = nodes[node_type]
        if node_type in nodes.keys():
            for key, value in data_record["rawData"].items():
                anode_keys = anode.keys()
                if "properties" not in anode_keys:
                    result["errors"].append(f"data record is not correctly formatted.")
                    break

                key_not_exist = key not in anode["properties"]
                value_invalid = not value.strip()
                if key_not_exist or value_invalid:
                    result["errors"].append(f"Required property '{key}' is missing or empty.")

        if len(result["errors"]) == 0:
            result["result"] = PASSED
        return result

    def validate_prop_value(self, dataRecord, model):
        return None
    
    def validate_relationship(self, dataRecord, model):
        return None


