#!/usr/bin/env python3

import pandas as pd
import json
from datetime import datetime
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger, DATE_FORMATS, DATETIME_FORMAT
from common.constants import SQS_NAME, SQS_TYPE, SCOPE, MODEL, SUBMISSION_ID, ERRORS, WARNINGS, STATUS_ERROR, \
    STATUS_WARNING, STATUS_PASSED, STATUS, UPDATED_AT, MODEL_FILE_DIR, TIER_CONFIG, DATA_COMMON_NAME, \
    NODE_TYPE, PROPERTIES, TYPE, MIN, MAX, VALUE_EXCLUSIVE, VALUE_PROP, VALID_PROP_TYPE_LIST, VALIDATION_RESULT, VALIDATED_AT
from common.utils import current_datetime_str, get_exception_msg, dump_dict_to_json, create_error
from common.model_store import ModelFactory
from common.error_messages import FAILED_VALIDATE_RECORDS

VISIBILITY_TIMEOUT = 20

def metadataValidate(configs, job_queue, mongo_dao):
    batches_processed = 0
    log = get_logger('Metadata Validation Service')
    try:
        model_store = ModelFactory(configs[MODEL_FILE_DIR], configs[TIER_CONFIG]) 
        # dump models to json files
        dump_dict_to_json(model_store.models, f"models/data_model.json")
    except Exception as e:
        log.debug(e)
        log.exception(f'Error occurred when initialize metadata validation service: {get_exception_msg()}')
        return 1
    validator = MetaDataValidator(mongo_dao, model_store)

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
                    if data.get(SQS_TYPE) == "Validate Metadata" and data.get(SUBMISSION_ID) and data.get(SCOPE):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        scope = data[SCOPE]
                        submissionID = data[SUBMISSION_ID]
                        status = validator.validate(submissionID, scope) 
                        if status and status != "Failed": 
                            mongo_dao.set_submission_error(validator.submission, status, None, False)
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
    
    def __init__(self, mongo_dao, model_store):
        self.log = get_logger('MetaData Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.submission = None

    def validate(self, submissionID, scope):
        #1. # get data common from submission
        submission = self.mongo_dao.get_submission(submissionID)
        if not submission:
            msg = f'Invalid submissionID, no submission found, {submissionID}!'
            self.log.error(msg)
            return "Failed"
        # submission[ERRORS] = [] if not submission.get(ERRORS) else submission[ERRORS]
        if not submission.get(DATA_COMMON_NAME):
            msg = f'Invalid submission, no datacommon found, {submissionID}!'
            self.log.error(msg)
            # error = {"title": "Invalid submission", "description": msg}
            return "Failed"
        self.submission = submission
        datacommon = submission.get(DATA_COMMON_NAME)
        model = self.model_store.get_model_by_data_common(datacommon)
        #1. call mongo_dao to get dataRecords based on submissionID and scope
        dataRecords = self.mongo_dao.get_dataRecords(submissionID, scope)
        if not dataRecords or len(dataRecords) == 0:
            msg = f'No dataRecords found for the submission, {submissionID} at scope, {scope}!'
            self.log.error(msg)
            return None
        #2. loop through all records and call validateNode
        updated_records = []
        isError = False
        isWarning = False
        try:
            for record in dataRecords:
                status, errors, warnings = self.validate_node(record, model)
                # todo set record with status, errors and warnings
                if errors and len(errors) > 0:
                    record[ERRORS] = record[ERRORS] + errors if record.get(ERRORS) else errors
                    isError = True
                if warnings and len(warnings)> 0: 
                    record[WARNINGS] = record[WARNINGS] + warnings if record.get(WARNINGS) else warnings
                    isWarning = True
                record[STATUS] = status
                record[UPDATED_AT] = record[VALIDATED_AT] = current_datetime_str()
                updated_records.append(record)
        except Exception as e:
            self.log.debug(e)
            msg = f'Failed to validate dataRecords for the submission, {submissionID} at scope, {scope}!'
            self.log.exception(msg)
            # error = {"title": "Failed to validate dataRecords", "description": msg}
            # submission[ERRORS].append(error)
            return "Failed"
        #3. update data records based on record's _id
        result = self.mongo_dao.update_files(updated_records)
        if not result:
            #4. set errors in submission
            msg = f'Failed to update dataRecords for the submission, {submissionID} at scope, {scope}!'
            self.log.error(msg)
            return None
            # error = {"title": "Failed to update dataRecords", "description": msg}
            # submission[ERRORS].append(error)
        return STATUS_ERROR if isError else STATUS_WARNING if isWarning else STATUS_PASSED  

    def validate_node(self, dataRecord, model):
        # set default return values
        errors = []
        warnings = []

        # call validate_required_props
        result_required= self.validate_required_props(dataRecord, model)
        # call validate_prop_value
        result_prop_value = self.validate_props(dataRecord, model)
        # call validate_relationship
        result_rel = self.validate_relationship(dataRecord, model)

        # concatenation of all errors
        errors = result_required.get(ERRORS, []) +  result_prop_value.get(ERRORS, []) + result_rel.get(ERRORS, [])
        # concatenation of all warnings
        warnings = result_required.get(WARNINGS, []) +  result_prop_value.get(WARNINGS, []) + result_rel.get(WARNINGS, [])
        # if there are any errors set the result to "Error"
        if len(errors) > 0:
            return STATUS_ERROR, errors, warnings
        # if there are no errors but warnings,  set the result to "Warning"
        if len(warnings) > 0:
            return STATUS_WARNING, errors, warnings
        #  if there are neither errors nor warnings, return default values
        return STATUS_PASSED, errors, warnings
    
    def validate_required_props(self, data_record, node_definition):
        result = {"result": STATUS_ERROR, ERRORS: [], WARNINGS: []}
        # check the correct format from the data_record
        if "nodeType" not in data_record.keys() or "props" not in data_record.keys() or len(data_record["props"].items()) == 0:
            result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, "data record is not correctly formatted."))
            return result

        # validation start
        nodes = node_definition.model[MODEL].get("nodes", {})
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
    
    def validate_props(self, dataRecord, model):
        # set default return values
        errors = []
        props_def = model.get_node_props(dataRecord.get(NODE_TYPE))
        props = dataRecord.get(PROPERTIES)
        for k, v in props.items():
            prop_def = props_def.get(k)
            if not prop_def: 
                errors.append(create_error("Property not defined", f"The property, {k}, is not defined in model!"))
                continue
            else:
                if v is None:
                    continue
            
            errs = self.validate_prop_value(v, prop_def)
            if len(errs) > 0:
                errors.extend(errs)

        return {VALIDATION_RESULT: STATUS_ERROR if len(errors) > 0 else STATUS_PASSED, ERRORS: errors, WARNINGS: []}

    def get_parent_node_cache(self, data_record_parent_nodes):
        parent_nodes = []
        for parent_node in data_record_parent_nodes:
            parent_type = parent_node.get("parentType")
            parent_id_property = parent_node.get("parentIDPropName")
            parent_id_value = parent_node.get("parentIDValue")
            if parent_type and parent_id_value and parent_id_value is not None:
                parent_nodes.append({"type": parent_type, "key": parent_id_property, "value": parent_id_value})
        exist_parent_nodes = self.mongo_dao.search_nodes_by_type_and_value(parent_nodes)

        parent_node_cache = set()
        for node in exist_parent_nodes:
            if node.get("props"):
                for key, value in node["props"].items():
                    parent_node_cache.add(tuple([node.get("nodeType"), key, value]))
        return parent_node_cache


    def validate_relationship(self, data_record, model):
        # set default return values
        result = {"result": STATUS_ERROR, ERRORS: [], WARNINGS: []}
        if not data_record.get("parents"):
            result["result"] = STATUS_WARNING
            result[WARNINGS].append(create_error(FAILED_VALIDATE_RECORDS, "Parent property does not exist or empty"))
            return result

        data_record_parent_nodes = data_record.get("parents")
        node_keys = model.get_node_keys()

        node_type = data_record.get("nodeType")
        node_relationships = model.get_node_relationships(node_type)
        if not node_type or node_type not in node_keys:
            node_type = f'\'{node_type}\'' if node_type else ''
            result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"Current node property {node_type} does not exist."))

        parent_node_cache = self.get_parent_node_cache(data_record_parent_nodes)
        for parent_node in data_record_parent_nodes:
            parent_type = parent_node.get("parentType")
            if not parent_type or parent_type not in node_keys:
                result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"Parent property '{parent_type}' does not exist."))
                continue

            parent_id_property = parent_node.get("parentIDPropName")
            model_properties = model.get_node_props(parent_type)

            if not model_properties or parent_id_property not in model_properties:
                result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"ID property in parent node '{parent_id_property}' does not exist."))
                continue
            # check node relationship
            if not node_relationships or not node_relationships.get(parent_type):
                result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"parent node '{parent_type}' is not defined in the node relationship."))
                continue

            # these should be defined in the data model in the properties
            is_parent_id_valid_format = model.get_node_props(parent_type)
            is_parent_id_exist = is_parent_id_valid_format and is_parent_id_valid_format.get(parent_id_property)
            if not is_parent_id_valid_format or not is_parent_id_exist:
                result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"ID property in parent node '{parent_id_property}' does not exist"))
                continue

            # collect all node_type, node_value, parentIDValue for the parent nodes
            parent_id_value = parent_node.get("parentIDValue")
            if parent_id_value is None or (isinstance(parent_id_value, str) and not parent_id_value.strip()):
                result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"'{parent_id_property}'s parent value is missing or empty."))
                continue

            if (parent_type, parent_id_property, parent_id_value) not in parent_node_cache:
                result[ERRORS].append(create_error(FAILED_VALIDATE_RECORDS, f"Parent parent node '{parent_id_property}' does not exist in the database."))

        if len(result[WARNINGS]) > 0:
            result["result"] = STATUS_WARNING

        if len(result[ERRORS]) == 0 and len(result[WARNINGS]) == 0:
            result["result"] = STATUS_PASSED
        return result

    def validate_prop_value(self, value, prop_def):
        # set default return values
        errors = []
        type = prop_def.get(TYPE)
        if not type or not type in VALID_PROP_TYPE_LIST:
            errors.append(f"Invalid property type, {type}!")
        else:
            permissive_vals = prop_def.get("permissible_values")
            minimum = prop_def.get(MIN)
            maximum = prop_def.get(MAX)
            if type == "string":
                val = str(value)
                result, error = check_permissive(val, permissive_vals)
                if not result:
                    errors.append(error)
            elif type == "integer":
                try:
                    val = int(value)
                except ValueError as e:
                    errors.append(create_error("Not a integer", f"The value, {value}, is not a integer!"))

                result, error = check_permissive(val, permissive_vals)
                if not result:
                    errors.append(error)

                errs = check_boundary(val, minimum, maximum)
                if len(errs) > 0:
                    errors.extend(errs)

            elif type == "number":
                try:
                    val = float(value)
                except ValueError as e:
                    errors.append(create_error("Not a number", f"The value, {value}, is not a number!"))
                result, error = check_permissive(val, permissive_vals)
                if not result:
                    errors.append(error)

                errs = check_boundary(val, minimum, maximum)
                if len(errs) > 0:
                    errors.extend(errs)

            elif type == "datetime":
                try:
                    val = datetime.strptime(value, DATETIME_FORMAT)
                except ValueError as e:
                    errors.append(create_error("Not a valid datetime", f"The value, {value}, is not a valid datetime!"))

            elif type == "date":
                val = None
                for date_format in DATE_FORMATS:
                    try:
                        val = datetime.strptime(value, date_format)
                        break #if the value can be parsed with the format
                    except ValueError as e:
                        continue
                if val is None:
                    errors.append(create_error("Not a valid date", f"The value, {value}, is not a valid date!"))

            elif type == "boolean":
                if not isinstance(value, bool) and value not in ["yes", "true", "no", "false"]:
                    errors.append(create_error("Not a boolean", f"The value, {value}, is not a boolean!"))
            
            elif type == "array":
                arr = value.split("*")
                for item in arr:
                    result, error = check_permissive(item, permissive_vals)
                    if not result:
                        errors.append(error)
            else:
                errors.append(create_error("Not a valid type", f"Invalid data type, {type}!"))

        return errors
    
"""util functions"""
def check_permissive(value, permissive_vals):
    result = True,
    error = None
    if permissive_vals and len(permissive_vals) and value not in permissive_vals:
       result = False
       error = create_error("Not permitted value", f"The value, {value} is not allowed!")
    return result, error

def check_boundary(value, min, max):
    errors = []
    if min and min.get(VALUE_PROP):
        val = min.get(VALUE_PROP)
        exclusive = min.get(VALUE_EXCLUSIVE)
        if (exclusive and value <= val) or (not exclusive and value < val):
            errors.append(create_error("Less than minimum", f"The value is less than minimum, {value} < {min}!"))

    if max and max.get(VALUE_PROP):
        val = max.get(VALUE_PROP)
        exclusive = max.get(VALUE_EXCLUSIVE)
        if (exclusive and value >= val) or (not exclusive and value > val):
            errors.append(create_error("More than maximum", f"The value is more than maximum, {value} > {min}!"))      

    return errors

