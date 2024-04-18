#!/usr/bin/env python3

import pandas as pd
import json
from datetime import datetime
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger, DATE_FORMATS, DATETIME_FORMAT
from common.constants import SQS_NAME, SQS_TYPE, SCOPE, SUBMISSION_ID, ERRORS, WARNINGS, STATUS_ERROR, ID, FAILED, \
    STATUS_WARNING, STATUS_PASSED, STATUS, UPDATED_AT, MODEL_FILE_DIR, TIER_CONFIG, DATA_COMMON_NAME, MODEL_VERSION, \
    NODE_TYPE, PROPERTIES, TYPE, MIN, MAX, VALUE_EXCLUSIVE, VALUE_PROP, VALIDATION_RESULT, \
    VALIDATED_AT, SERVICE_TYPE_METADATA, NODE_ID, PROPERTIES, PARENTS, KEY
from common.utils import current_datetime, get_exception_msg, dump_dict_to_json, create_error
from common.model_store import ModelFactory
from common.model_reader import valid_prop_types
from service.ecs_agent import set_scale_in_protection

VISIBILITY_TIMEOUT = 20
BATCH_SIZE = 1000

def metadataValidate(configs, job_queue, mongo_dao):
    log = get_logger('Metadata Validation Service')
    try:
        model_store = ModelFactory(configs[MODEL_FILE_DIR], configs[TIER_CONFIG]) 
        # dump models to json files
        dump_dict_to_json(model_store.models, f"models/data_model.json")
    except Exception as e:
        log.debug(e)
        log.exception(f'Error occurred when initialize metadata validation service: {get_exception_msg()}')
        return 1

    #step 3: run validator as a service
    log.info(f'{SERVICE_TYPE_METADATA} service started')
    batches_processed = 0
    scale_in_protection_flag = False
    while True:
        try:
            msgs = job_queue.receiveMsgs(VISIBILITY_TIMEOUT)
            if len(msgs) > 0:
                log.info(f'New message is coming: {configs[SQS_NAME]}, '
                         f'{batches_processed} {SERVICE_TYPE_METADATA} validation(s) have been processed so far')
                scale_in_protection_flag = True
                set_scale_in_protection(True)
            else:
                if scale_in_protection_flag is True:
                    scale_in_protection_flag = False
                    set_scale_in_protection(False)

            for msg in msgs:
                log.info(f'Received a job!')
                extender = None
                data = None
                validator = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    # Make sure job is in correct format
                    if data.get(SQS_TYPE) == "Validate Metadata" and data.get(SUBMISSION_ID) and data.get(SCOPE):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        scope = data[SCOPE]
                        submission_id = data[SUBMISSION_ID]
                        validator = MetaDataValidator(mongo_dao, model_store)
                        status = validator.validate(submission_id, scope)
                        mongo_dao.set_submission_validation_status(validator.submission, None, status, None)
                    else:
                        log.error(f'Invalid message: {data}!')

                    log.info(f'Processed {SERVICE_TYPE_METADATA} validation for the submission: {data[SUBMISSION_ID]}!')
                    batches_processed += 1
                    msg.delete()
                except Exception as e:
                    log.debug(e)
                    log.critical(
                        f'Something wrong happened while processing file! Check debug log for details.')
                finally:
                    if validator:
                        del validator
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
        self.model = None
        self.submission = None
        self.isError = None
        self.isWarning = None

    def validate(self, submission_id, scope):
        #1. # get data common from submission
        submission = self.mongo_dao.get_submission(submission_id)
        if not submission:
            msg = f'Invalid submissionID, no submission found, {submission_id}!'
            self.log.error(msg)
            return FAILED
        if not submission.get(DATA_COMMON_NAME):
            msg = f'Invalid submission, no datacommon found, {submission_id}!'
            self.log.error(msg)
            return FAILED
        self.submission = submission
        datacommon = submission.get(DATA_COMMON_NAME)
        model_version = submission.get(MODEL_VERSION)
        #2 get data model based on datacommon and version
        self.model = self.model_store.get_model_by_data_common_version(datacommon, model_version)
        if not self.model.model or not self.model.get_nodes():
            msg = f'{self.datacommon} model version "{model_version}" is not available.'
            self.log.error(msg)
            return STATUS_ERROR
        #3 retrieve data batch by batch
        start_index = 0
        validated_count = 0
        while True:
            data_records = self.mongo_dao.get_dataRecords_chunk(submission_id, scope, start_index, BATCH_SIZE)
            if start_index == 0 and (not data_records or len(data_records) == 0):
                msg = f'No more new metadata to be validated.'
                self.log.error(msg)
                return FAILED
            
            count = len(data_records) 
            validated_count += self.validate_nodes(data_records, submission_id, scope)
            if count < BATCH_SIZE: 
                self.log.info(f"{submission_id}: {validated_count} out of {count + start_index} nodes are validated.")
                return STATUS_ERROR if self.isError else STATUS_WARNING if self.isWarning  else STATUS_PASSED 
            start_index += count  

    def validate_nodes(self, data_records, submission_id, scope):
        #2. loop through all records and call validateNode
        updated_records = []
        validated_count = 0
        try:
            for record in data_records:
                status, errors, warnings = self.validate_node(record)
                # todo set record with status, errors and warnings
                if errors and len(errors) > 0:
                    record[ERRORS] = errors
                    self.isError = True
                if warnings and len(warnings)> 0: 
                    record[WARNINGS] = warnings
                    self.isWarning = True
                record[STATUS] = status
                record[UPDATED_AT] = record[VALIDATED_AT] = current_datetime()
                updated_records.append(record)
                validated_count += 1
        except Exception as e:
            self.log.debug(e)
            msg = f'Failed to validate dataRecords for the submission, {submission_id} at scope, {scope}!'
            self.log.exception(msg) 
            self.isError = True 
        #3. update data records based on record's _id
        result = self.mongo_dao.update_data_records_status(updated_records)
        if not result:
            #4. set errors in submission
            msg = f'Failed to update dataRecords for the submission, {submission_id} at scope, {scope}!'
            self.log.error(msg)
            self.isError = True

        return validated_count

    def validate_node(self, data_record):
        # set default return values
        errors = []
        warnings = []
        msg_prefix = f'[{data_record.get("orginalFileName")}: line {data_record.get("lineNumber")}]'
        node_keys = self.model.get_node_keys()
        node_type = data_record.get("nodeType")
        if not node_type or node_type not in node_keys:
            return STATUS_ERROR,[create_error("Invalid node type", f'{msg_prefix} Node type “{node_type}” is not defined')], None
        try:
            # call validate_required_props
            result_required= self.validate_required_props(data_record)
            # call validate_prop_value
            result_prop_value = self.validate_props(data_record)
            # call validate_relationship
            result_rel = self.validate_relationship(data_record)

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
        except Exception as e:
            self.log.debug(e)
            msg = f'Failed to validate dataRecords for the submission, {submission_id} at scope, {scope}!'
            self.log.exception(msg) 
            error = create_error("Internal error", "{msg_prefix} metadata validation failed due to internal errors.  Please try again and contact the helpdesk if this error persists.")
            return STATUS_ERROR,[error], None
        #  if there are neither errors nor warnings, return default values
        return STATUS_PASSED, errors, warnings
    
    def validate_required_props(self, data_record):
        result = {"result": STATUS_ERROR, ERRORS: [], WARNINGS: []}
        msg_prefix = f'[{data_record.get("orginalFileName")}: line {data_record.get("lineNumber")}]'
        # check the correct format from the data_record
        if "nodeType" not in data_record.keys() or "props" not in data_record.keys() or len(data_record[PROPERTIES].items()) == 0:
            result[ERRORS].append(create_error("Invalid node", f'{msg_prefix} "nodeType" or "props" is empty.'))
            return result

        # validation start
        nodes = self.model.get_nodes()
        node_type = data_record["nodeType"]
        # extract a node from the data record
        anode_definition = nodes[node_type]
        id_property_key = anode_definition["id_property"]
        id_property_value = data_record[PROPERTIES].get(id_property_key, None)
        # check id property key and value are valid
        if not str(id_property_value).strip():
            result[ERRORS].append(create_error("Missing ID property", f'{msg_prefix} ID property, "{id_property_key}" is empty.'))
        else:
            # check if duplicate records
            results = self.mongo_dao.search_nodes_by_index([{TYPE: node_type, KEY: id_property_key, VALUE_PROP: id_property_value}], self.submission[ID])
            if len(results) > 1:
                duplicates = ""
                for item in results:
                    if item[ID] == data_record[ID]:
                        continue
                    duplicates += f'"{item.get("orginalFileName")}" line {item.get("lineNumber")},'
                duplicates = duplicates.strip(",")    
                result[ERRORS].append(create_error("Duplicate IDs", f'{msg_prefix} same ID also appears in {duplicates}.'))

        for data_key, data_value in data_record[PROPERTIES].items():
            anode_keys = anode_definition.keys()
            if "properties" not in anode_keys:
                result[ERRORS].append(create_error("Invalid data model", f'"properties" is not defined in the model.'))
                continue

            if data_key not in anode_definition["properties"].keys():
                result[WARNINGS].append(create_error("Invalid property", f'{msg_prefix} Property "{data_key}" is not defined in the model.'))
                continue

            # check missing required key and empty value
            if anode_definition["properties"][data_key]["required"]:
                if data_value is None or not str(data_value).strip():
                    result[ERRORS].append(create_error("Missing required property", f'{msg_prefix} Required property "{data_key}" is empty.'))

        if len(result[WARNINGS]) > 0:
            result["result"] = STATUS_WARNING

        if len(result[ERRORS]) == 0 and len(result[WARNINGS]) == 0:
            result["result"] = STATUS_PASSED
        return result
    
    def validate_props(self, dataRecord):
        # set default return values
        errors = []
        props_def = self.model.get_node_props(dataRecord.get(NODE_TYPE))
        props = dataRecord.get(PROPERTIES)
        file_name = dataRecord.get("orginalFileName")
        line_num = dataRecord.get("lineNumber")
        msg_prefix = f'[{file_name}: line {line_num}]'
        for k, v in props.items():
            prop_def = props_def.get(k)
            if not prop_def or v is None: 
                continue
            
            errs = self.validate_prop_value(k, v, prop_def, file_name, line_num)
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
        exist_parent_nodes = self.mongo_dao.search_nodes_by_index(parent_nodes, self.submission[ID])
        parent_node_cache = set()
        for node in exist_parent_nodes:
            if node.get(PROPERTIES):
                for key, value in node[PROPERTIES].items():
                    parent_node_cache.add(tuple([node.get("nodeType"), key, value]))
        return parent_node_cache
    
    def validate_relationship(self, data_record):
        # set default return values
        result = {"result": STATUS_ERROR, ERRORS: [], WARNINGS: []}
        msg_prefix = f'[{data_record.get("orginalFileName")}: line {data_record.get("lineNumber")}]'
        node_type = data_record.get("nodeType")
        node_relationships = self.model.get_node_relationships(node_type)
        if not node_relationships or len(node_relationships) == 0: 
            result["result"] = STATUS_PASSED
            return result
        data_record_parent_nodes = data_record.get(PARENTS)
        if not data_record_parent_nodes or len(data_record_parent_nodes) == 0:
            result["result"] = STATUS_WARNING
            result[ERRORS].append(create_error("Relationship not specified", f'{msg_prefix} No relationships specified.'))
            return result
        node_keys = self.model.get_node_keys()
        node_relationships = self.model.get_node_relationships(node_type)
        parent_node_cache = self.get_parent_node_cache(data_record_parent_nodes)
        data_common = data_record.get(DATA_COMMON_NAME)
        for parent_node in data_record_parent_nodes:
            parent_type = parent_node.get("parentType")
            if not parent_type or parent_type not in node_keys:
                result[ERRORS].append(create_error("Invalid relationship", f'{msg_prefix} Relationship to a “{parent_type}” node is not defined.'))
                continue

            parent_id_property = parent_node.get("parentIDPropName")
            model_properties = self.model.get_node_props(parent_type)

            if not model_properties or parent_id_property not in model_properties:
                result[ERRORS].append(create_error("Invalid relationship", f'"{parent_id_property}" is not a property of "{parent_type}".'))
                continue
            # check node relationship
            if not node_relationships or not node_relationships.get(parent_type):
                result[ERRORS].append(create_error("Invalid relationship", f'Relationship to a “{parent_type}” node is not defined.'))
                continue

            # these should be defined in the data model in the properties
            is_parent_id_valid_format = self.model.get_node_props(parent_type)
            is_parent_id_exist = is_parent_id_valid_format and is_parent_id_valid_format.get(parent_id_property)
            if not is_parent_id_valid_format or not is_parent_id_exist:
                result[ERRORS].append(create_error("Invalid relationship", f'“{parent_id_property}" is not a property of “{parent_type}”.'))
                continue

            # collect all node_type, node_value, parentIDValue for the parent nodes
            parent_id_value = parent_node.get("parentIDValue")
            if parent_id_value is None or (isinstance(parent_id_value, str) and not parent_id_value.strip()):
                result[ERRORS].append(create_error("Invalid relationship", f'Property “{parent_id_property}" of related node “{parent_type}” is empty.'))
                continue

            if (parent_type, parent_id_property, parent_id_value) not in parent_node_cache:
                released_parent = self.mongo_dao.search_released_node(data_common, parent_type, parent_id_value)
                if not released_parent:
                    result[ERRORS].append(create_error("Related node not found", f'Related node “{parent_type}” [“{parent_id_property}”: “{parent_id_value}"] not found.'))

        if len(result[WARNINGS]) > 0:
            result["result"] = STATUS_WARNING

        if len(result[ERRORS]) == 0 and len(result[WARNINGS]) == 0:
            result["result"] = STATUS_PASSED
        return result

    def validate_prop_value(self, prop_name, value, prop_def, file_name, line_num):
        # set default return values
        errors = []
        type = prop_def.get(TYPE)
        msg_prefix = f'[{file_name}: line {line_num}]'
        if not type or not type in valid_prop_types:
            errors.append(create_error("Invalid property definition", f'{msg_prefix} Property "{prop_name}": “{type}” type is not an allowed property type for this model.'))
        else:
            val = None
            permissive_vals = prop_def.get("permissible_values")
            minimum = prop_def.get(MIN)
            maximum = prop_def.get(MAX)
            if type == "string":
                val = str(value)
                result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name)
                if not result:
                    errors.append(error)
            elif type == "integer":
                try:
                    val = int(value)
                except ValueError as e:
                    errors.append(create_error("Invalid integer value", f'{msg_prefix} Property "{prop_name}": "{value}" is not a valid integer type.'))

                result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name)
                if not result:
                    errors.append(error)

                errs = check_boundary(val, minimum, maximum, msg_prefix, prop_name)
                if len(errs) > 0:
                    errors.extend(errs)

            elif type == "number":
                try:
                    val = float(value)
                except ValueError as e:
                    errors.append(create_error("Invalid number value", f'{msg_prefix} Property "{prop_name}": "{value}" is not a valid number type.'))
                result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name)
                if not result:
                    errors.append(error)

                errs = check_boundary(val, minimum, maximum, msg_prefix, prop_name)
                if len(errs) > 0:
                    errors.extend(errs)

            elif type == "datetime":
                try:
                    val = datetime.strptime(value, DATETIME_FORMAT)
                except ValueError as e:
                    errors.append(create_error("Invalid datetime value", f'{msg_prefix} Property "{prop_name}": "{value}" is not a valid datetime type.'))

            elif type == "date":
                val = None
                for date_format in DATE_FORMATS:
                    try:
                        val = datetime.strptime(value, date_format)
                        break #if the value can be parsed with the format
                    except ValueError as e:
                        continue
                if val is None:
                    errors.append(create_error("Invalid date value", f'{msg_prefix} Property "{prop_name}": "{value}" is not a valid date type.'))

            elif type == "boolean":
                if not isinstance(value, bool) and value not in ["yes", "true", "no", "false"]:
                    errors.append(create_error("Invalid boolean value", f'{msg_prefix} Property "{prop_name}": "{value}" is not a valid boolean type.'))
            
            elif type == "array" or type == "value-list":
                val = str(value)
                arr = val.split("*") if "*" in val else val.split(",") if "," in val else [value]
                for item in arr:
                    val = item.strip() if item and isinstance(item, str) else item
                    result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name)
                    if not result:
                        errors.append(error)
            else:
                errors.append(create_error("Invalid property definition", f'{msg_prefix} Property "{prop_name}": “{type}” type is not an allowed property type for this model.'))

        return errors
    
"""util functions"""
def check_permissive(value, permissive_vals, msg_prefix, prop_name):
    result = True,
    error = None
    if permissive_vals and len(permissive_vals) > 0 and value not in permissive_vals:
       result = False
       error = create_error("Value not permitted", f'{msg_prefix} "{value}" is not a permissible value for property “{prop_name}”.')
    return result, error

def check_boundary(value, min, max, msg_prefix, prop_name):
    errors = []
    if min and min.get(VALUE_PROP):
        val = min.get(VALUE_PROP)
        exclusive = min.get(VALUE_EXCLUSIVE)
        if (exclusive and value <= val) or (not exclusive and value < val):
            errors.append(create_error("Value out of range", f'{msg_prefix}  Property "{prop_name}": "{value}" is below lower bound.'))

    if max and max.get(VALUE_PROP):
        val = max.get(VALUE_PROP)
        exclusive = max.get(VALUE_EXCLUSIVE)
        if (exclusive and value >= val) or (not exclusive and value > val):
            errors.append(create_error("Value out of range", f'{msg_prefix}  Property "{prop_name}": "{value}" is above upper bound.'))      

    return errors

