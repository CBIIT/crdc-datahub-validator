#!/usr/bin/env python3
import json
from datetime import datetime
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger, DATE_FORMATS
from common.constants import SQS_NAME, SQS_TYPE, SCOPE, SUBMISSION_ID, ERRORS, WARNINGS, STATUS_ERROR, ID, FAILED, \
    STATUS_WARNING, STATUS_PASSED, STATUS, UPDATED_AT, MODEL_FILE_DIR, TIER_CONFIG, DATA_COMMON_NAME, MODEL_VERSION, \
    NODE_TYPE, PROPERTIES, TYPE, MIN, MAX, VALUE_EXCLUSIVE, VALUE_PROP, VALIDATION_RESULT, ORIN_FILE_NAME, \
    VALIDATED_AT, SERVICE_TYPE_METADATA, NODE_ID, PROPERTIES, PARENTS, KEY, NODE_ID, PARENT_TYPE, PARENT_ID_NAME, PARENT_ID_VAL, \
    SUBMISSION_INTENTION, SUBMISSION_INTENTION_NEW_UPDATE, SUBMISSION_INTENTION_DELETE, TYPE_METADATA_VALIDATE, TYPE_CROSS_SUBMISSION, \
    SUBMISSION_REL_STATUS_RELEASED, VALIDATION_ID, VALIDATION_ENDED, CDE_TERM, TERM_CODE, TERM_VERSION, CDE_PERMISSIVE_VALUES, \
    QC_RESULT_ID, BATCH_IDS, VALIDATION_TYPE_METADATA, S3_FILE_INFO, VALIDATION_TYPE_FILE, QC_SEVERITY, QC_VALIDATE_DATE, QC_ORIGIN, \
    QC_ORIGIN_METADATA_VALIDATE_SERVICE, QC_ORIGIN_FILE_VALIDATE_SERVICE, DISPLAY_ID, UPLOADED_DATE, LATEST_BATCH_ID, SUBMITTED_ID, \
    LATEST_BATCH_DISPLAY_ID, QC_VALIDATION_TYPE, DATA_RECORD_ID, PV_TERM
from common.utils import current_datetime, get_exception_msg, dump_dict_to_json, create_error, get_uuid_str
from common.model_store import ModelFactory
from common.model_reader import valid_prop_types
from service.ecs_agent import set_scale_in_protection
from x_submission_validator import CrossSubmissionValidator
from pv_puller import get_pv_by_datacommon_version_cde

VISIBILITY_TIMEOUT = 20
BATCH_SIZE = 1000
CDE_NOT_FOUND = "CDE not available"

def metadataValidate(configs, job_queue, mongo_dao):
    log = get_logger('Metadata Validation Service')
    try:
        model_store = ModelFactory(configs[MODEL_FILE_DIR], configs[TIER_CONFIG]) 
        # dump models to json files
        dump_dict_to_json(model_store.models, f"models/data_model.json")
    except Exception as e:
        log.exception(e)
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
                    extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                    submission_id = data.get(SUBMISSION_ID)
                    if data.get(SQS_TYPE) == TYPE_METADATA_VALIDATE and submission_id and data.get(SCOPE) and data.get(VALIDATION_ID):
                        scope = data[SCOPE]
                        validator = MetaDataValidator(mongo_dao, model_store, configs)
                        status = validator.validate(submission_id, scope)
                        validation_id = data[VALIDATION_ID]
                        validation_end_at = current_datetime()
                        mongo_dao.update_validation_status(validation_id, status, validation_end_at)
                        validator.submission[VALIDATION_ENDED] = validation_end_at
                        mongo_dao.set_submission_validation_status(validator.submission, None, status, None, None)
                    elif data.get(SQS_TYPE) == TYPE_CROSS_SUBMISSION and submission_id:
                        validator = CrossSubmissionValidator(mongo_dao)
                        status = validator.validate(submission_id)
                        if validator.submission:
                            mongo_dao.set_submission_validation_status(validator.submission, None, None, status, None)
                    else:
                        log.error(f'Invalid message: {data}!')
                    log.info(f'Processed {SERVICE_TYPE_METADATA} validation for the submission: {data[SUBMISSION_ID]}!')
                    batches_processed += 1
                    msg.delete()
                except Exception as e:
                    log.exception(e)
                    log.critical(
                        f'Something wrong happened while processing metadata! Check debug log for details.')
                finally:
                    if validator:
                        del validator
                    if extender:
                        extender.stop()
                        extender = None
        except KeyboardInterrupt:
            log.info('Good bye!')
            return
class MetaDataValidator:
    
    def __init__(self, mongo_dao, model_store, config):
        self.log = get_logger('MetaData Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.config = config
        self.model = None
        self.submission_id = None
        self.scope = None
        self.submission = None
        self.isError = None
        self.isWarning = None
        self.searched_sts = False
        self.not_found_cde = False


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
        self.submission_id = submission_id
        self.scope = scope
        self.submission = submission
        datacommon = submission.get(DATA_COMMON_NAME)
        self.datacommon = datacommon
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
            validated_count += self.validate_nodes(data_records)
            if count < BATCH_SIZE: 
                self.log.info(f"{submission_id}: {validated_count} out of {count + start_index} nodes are validated.")
                return STATUS_ERROR if self.isError else STATUS_WARNING if self.isWarning  else STATUS_PASSED 
            start_index += count  

    def validate_nodes(self, data_records):
        #2. loop through all records and call validateNode
        updated_records = []
        qc_results = []
        validated_count = 0
        try:
            for record in data_records:
                qc_result = None
                if record.get(QC_RESULT_ID):
                    qc_result = self.mongo_dao.get_qcRecord(record[QC_RESULT_ID])
                status, errors, warnings = self.validate_node(record)
                if status == STATUS_PASSED:
                    if qc_result:
                        self.mongo_dao.delete_qcRecord(qc_result[ID])
                        qc_result = None 
                    record[QC_RESULT_ID] = None
                else:
                    if not qc_result:
                        record[QC_RESULT_ID] = None
                        qc_result = get_qc_result(record, VALIDATION_TYPE_METADATA, self.mongo_dao)
                    if errors and len(errors) > 0:
                        self.isError = True
                        qc_result[ERRORS] = errors
                        qc_result[QC_SEVERITY] = STATUS_ERROR
                    else:
                        qc_result[ERRORS] = []
                    if warnings and len(warnings)> 0: 
                        self.isWarning = True
                        qc_result[WARNINGS] = warnings
                        if not errors or len(errors) == 0:
                            qc_result[QC_SEVERITY] = STATUS_WARNING
                    else:
                        qc_result[WARNINGS] = []

                    qc_result[QC_VALIDATE_DATE] = current_datetime()
                    if not qc_result:
                        record[QC_RESULT_ID] = None
                        qc_result = get_qc_result(record, VALIDATION_TYPE_METADATA, self.mongo_dao)
                    if errors and len(errors) > 0:
                        self.isError = True
                        qc_result[ERRORS] = errors
                        qc_result[QC_SEVERITY] = STATUS_ERROR
                    else:
                        qc_result[ERRORS] = []
                    if warnings and len(warnings)> 0: 
                        self.isWarning = True
                        qc_result[WARNINGS] = warnings
                        if not errors or len(errors) == 0:
                            qc_result[QC_SEVERITY] = STATUS_WARNING
                    else:
                        qc_result[WARNINGS] = []

                    qc_result[QC_VALIDATE_DATE] = current_datetime()
                    qc_results.append(qc_result)
                    record[QC_RESULT_ID] = qc_result[ID]
                    
                record[STATUS] = status
                record[UPDATED_AT] = record[VALIDATED_AT] = current_datetime()
                updated_records.append(record)
                validated_count += 1
        except Exception as e:
            self.log.exception(e)
            msg = f'Failed to validate dataRecords for the submission, {self.submission_id} at scope, {self.scope}!'
            self.log.exception(msg) 
            self.isError = True 

        #3. update data records based on record's _id
        if len(qc_results) > 0:
            result = self.mongo_dao.save_qc_results(qc_results)
            if not result:
                msg = f'Failed to save qcResults for the submission, {self.submission_id} at scope, {self.scope}!'
                self.log.error(msg)
                
        result = self.mongo_dao.update_data_records_status(updated_records)
        if not result:
            #4. set errors in submission
            msg = f'Failed to update dataRecords for the submission, {self.submission_id} at scope, {self.scope}!'
            self.log.error(msg)
            self.isError = True
        return validated_count

    def validate_node(self, data_record):
        # set default return values
        errors = []
        warnings = []
        msg_prefix = f'[{data_record.get(ORIN_FILE_NAME)}: line {data_record.get("lineNumber")}]'
        node_type = data_record.get(NODE_TYPE)
        def_file_nodes = self.model.get_file_nodes()
        # submission-level validation
        sub_intention = self.submission.get(SUBMISSION_INTENTION)
        try:
            # call validate_required_props
            result_required= self.validate_required_props(data_record, msg_prefix) if sub_intention != SUBMISSION_INTENTION_DELETE else self.validate_file_name(data_record, def_file_nodes, node_type, msg_prefix)
            # call validate_prop_value
            result_prop_value = self.validate_props(data_record, msg_prefix) if sub_intention != SUBMISSION_INTENTION_DELETE else {}
            # call validate_relationship
            result_rel = self.validate_relationship(data_record, msg_prefix) if sub_intention != SUBMISSION_INTENTION_DELETE else {}

            # concatenation of all errors
            errors = result_required.get(ERRORS, []) +  result_prop_value.get(ERRORS, []) + result_rel.get(ERRORS, [])
            # concatenation of all warnings
            warnings = result_required.get(WARNINGS, []) +  result_prop_value.get(WARNINGS, []) + result_rel.get(WARNINGS, [])
            #check if existed nodes in release collection
            if sub_intention and sub_intention in [SUBMISSION_INTENTION_NEW_UPDATE, SUBMISSION_INTENTION_DELETE]:
                exist_releases = self.mongo_dao.search_released_node_with_status(self.submission[DATA_COMMON_NAME], node_type, data_record[NODE_ID], [SUBMISSION_REL_STATUS_RELEASED, None])
                if sub_intention == SUBMISSION_INTENTION_NEW_UPDATE and (exist_releases and len(exist_releases) > 0):
                    # check if file node
                    if not node_type in def_file_nodes:
                        warnings.append(create_error("M018", [msg_prefix, node_type, f'{self.model.get_node_id(node_type)}: {data_record[NODE_ID]}'],
                                                      NODE_ID, self.model.get_node_id(node_type)))
                    else:
                        warnings.append(create_error("M018", f'{msg_prefix} “{node_type}”: {{“{self.model.get_node_id(node_type)}": “{data_record[NODE_ID]}"}} already exists and will be updated. Its associated data file will also be replaced if uploaded.',
                                                      NODE_ID, self.model.get_node_id(node_type)))
                elif sub_intention == SUBMISSION_INTENTION_DELETE and (not exist_releases or len(exist_releases) == 0):
                    errors.append(create_error("M019", [msg_prefix, node_type, data_record[NODE_ID]], NODE_ID, self.model.get_node_id(node_type)))
            # if there are any errors set the result to "Error"
            if len(errors) > 0:
                return STATUS_ERROR, errors, warnings
            # if there are no errors but warnings,  set the result to "Warning"
            if len(warnings) > 0:
                return STATUS_WARNING, errors, warnings
        except Exception as e:
            self.log.exception(e)
            msg = f'Failed to validate dataRecords for the submission, {self.submission_id} at scope, {self.scope}!'
            self.log.exception(msg) 
            error = create_error("M020", [], "", "")
            return STATUS_ERROR,[error], None
        #  if there are neither errors nor warnings, return default values
        return STATUS_PASSED, errors, warnings
    
    def validate_required_props(self, data_record, msg_prefix):
        result = {"result": STATUS_ERROR, ERRORS: [], WARNINGS: []}
        # check the correct format from the data_record
        if "nodeType" not in data_record.keys() or "props" not in data_record.keys() or len(data_record[PROPERTIES].items()) == 0:
            result[ERRORS].append(create_error("M021", [msg_prefix],  "NodeType", ""))
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
            result[ERRORS].append(create_error("M022", [msg_prefix, id_property_key], id_property_key, ""))
        else:
            # check if duplicate records
            results = self.mongo_dao.search_nodes_by_index([{TYPE: node_type, KEY: id_property_key, VALUE_PROP: id_property_value}], self.submission[ID])
            if len(results) > 1:
                duplicates = ""
                for item in results:
                    if item[ID] == data_record[ID]:
                        continue
                    duplicates += f'"{item.get(ORIN_FILE_NAME)}" line {item.get("lineNumber")},'
                duplicates = duplicates.strip(",")    
                result[ERRORS].append(create_error("M016", [msg_prefix, duplicates], id_property_key, id_property_value))

        for data_key, data_value in data_record[PROPERTIES].items():
            anode_keys = anode_definition.keys()
            if "properties" not in anode_keys:
                result[ERRORS].append(create_error("Invalid data model", f'"properties" is not defined in the model.', "M026", "Error", "", ""))
                continue

            if data_key not in anode_definition["properties"].keys():
                result[WARNINGS].append(create_error("M017", [msg_prefix, data_key], data_key, data_value))
                continue

            # check missing required key and empty value
            if anode_definition["properties"][data_key]["required"]:
                if data_value is None or not str(data_value).strip():
                    result[ERRORS].append(create_error("M003",[msg_prefix, data_key], data_key, data_value))

        if len(result[WARNINGS]) > 0:
            result["result"] = STATUS_WARNING

        if len(result[ERRORS]) == 0 and len(result[WARNINGS]) == 0:
            result["result"] = STATUS_PASSED
        return result
    
    def validate_file_name(self, data_record, def_file_nodes, node_type, msg_prefix):
        result = {"result": STATUS_PASSED, ERRORS: [], WARNINGS: []}
        if node_type not in def_file_nodes.keys():
            return result
        
        # extract a file name property
        def_file_name = self.model.get_file_name()
        # check is file name property is empty
        if def_file_name not in data_record[PROPERTIES].keys() or not data_record[PROPERTIES].get(def_file_name) or not str(data_record[PROPERTIES][def_file_name]).strip():
            result[ERRORS].append(create_error("M003", [msg_prefix, def_file_name], def_file_name, str(data_record[PROPERTIES][def_file_name]).strip()))
            result["result"] = STATUS_ERROR

        return result
    
    def validate_props(self, dataRecord, msg_prefix):
        # set default return values
        errors = []
        props_def = self.model.get_node_props(dataRecord.get(NODE_TYPE))
        props = dataRecord.get(PROPERTIES)
        for k, v in props.items():
            prop_def = props_def.get(k)
            if not prop_def or v is None: 
                continue
            
            errs = self.validate_prop_value(k, v, prop_def, msg_prefix)
            if len(errs) > 0:
                errors.extend(errs)

        return {VALIDATION_RESULT: STATUS_ERROR if len(errors) > 0 else STATUS_PASSED, ERRORS: errors, WARNINGS: []}

    def get_parent_nodes(self, data_record_parent_nodes):
        parent_nodes = []
        for parent_node in data_record_parent_nodes:
            parent_type = parent_node.get("parentType")
            parent_id_property = parent_node.get("parentIDPropName")
            parent_id_value = parent_node.get("parentIDValue")
            if parent_type and parent_id_value and parent_id_value is not None:
                parent_nodes.append({"type": parent_type, "key": parent_id_property, "value": parent_id_value})
        exist_parent_nodes = self.mongo_dao.search_nodes_by_index(parent_nodes, self.submission[ID])
        parent_node_cache = []
        for node in exist_parent_nodes:
            if node.get(PROPERTIES):
                for key, value in node[PROPERTIES].items():
                    parent_node_cache.append(tuple([node.get("nodeType"), key, value]))
        return parent_node_cache
    
    def validate_relationship(self, data_record, msg_prefix):
        # set default return values
        result = {"result": STATUS_ERROR, ERRORS: [], WARNINGS: []}
        node_type = data_record.get(NODE_TYPE)
        node_id = data_record.get(NODE_ID)
        node_relationships = self.model.get_node_relationships(node_type)
        if not node_relationships or len(node_relationships) == 0: 
            result["result"] = STATUS_PASSED
            return result
        data_record_parent_nodes = data_record.get(PARENTS)
        if not data_record_parent_nodes or len(data_record_parent_nodes) == 0:
            result["result"] = STATUS_WARNING
            result[ERRORS].append(create_error("M013", [msg_prefix], node_type, node_id))
            return result

        node_keys = self.model.get_node_keys()
        node_relationships = self.model.get_node_relationships(node_type)
        parent_nodes = self.get_parent_nodes(data_record_parent_nodes)
        data_common = data_record.get(DATA_COMMON_NAME)
        multi_parents = []
        for parent_node in data_record_parent_nodes:
            parent_type = parent_node.get("parentType")
            if not parent_type or parent_type not in node_keys:
                result[ERRORS].append(create_error("M023", [msg_prefix, parent_type], node_type, node_id))
                continue

            parent_id_property = parent_node.get("parentIDPropName")
            model_properties = self.model.get_node_props(parent_type)

            if not model_properties or parent_id_property not in model_properties:
                result[ERRORS].append(create_error("M023", [msg_prefix, f'"{parent_id_property}" of "{parent_type}"'], node_type, node_id))
                continue
            # check node relationship
            if not node_relationships or not node_relationships.get(parent_type):
                result[ERRORS].append(create_error("M023",[msg_prefix, parent_type], node_type, node_id))
                continue

            # these should be defined in the data model in the properties
            is_parent_id_valid_format = self.model.get_node_props(parent_type)
            is_parent_id_exist = is_parent_id_valid_format and is_parent_id_valid_format.get(parent_id_property)
            if not is_parent_id_valid_format or not is_parent_id_exist:
                result[ERRORS].append(create_error("M023", [msg_prefix, f'“{parent_id_property}" of “{parent_type}”'], node_type, node_id))
                continue

            # collect all node_type, node_value, parentIDValue for the parent nodes
            parent_id_value = parent_node.get("parentIDValue")
            if parent_id_value is None or (isinstance(parent_id_value, str) and not parent_id_value.strip()):
                result[ERRORS].append(create_error("M023", [msg_prefix, f'“{parent_id_property}" of “{parent_type}”'], node_type, node_id))
                continue

            rel_type = node_relationships[parent_type].get(TYPE)
            # check if there is only one parent for both one_to_one and many_to_one relationships.
            if rel_type != "many_to_many": 
                if parent_node not in multi_parents:
                    multi_parents = [item for item in data_record_parent_nodes if item[PARENT_TYPE] == parent_type and item[PARENT_ID_NAME] == parent_id_property ]
                    if len(multi_parents) > 1:
                        error_type = "One-to-one relationship conflict" if rel_type == "one_to_one" else "Many-to-one relationship conflict"
                        parent_node_ids = [item[PARENT_ID_VAL] for item in multi_parents]
                        result[ERRORS].append(create_error("M024" if rel_type == "one_to_one" else "M025", 
                                    [msg_prefix, parent_type, json.dumps(parent_node_ids)], node_type, node_id))
                        
            has_parent = (parent_type, parent_id_property, parent_id_value) in parent_nodes
            if not has_parent:
                released_parent = self.mongo_dao.search_released_node(data_common, parent_type, parent_id_value)
                if not released_parent:
                    result[ERRORS].append(create_error("M014", [msg_prefix, parent_type, f'[“{parent_id_property}”: “{parent_id_value}"]'], node_type, node_id))
                else:
                    has_parent = True

            if has_parent and rel_type == "one_to_one":
                # check released and current children by current parent
                child_node_ids = self.get_unique_child_node_ids(data_common, node_type, parent_node, self.submission_id)
                if child_node_ids and len(child_node_ids) > 1:
                    result[ERRORS].append(create_error("M024", 
                                f'"{msg_prefix}": associated node “{parent_type}”: “{parent_id_value}" has multiple nodes associated: {json.dumps(child_node_ids)}.', node_type, node_id))

        if len(result[WARNINGS]) > 0:
            result["result"] = STATUS_WARNING

        if len(result[ERRORS]) == 0 and len(result[WARNINGS]) == 0:
            result["result"] = STATUS_PASSED
        return result
    
    def get_unique_child_node_ids(self, data_common, node_type, parent_node, submission_id):
        children = self.mongo_dao.get_nodes_by_parent_prop(node_type, parent_node, submission_id)
        if not children:
            return None
        child_id_list = list(set([item[NODE_ID] for item in children]))
        if len(child_id_list) > 1:
            return child_id_list
        else:
            children = self.mongo_dao.find_released_nodes_by_parent(node_type, data_common, parent_node)
            if children and len(children) > 0:
                child_id_list = list(set(child_id_list + [item[NODE_ID] for item in children]))
            return child_id_list

    def validate_prop_value(self, prop_name, value, prop_def, msg_prefix):
        # set default return values
        errors = []
        type = prop_def.get(TYPE)
        if not type or not type in valid_prop_types:
            errors.append(create_error("M009", [msg_prefix, prop_name, type], prop_name, value))
        else:
            val = None
            minimum = prop_def.get(MIN)
            maximum = prop_def.get(MAX)
            permissive_vals, msg = self.get_permissive_value(prop_def)
            if msg and msg == CDE_NOT_FOUND:
                errors.append(create_error("M027", [msg_prefix, prop_name], prop_name, value))
            if type == "string":
                val = str(value)
                result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name, self.mongo_dao)
                if not result:
                    errors.append(error)
            elif type == "integer":
                try:
                    val = int(value)
                except ValueError as e:
                    errors.append(create_error("M004",[msg_prefix, prop_name, value], prop_name, value))

                result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name, self.mongo_dao)
                if not result:
                    errors.append(error)

                errs = check_boundary(val, minimum, maximum, msg_prefix, prop_name)
                if len(errs) > 0:
                    errors.extend(errs)

            elif type == "number":
                try:
                    val = float(value)
                except ValueError as e:
                    errors.append(create_error("M005", [msg_prefix, prop_name, value], prop_name, value))
                result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name, self.mongo_dao)
                if not result:
                    errors.append(error)

                errs = check_boundary(val, minimum, maximum, msg_prefix, prop_name)
                if len(errs) > 0:
                    errors.extend(errs)

            elif type == "date" or type == "datetime":
                val = None
                for date_format in DATE_FORMATS:
                    try:
                        val = datetime.strptime(value, date_format)
                        break #if the value can be parsed with the format
                    except ValueError as e:
                        continue
                if val is None:
                    errors.append(create_error("M007",[msg_prefix, prop_name, value], prop_name, value))

            elif type == "boolean":
                if not isinstance(value, bool) and value not in ["yes", "true", "no", "false"]:
                    errors.append(create_error("M008",[msg_prefix, prop_name, value], prop_name, value))
            
            elif (type == "array" or type == "value-list"):
                if not permissive_vals or len(permissive_vals) == 0:
                    return errors #skip validation by crdcdh-1723
                val = str(value)
                list_delimiter = self.model.get_list_delimiter()
                arr = val.split(list_delimiter) if list_delimiter in val else [value]
                for item in arr:
                    val = item.strip() if item and isinstance(item, str) else item
                    result, error = check_permissive(val, permissive_vals, msg_prefix, prop_name, self.mongo_dao)
                    if not result:
                        errors.append(error)
            else:
                errors.append(create_error("M009", [msg_prefix, prop_name, value], prop_name, value))

        return errors
    
    """
    get permissible values of a property
    """
    def get_permissive_value(self, prop_def):
        permissive_vals = prop_def.get("permissible_values") 
        msg = None
        if prop_def.get(CDE_TERM) and len(prop_def.get(CDE_TERM)) > 0:
            # retrieve permissible values from DB or cde site
            cde_code = None
            cde_terms = [ct for ct in prop_def[CDE_TERM] if 'caDSR' in ct.get('Origin', '')]
            if cde_terms and len(cde_terms) > 0:
                cde_code = cde_terms[0].get(TERM_CODE) 
                cde_version = cde_terms[0].get(TERM_VERSION)
            if not cde_code:
                return permissive_vals
            
            cde = self.mongo_dao.get_cde_permissible_values(cde_code, cde_version)
            if cde:
                if cde.get(CDE_PERMISSIVE_VALUES) is not None: 
                    if len(cde.get(CDE_PERMISSIVE_VALUES)) > 0:
                        permissive_vals = cde[CDE_PERMISSIVE_VALUES]
                    else:
                        permissive_vals = None
            else:
                if not self.searched_sts:
                    cde = get_pv_by_datacommon_version_cde(self.config[TIER_CONFIG], self.submission[DATA_COMMON_NAME], 
                                                            self.submission[MODEL_VERSION], cde_code, cde_version, self.log, self.mongo_dao)
                    self.searched_sts = True
                    if cde:
                        if cde.get(CDE_PERMISSIVE_VALUES) is not None:
                            if len(cde[CDE_PERMISSIVE_VALUES]) > 0:
                                permissive_vals = cde[CDE_PERMISSIVE_VALUES]
                            else:
                                permissive_vals =  None #escape validation
                    else:
                        msg = CDE_NOT_FOUND
                        self.not_found_cde = True
                else: 
                    if self.not_found_cde:
                        msg = CDE_NOT_FOUND

                       
        # strip white space if the value is string
        if permissive_vals and len(permissive_vals) > 0 and isinstance(permissive_vals[0], str):
            permissive_vals = [item.strip() for item in permissive_vals]
        return permissive_vals, msg

"""util functions"""
def check_permissive(value, permissive_vals, msg_prefix, prop_name, dao):
    result = True,
    error = None
    # strip white space from input value
    if value and isinstance(value, str):
        value = value.strip()
    if permissive_vals and len(permissive_vals) > 0 and value not in permissive_vals:
        result = False
        error = create_error("M010", [msg_prefix, value, prop_name], prop_name, value)
        # check synonym
        synonyms = dao.find_pvs_by_synonym(value)
        if not synonyms or len(synonyms) == 0:
             return result, error 
        suggested_pvs = [item[PV_TERM] for item in synonyms]
        permissive_val = [item for item in permissive_vals if item in suggested_pvs]
        if permissive_val and len(permissive_val) > 0: 
            error["description"] += f' It is recommended to use "{permissive_val[0]}", as it is semantically equivalent to "{value}"' 
    return result, error

def check_boundary(value, min, max, msg_prefix, prop_name):
    errors = []
    if min and min.get(VALUE_PROP):
        val = min.get(VALUE_PROP)
        exclusive = min.get(VALUE_EXCLUSIVE)
        if (exclusive and value <= val) or (not exclusive and value < val):
            errors.append(create_error("M011", [msg_prefix, prop_name, value], prop_name, value))

    if max and max.get(VALUE_PROP):
        val = max.get(VALUE_PROP)
        exclusive = max.get(VALUE_EXCLUSIVE)
        if (exclusive and value >= val) or (not exclusive and value > val):
            errors.append(create_error("M012",[msg_prefix, prop_name, value], prop_name, value))      

    return errors

"""
get qc result for the node record by qc_id
"""
def get_qc_result(node, validation_type, mongo_dao):
    qc_id = node.get(QC_RESULT_ID) if validation_type == VALIDATION_TYPE_METADATA else node[S3_FILE_INFO].get(QC_RESULT_ID)
    qc_result = None
    if not qc_id:
        qc_result = create_new_qc_result(node, validation_type)
    else: 
        qc_result = mongo_dao.get_qcRecord(qc_id)
        if not qc_result:
            qc_result = create_new_qc_result(node, validation_type)
    return qc_result

def create_new_qc_result(node, validation_type):
    qc_result = {
        ID: get_uuid_str(),
        SUBMISSION_ID: node[SUBMISSION_ID],
        DATA_RECORD_ID: node[ID],
        QC_VALIDATION_TYPE: validation_type,
        BATCH_IDS: node[BATCH_IDS],
        LATEST_BATCH_ID: node[LATEST_BATCH_ID],
        DISPLAY_ID: node.get(LATEST_BATCH_DISPLAY_ID),
        TYPE: node[NODE_TYPE] if validation_type == VALIDATION_TYPE_METADATA else VALIDATION_TYPE_FILE,
        SUBMITTED_ID: node[NODE_ID] if validation_type == VALIDATION_TYPE_METADATA else node[S3_FILE_INFO].get("fileName"),
        UPLOADED_DATE: node.get(UPLOADED_DATE),
        QC_ORIGIN: QC_ORIGIN_METADATA_VALIDATE_SERVICE if validation_type == VALIDATION_TYPE_METADATA else QC_ORIGIN_FILE_VALIDATE_SERVICE
    }
    return qc_result