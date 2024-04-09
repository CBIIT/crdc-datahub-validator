#!/usr/bin/env python3
import pandas as pd
import json
from datetime import datetime
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger, DATE_FORMATS, DATETIME_FORMAT
from common.constants import SQS_NAME, SQS_TYPE, SCOPE, SUBMISSION_ID, ERRORS, ADDITION_ERRORS, STATUS_ERROR, ID, FAILED, \
    STATUS_WARNING, STATUS_PASSED, STATUS, UPDATED_AT, MODEL_FILE_DIR, TIER_CONFIG, DATA_COMMON_NAME, MODEL_VERSION, \
    NODE_TYPE, PROPERTIES, TYPE, MIN, MAX, VALUE_EXCLUSIVE, VALUE_PROP, VALIDATION_RESULT, SUBMISSION_INTENTION, \
    VALIDATED_AT, SERVICE_TYPE_METADATA, NODE_ID, PROPERTIES, PARENTS, KEY, INTENTION_NEW, INTENTION_DELETE, \
    SUBMISSION_REL_STATUS_RELEASED, ORIN_FILE_NAME, TYPE_METADATA_VALIDATE, TYPE_CROSS_SUBMISSION
from common.utils import current_datetime, get_exception_msg, dump_dict_to_json, create_error
from common.model_store import ModelFactory
from common.model_reader import valid_prop_types

BATCH_SIZE = 1000

class CrossSubmissionValidator:
    def __init__(self, mongo_dao, model_store):
        self.log = get_logger('Cross Submission Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.model = None
        self.submission = None
        self.isError = None

    def validate(self, submission_id):
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
            data_records = self.mongo_dao.get_dataRecords_chunk(submission_id, None, start_index, BATCH_SIZE)
            if start_index == 0 and (not data_records or len(data_records) == 0):
                msg = f'No metadata to be validated.'
                self.log.error(msg)
                return FAILED
            
            count = len(data_records) 
            validated_count += self.validate_nodes(data_records, submission_id, None)
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
                status, errors = self.validate_node(record)
                
                if errors and len(errors) > 0:
                    self.isError = True
                # set status, errors and warnings
                record[ADDITION_ERRORS] = errors
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
        result = self.mongo_dao.update_data_records(updated_records)
        if not result:
            #4. set errors in submission
            msg = f'Failed to update dataRecords for the submission, {submission_id} at scope, {scope}!'
            self.log.error(msg)
            self.isError = True

        return validated_count
    
    def validate_node(self, data_record):
        # set default return values
        errors = []
        msg_prefix = f'[{data_record.get(ORIN_FILE_NAME)}: line {data_record.get("lineNumber")}]'
        node_keys = self.model.get_node_keys()
        node_type = data_record.get(NODE_TYPE)
        if not node_type or node_type not in node_keys:
            return STATUS_ERROR,[create_error("Invalid node type", f'{msg_prefix} Node type “{node_type}” is not defined')], None
        try:
            # validate cross submission
            results = self.mongo_dao.find_node_in_other_submission( self.submission[ID], self.submission[DATA_COMMON_NAME], node_type, data_record[NODE_ID])
            # if there are any errors set the result to "Error"
            if len(errors) > 0:
                return STATUS_ERROR, errors
            # if there are no errors but warnings,  set the result to "Warning"
        except Exception as e:
            self.log.exception(e) 
            error = create_error("Internal error", "{msg_prefix} metadata validation failed due to internal errors.  Please try again and contact the helpdesk if this error persists.")
            return STATUS_ERROR,[error], None
        #  if there are neither errors nor warnings, return default values
        return STATUS_PASSED, errors