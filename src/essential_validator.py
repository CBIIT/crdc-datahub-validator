#!/usr/bin/env python3
import pandas as pd
import numpy as np
import re
import json
import os
from botocore.exceptions import ClientError
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import STATUS, BATCH_TYPE_METADATA, DATA_COMMON_NAME, ROOT_PATH, NODE_ID, \
    ERRORS, S3_DOWNLOAD_DIR, SQS_NAME, BATCH_ID, BATCH_STATUS_UPLOADED, SQS_TYPE, TYPE_LOAD, STATUS_PASSED,\
    BATCH_STATUS_FAILED, ID, FILE_NAME, TYPE, FILE_PREFIX, MODEL_VERSION, MODEL_FILE_DIR, \
    TIER_CONFIG, STATUS_ERROR, STATUS_NEW, SERVICE_TYPE_ESSENTIAL, SUBMISSION_ID, SUBMISSION_INTENTION_DELETE, NODE_TYPE, \
    SUBMISSION_INTENTION, TYPE_DELETE, BATCH_BUCKET, METADATA_VALIDATION_STATUS, STATUS_WARNING, DCF_PREFIX, NODE_IDS, DELETE_ALL, EXCLUSIVE_IDS
from common.utils import cleanup_s3_download_dir, get_exception_msg, dump_dict_to_json, removeTailingEmptyColumnsAndRows, validate_uuid_by_rex, get_date_time
from common.model_store import ModelFactory
from metadata_remover import MetadataRemover
from data_loader import DataLoader
from service.ecs_agent import set_scale_in_protection

VISIBILITY_TIMEOUT = 20
SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
BATCH_ERROR_LIMIT = 1000
FILE_ERROR_LIMIT = 100

"""
Interface for essential validation of metadata via SQS
"""

def essentialValidate(configs, job_queue, mongo_dao):
    batches_processed = 0
    log = get_logger('Essential Validation Service')
    try:
        model_store = ModelFactory(configs[MODEL_FILE_DIR], configs[TIER_CONFIG]) 
        # dump models to json files
        # dump_dict_to_json([model[MODEL] for model in model_store.models], f"tmp/data_models_dump.json")
        # dump_dict_to_json(model_store.models, f"models/data_model.json")
    except Exception as e:
        log.exception(e)
        log.exception(f'Error occurred when initialize essential validation service: {get_exception_msg()}')
        return 1
    #step 3: run validator as a service
    scale_in_protection_flag = False
    #cleanup contents in the s3 download dir
    cleanup_s3_download_dir(S3_DOWNLOAD_DIR)
    log.info(f'{SERVICE_TYPE_ESSENTIAL} service started')
    while True:
        try:
            msgs = job_queue.receiveMsgs(VISIBILITY_TIMEOUT)
            if len(msgs) > 0:
                log.info(f'New message is coming: {configs[SQS_NAME]}, '
                         f'{batches_processed} batches have been processed so far')
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
                data_loader = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    # Make sure job is in correct format
                    if data.get(SQS_TYPE) == TYPE_LOAD and data.get(BATCH_ID):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        #1 call mongo_dao to get batch by batch_id
                        batch = mongo_dao.get_batch(data[BATCH_ID])
                        if not batch:
                            log.error(f"No batch find for {data[BATCH_ID]}")
                            batches_processed +=1
                            msg.delete()
                            continue
                        #2. validate batch and files.
                        validator = EssentialValidator(mongo_dao, model_store)
                        try:
                            result = validator.validate(batch)
                            if result and validator.download_file_list and len(validator.download_file_list) > 0:
                                #3. call mongo_dao to load data
                                data_loader = DataLoader(validator.model, batch, mongo_dao, validator.bucket, validator.root_path, validator.datacommon, validator.submission)
                                result, errors = data_loader.load_data(validator.download_file_list)
                                if result:
                                    batch[STATUS] = BATCH_STATUS_UPLOADED
                                    submission_meta_status = STATUS_NEW
                                else:
                                    batch[STATUS] = BATCH_STATUS_FAILED
                                    submission_meta_status = BATCH_STATUS_FAILED
                            else:
                                batch[STATUS] = BATCH_STATUS_FAILED
                                submission_meta_status = BATCH_STATUS_FAILED

                        except Exception as e:  # catch any unhandled errors
                            error = f'{batch[SUBMISSION_ID]}: Failed to upload metadata for the batch, {batch[ID]}, {get_exception_msg()}!'
                            log.error(error)
                            batch[ERRORS] =  ['Batch loading failed - internal error.  Please try again and contact the helpdesk if this error persists.']
                            batch[STATUS] = BATCH_STATUS_FAILED
                            submission_meta_status = STATUS_ERROR
                        finally:
                            #5. update submission's metadataValidationStatus
                            if batch[ERRORS] and len(batch[ERRORS]) > BATCH_ERROR_LIMIT:
                                batch[ERRORS] = batch[ERRORS][:BATCH_ERROR_LIMIT]
                            mongo_dao.update_batch(batch)
                            if validator.submission and submission_meta_status == STATUS_NEW:
                                mongo_dao.set_submission_validation_status(validator.submission, None, submission_meta_status, None, None)
                    
                    elif data.get(SQS_TYPE) == TYPE_DELETE and data.get(SUBMISSION_ID) and data.get(NODE_TYPE):
                        # if both nodeIDs and deleteAll are not provided, raise error
                        if not (data.get(NODE_IDS) or data.get(DELETE_ALL)):
                            raise ValueError(f'Invalid message: {data}!')
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        submission_id = data.get(SUBMISSION_ID)
                        node_type = data.get(NODE_TYPE)
                        node_ids = data.get(NODE_IDS)
                        delete_all = data.get(DELETE_ALL)
                        exclusive_ids = data.get(EXCLUSIVE_IDS)
                        validator = MetadataRemover(mongo_dao, model_store)
                        try:
                            if delete_all:
                                # get all node ids for the node type in the submission
                                node_type_ids = mongo_dao.search_nodes_by_type_and_submission(node_type, submission_id, exclusive_ids)
                                node_ids = node_type_ids
                            result = validator.remove_metadata(submission_id, node_type, node_ids)
                        except Exception as e:  # catch any unhandled errors
                            error = f'{submission_id}: Failed to delete metadata, {get_exception_msg()}!'
                            log.error(error)
                        finally:
                            #5. update submission's metadataValidationStatus
                            if validator.submission:
                                status = validator.submission.get(METADATA_VALIDATION_STATUS)
                                # only need update the status if error or warning. In the dao function will check the count of error or warning to get real time status.
                                status = STATUS_PASSED if status in [STATUS_ERROR, STATUS_WARNING] else status 
                                mongo_dao.set_submission_validation_status(validator.submission, None, status, None, None, True)
                    else:
                        log.error(f'Invalid message: {data}!')

                    log.info(f'Processed {SERVICE_TYPE_ESSENTIAL} validation for the batch, {data.get(BATCH_ID)}!')
                    batches_processed += 1
                    msg.delete()
                except Exception as e:
                    log.exception(e)
                    log.critical(
                        f'Something wrong happened while processing file! Check debug log for details.')
                finally:
                    # msg.delete()
                    if data_loader:
                        del data_loader
                    if validator:
                        validator.close()
                        del validator
                    if extender:
                        extender.stop()
                        extender = None
                    #cleanup contents in the s3 download dir
                    cleanup_s3_download_dir(S3_DOWNLOAD_DIR)

        except KeyboardInterrupt:
            log.info('Good bye!')
            return


""" Requirement for the ticket crdcdh-496
Non-conformed metadata file Format, only TSV (.tsv or .txt) files are allowed
Metadata files must have a "type" column, values in the column must be a valid node type defined in the data model
Metadata files must not have empty columns of empty rows
Each row in a metadata file must have same number of columns as the header row
When metadata intention is "New", all IDs must not exist in the database or current file being validated
"""
class EssentialValidator:
    
    def __init__(self, mongo_dao, model_store):
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('Essential Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.datacommon = None
        self.model = None
        self.submission = None
        self.submission_id = None
        self.root_path = None
        self.download_file_list = None
        self.bucket = None
        self.batch = None
        self.def_file_nodes = None
        self.def_file_name = None

    def validate(self,batch):
        self.bucket = S3Bucket(batch.get(BATCH_BUCKET))
        if not self.validate_batch(batch):
            return False
        self.def_file_nodes = self.model.get_file_nodes()
        self.def_file_name = self.model.get_file_name()
        try:
            for file_info in self.file_info_list: 
                #1. download the file in s3 and load tsv file into dataframe
                if not self.download_file(file_info):
                    file_info[STATUS] = STATUS_ERROR
                    # return False
                    continue
                #2. validate meatadata in self.df
                if not self.validate_data(file_info):
                    file_info[STATUS] = STATUS_ERROR
                if len(file_info[ERRORS]) > FILE_ERROR_LIMIT:
                    file_info[ERRORS] =  file_info[ERRORS][:FILE_ERROR_LIMIT]  
            return True if len(self.batch[ERRORS]) == 0 else False
        except Exception as e:
            self.log.exception(e)
            msg = f'Failed to validate the batch files, {get_exception_msg()}!'
            self.log.exception(msg)
            batch[ERRORS].append(f'Batch validation failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return False
        return True
    
    def validate_batch(self, batch):
        msg = None
        batch[ERRORS] = []
        #This service only processes metadata batches, if a file batch is passed, it should be ignored (output an error message in the log).
        if batch.get(TYPE) != BATCH_TYPE_METADATA:
            msg = f'Invalid batch type, only metadata allowed, {batch[ID]}!'
            self.log.error(msg)
            batch[ERRORS].append(msg)
            return False

        if not batch.get("files") or len(batch["files"]) == 0:
            msg = f'Invalid batch, no files found, {batch[ID]}!'
            self.log.error(msg)
            batch[ERRORS].append(msg)
            return False
        
        #Non-conformed metadata file Format, only TSV (.tsv or .txt) files are allowed
        file_info_list = [ file for file in batch["files"] if file.get(FILE_NAME) and file[FILE_NAME].lower().endswith(".tsv") or file[FILE_NAME].lower().endswith(".txt") ]
        if not file_info_list or len(file_info_list) == 0:
            msg = f'Invalid batch, no metadata files found, {batch[ID]}!'
            self.log.error(msg)
            batch[ERRORS].append(msg)
            return False
        else:
            self.file_info_list = file_info_list
            self.batch = batch
            # get data common from submission
            submission = self.mongo_dao.get_submission(batch.get(SUBMISSION_ID))
            if not submission or not submission.get(DATA_COMMON_NAME):
                msg = f'Invalid batch, no datacommon found, {batch[ID]}!'
                self.log.error(msg)
                batch[ERRORS].append(msg)
                return False
            self.submission = submission
            self.datacommon = submission.get(DATA_COMMON_NAME)
            self.submission_id  = submission[ID]
            self.submission_intention = submission.get(SUBMISSION_INTENTION)
            self.root_path = submission.get(ROOT_PATH)
            self.download_file_list = []
            model_version = submission.get(MODEL_VERSION) 
            self.model = self.model_store.get_model_by_data_common_version(self.datacommon, model_version)
            if not self.model.model or not self.model.get_nodes():
                msg = f'{self.datacommon} model version "{model_version}" is not available.'
                self.log.error(msg)
                batch[ERRORS].append(msg)
                return False
            return True
    
    def download_file(self, file_info):
        key = os.path.join(self.batch[FILE_PREFIX], file_info[FILE_NAME])
        # todo set download file 
        download_file = os.path.join(S3_DOWNLOAD_DIR, file_info[FILE_NAME])
        msg = None
        try:
            if not self.bucket.file_exists_on_s3(key):
                msg = f'Reading metadata file “{file_info[FILE_NAME]}.” failed - file not found.'
                self.log.exception(msg)
                file_info[ERRORS] = [msg]
                self.batch[ERRORS].append(msg)
                return False
            self.bucket.download_file(key, download_file)
            if os.path.isfile(download_file):
                df = pd.read_csv(download_file, sep=SEPARATOR_CHAR, header=0, dtype='str', encoding=UTF8_ENCODE, keep_default_na=False, na_values=[''])
                self.df = (df.rename(columns=lambda x: x.strip())).apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # stripe white space.
                self.download_file_list.append(download_file)
            return True # if no exception
        except ClientError as ce:
            self.df = None
            self.log.exception(ce)
            self.log.exception(f"Failed to download file, {file_info[FILE_NAME]}. {get_exception_msg()}.")
            msg = f'Reading metadata file “{file_info[FILE_NAME]}.” failed - network error. Please try again and contact the helpdesk if this error persists.'
            file_info[ERRORS] = [msg]
            self.batch[ERRORS].append(msg)
            return False
        except pd.errors.ParserError as pe:
            self.df = None
            self.log.exception(pe)
            msg = get_exception_msg()
            self.log.exception(f'Invalid metadata file! {msg}.')
            msg = msg.split(":")[-1].strip()
            line_number = None
            if " line " in msg and "," in msg:
                line_number = msg.split(" line ")[1].split(",")[0]
            if line_number and line_number.strip():
                msg = f'“{file_info[FILE_NAME]}: line {line_number.strip()}": {" ".join(msg.split(" in line " + line_number + ", "))}.'
            else:
                msg = f'“{file_info[FILE_NAME]}”: {msg}.'
            file_info[ERRORS] = [msg]
            self.batch[ERRORS].append(msg)
            return False
        except UnicodeDecodeError as ue:
            self.df = None
            self.log.exception(ue)
            self.log.exception('Invalid metadata file! non UTF-8 character(s) found.')
            msg = f'“{file_info[FILE_NAME]}”: non UTF-8 character(s) found.'
            file_info[ERRORS] = [msg]
            self.batch[ERRORS].append(msg)
            return False
        except Exception as e:
            self.df = None
            self.log.exception(e)
            self.log.exception('Invalid metadata file! Check debug log for detailed information.')
            msg = f'“{file_info[FILE_NAME]}”: is not a valid TSV file.'
            file_info[ERRORS] = [msg]
            self.batch[ERRORS].append(msg)
            return False
    
    def validate_data(self, file_info):
        """
        Metadata files must have a "type" column
        Metadata files must not have empty columns of empty rows
        Each row in a metadata file must have same number of columns as the header row
        When metadata intention is "New", all IDs must not exist in the database
        """
        msg = None
        type= None
        file_info[ERRORS] = [] if not file_info.get(ERRORS) else file_info[ERRORS] 
        
        # check if there are rows
        if len(self.df.index) == 0:
            msg = f'“{file_info[FILE_NAME]}": no metadata in the file.'
            self.log.error(msg)
            file_info[ERRORS].append(msg)
            self.batch[ERRORS].append(msg)
            return False
        
        # remove tailing empty columns and rows
        self.df = removeTailingEmptyColumnsAndRows(self.df)

        # check if there are rows after trimmed empty rows
        if len(self.df.index) == 0:
            msg = f'“{file_info[FILE_NAME]}": no metadata in the file.'
            self.log.error(msg)
            file_info[ERRORS].append(msg)
            self.batch[ERRORS].append(msg)
            return False 
        
        # Each row in a metadata file must have same number of columns as the header row
        # dataframe will set the column name to "Unnamed: {index}" when parsing a tsv file with empty header.
        columns = self.df.columns.tolist()
        empty_cols = [col for col in columns if not col or "Unnamed:" in col ]
        if empty_cols and len(empty_cols) > 0:
            for col in empty_cols:
                extra_cell_list = self.df[self.df[col].notna()].index.astype(int).tolist()
                if extra_cell_list and len(extra_cell_list):
                    for index in extra_cell_list: 
                        msg = f'“{file_info[FILE_NAME]}: line {index + 2}": extra columns/cells found.'
                        self.log.error(msg)
                        file_info[ERRORS].append(msg)
                        self.batch[ERRORS].append(msg)
                else:
                    msg = f'“{file_info[FILE_NAME]}": empty column(s) found.'
                    self.log.error(msg)
                    file_info[ERRORS].append(msg)
                    self.batch[ERRORS].append(msg)
                    break

        # check duplicate columns.
        for col in columns:
            if ".1" in col and col.replace(".1", "") in columns:
                msg = f'“{file_info[FILE_NAME]}": multiple columns with the same header ("{col.replace(".1", "")}") is not allowed.'
                self.log.error(msg)
                file_info[ERRORS].append(msg)
                self.batch[ERRORS].append(msg)

        # check if empty row.
        idx = self.df.index[self.df.isnull().all(1)]
        if not idx.empty: 
            for index in idx:
                msg = f'“{file_info[FILE_NAME]}: line {index + 2}": empty row found.'
                self.log.error(msg)
                file_info[ERRORS].append(msg)
                self.batch[ERRORS].append(msg)
            return False
        
        # check if missing "type" column
        if not TYPE in columns:
            msg = f'“{file_info[FILE_NAME]}”: “type” column is required.'
            self.log.error(msg)
            file_info[ERRORS].append(msg)
            self.batch[ERRORS].append(msg)
            file_info[NODE_TYPE] = ""
            return False
        else: 
            type = self.df[TYPE].iloc[0]
            file_info[NODE_TYPE] = type if not pd.isnull(type) else ""
            nan_count = self.df.isnull().sum()[TYPE] #check if any rows with empty node type
            if nan_count > 0: 
                for index in self.df[self.df[TYPE].isnull()].index.astype(int).tolist():
                    msg = f'“{file_info[FILE_NAME]}: line {index + 2}": “type” value is required.'
                    self.log.error(msg)
                    file_info[ERRORS].append(msg)
                    self.batch[ERRORS].append(msg)
                return False
            else:
                node_types = self.model.get_node_keys()
                line_num = 2
                if type not in node_types:
                    msg = f'“{file_info[FILE_NAME]}: {line_num}": Node type “{type}” is not defined.'
                    self.log.error(msg)
                    file_info[ERRORS].append(msg)
                    self.batch[ERRORS].append(msg)
                    return False
                
                types = self.df[TYPE].tolist() 
                unique_types = set(types)
                if len(unique_types) > 1: # check if all type values are the same
                    for node_type in self.df[TYPE]:
                        if type != node_type:
                            msg = f'“{file_info[FILE_NAME]}: {line_num}": Node type “{node_type}” is different from "{type}", only one node type is allowed.'
                            self.log.error(msg)
                            file_info[ERRORS].append(msg)
                            self.batch[ERRORS].append(msg)
                            return False
                        line_num += 1

        id_field = self.model.get_node_id(type)
        # check if missing id property
        if id_field and not id_field in columns: 
            msg = f'“{file_info[FILE_NAME]}”: Key property “{id_field}” is required.'
            self.log.error(msg)
            file_info[ERRORS].append(msg)
            self.batch[ERRORS].append(msg)
            return False
        #check if id property value is empty
        nan_count = self.df.isnull().sum()[id_field]
        # check if the node has composition id (user story CRDCDh-2631)
        composition_key = self.model.get_composition_key(type)
        # validate composition key properties
        if composition_key:
            # loop through all rows and check if all properties in composition key (array) values are empty row by row
            for index, row in self.df.iterrows():
                hasVal = False
                if not pd.isnull(row[id_field]):
                    continue
                for prop in composition_key:
                    # raise error if any columns required by the composite key is not present
                    if prop not in columns:
                        msg = f'“{file_info[FILE_NAME]}”: Column "{prop}" is required to generate composite ID.'
                        self.log.error(msg)
                        file_info[ERRORS].append(msg)
                        self.batch[ERRORS].append(msg)
                        return False
                    if not pd.isnull(row[prop]):
                        hasVal = True
                        break
                if not hasVal:
                    msg = f'“{file_info[FILE_NAME]}:{index + 2}”: all properties ({", ".join(composition_key)}) needed for composite ID are missing.'
                    self.log.error(msg)
                    file_info[ERRORS].append(msg)
                    self.batch[ERRORS].append(msg)
                    return False
        if nan_count > 0 and not composition_key: 
            nan_rows = self.df[self.df[id_field].isnull()].to_dict("index")
            for key in nan_rows.keys():
                msg = f'“{file_info[FILE_NAME]}:{key + 2}”:  Key property “{id_field}” value is required.'
                self.log.error(msg)
                file_info[ERRORS].append(msg)
                self.batch[ERRORS].append(msg)
            return False
        # check if file id property value is valid
        isFileNode = type in self.def_file_nodes
        if isFileNode:
            ids = self.df[id_field].tolist()
            index = 2
            isValidId = True
            for id in ids:
                result, msg = self.validate_file_id(id_field, id, file_info, index)
                if not result:
                    self.log.error(msg)
                    isValidId = False
                    file_info[ERRORS].append(msg)
                    if len(self.batch[ERRORS]) <= BATCH_ERROR_LIMIT:
                        self.batch[ERRORS].append(msg)
                    else: 
                        return False
                index += 1
            if not isValidId:
                return False
        if self.submission_intention != SUBMISSION_INTENTION_DELETE: 
            # check missing required proper 
            required_props = self.model.get_node_req_props(type)
            missed_props = [ prop for prop in required_props if prop not in columns and prop != id_field]
            if len(missed_props) > 0:
                msg = f'“{file_info[FILE_NAME]}”: '
                msg += f'Properties {json.dumps(missed_props)} are required.' if len(missed_props) > 1 else f'Property "{missed_props[0]}" is required.'
                self.log.error(msg)
                file_info[ERRORS].append(msg)
                self.batch[ERRORS].append(msg)
            # check relationship
            rel_props = [rel for rel in columns if "." in rel and not re.search('\.\d*$',rel)]
            rel_result, msgs = self.check_relationship(file_info, type, rel_props)
            if not rel_result:
                self.log.error(msgs)
                file_info[ERRORS].extend(msgs)
                self.batch[ERRORS].extend(msgs)
            # check duplicate rows with the same nodeID
            duplicate_ids = self.df[id_field][self.df[id_field].duplicated()].tolist() 
            if len(duplicate_ids) > 0:
                if len(rel_props) == 0 or not rel_result:
                    duplicated_rows = self.df[self.df[id_field].isin(duplicate_ids)].to_dict("index")
                    for key, val in duplicated_rows.items():
                        msg = f'“{file_info[FILE_NAME]}:{key + 2}”: duplicated data detected: “{id_field}”: "{val[id_field]}".'
                        self.log.error(msg)
                        file_info[ERRORS].append(msg)
                        self.batch[ERRORS].append(msg)
                    return False  
                # check many-to-many relationship
                result = self.check_m2m_relationship(columns, duplicate_ids, id_field, rel_props, file_info)
                if not result:
                    return False 
        else:
            if isFileNode:
                # check is file name property is empty
                if self.def_file_name not in columns:
                    msg = f'“{file_info[FILE_NAME]}”: '
                    msg += f'Property "{self.def_file_name}" is required.'
                    self.log.error(msg)
                    file_info[ERRORS].append(msg)
                    self.batch[ERRORS].append(msg)
                    return False
                #check if file name property value is empty
                nan_count = self.df.isnull().sum()[self.def_file_name]
                if nan_count > 0: 
                    nan_rows = self.df[self.df[self.def_file_name].isnull()].to_dict("index")
                    for key in nan_rows.keys():
                        msg = f'“{file_info[FILE_NAME]}:{key + 2}”:  file name property “{self.def_file_name}” value is required.'
                        self.log.error(msg)
                        file_info[ERRORS].append(msg)
                        self.batch[ERRORS].append(msg)
                    return False
        
        return True if len(self.batch[ERRORS]) == 0 else False
    
    """
    check if id field value is valid
    """
    def validate_file_id(self, id_field, id, file_info, lineNum):
        omit_prefix = self.model.get_omit_dcf_prefix()
        # check if is is uuid
        # check if file id prefix based on data model OMIT_DCF_PREFIX
        low_id = id.lower()
        if omit_prefix:
            msg = f'“{file_info[FILE_NAME]}:line {lineNum}”: "{id}" is not in correct format for file ID (property {id_field}). A correct file ID should look like "e041576e-3595-5c8b-b0b3-272bc7cb6aa8".'
            if low_id.startswith(DCF_PREFIX.lower()):
                return False, msg
            else:
                if not validate_uuid_by_rex(low_id):
                    return False, msg
        else:
            msg = msg = f'“{file_info[FILE_NAME]}:line {lineNum}”: "{id}" is not in correct format for file ID (property {id_field}). A correct file ID should look like "dg.4DFC/e041576e-3595-5c8b-b0b3-272bc7cb6aa8".'   
            if not low_id.startswith(DCF_PREFIX.lower()):
                return False, msg
            else:
                uuid = low_id.split('/')[1]
                if not validate_uuid_by_rex(uuid):
                    return False, msg
        return True, None
        
    """
    validate many to many relationship
    """
    def check_m2m_relationship(self, columns, duplicate_ids, id_field, rel_props, file_info):
        rtn_val = True
        for id in duplicate_ids:
            duplicate_rows = self.df[self.df[id_field] == id].to_dict('index')
            row_cnt = len(duplicate_rows)
            is_m2m = False
            for rel in rel_props:
                unique_rel_values = set([item[rel] for item in duplicate_rows.values()])
                if row_cnt == len(list(unique_rel_values)):
                    is_m2m = True
                    break

            if not is_m2m: # not a m2m rel or contain duplicate rel values
                for key in duplicate_rows.keys():
                    msg = f'“{file_info[FILE_NAME]}:{key + 2}”: duplicated data detected: “{id_field}”: {id}.'
                    self.log.error(msg)
                    file_info[ERRORS].append(msg)
                    if len(self.batch[ERRORS]) <= BATCH_ERROR_LIMIT:
                        self.batch[ERRORS].append(msg)
                rtn_val = False  
                break
            else:
                other_props = [col for col in columns if col not in rel_props + [TYPE, id_field]]
                for prop in other_props:
                    unique_prop_values =  list(set([item[prop] for item in duplicate_rows.values()]))
                    if len(unique_prop_values) > 1:
                        for value in unique_prop_values:
                            index = next(key for key, val in duplicate_rows.items() if val[prop] == value)
                            msg = f'“{file_info[FILE_NAME]}: {index + 2}”: conflict data detected: “{prop}”: "{value}".'
                            self.log.error(msg)
                            file_info[ERRORS].append(msg)
                            if len(self.batch[ERRORS]) <= BATCH_ERROR_LIMIT:
                                self.batch[ERRORS].append(msg)
                            else:
                                return False
                        rtn_val = False
                        break
        return rtn_val
    """
    validate relationship
    """
    def check_relationship(self, file_info, type, rel_props):
        def_rel = self.model.get_node_relationships(type)
        if not def_rel or len(def_rel.keys()) == 0:
            if len(rel_props) == 0:
                return True, None
            else: # check if invalid relationships
                msgs = []
                for item in rel_props:
                    msgs.append(f'“{file_info[FILE_NAME]}”: Relationship column "{item}" is invalid, "{type}" node should not have any relationship columns.')
                return False, msgs
        
        # check if has relationship
        if len(rel_props) == 0:
            return False, [f'“{file_info[FILE_NAME]}”: All relationship columns are missing. Please ensure at least one relationship column is included.']
        
        def_rel_nodes = [ key for key in def_rel.keys()]
        rel_props_dic = {rel.split(".")[0]: rel.split(".")[1] for rel in rel_props}
        rel_props_dic_types = rel_props_dic.keys()
        
        # check if parent node is valid
        def_node_types = self.model.get_node_keys()
        invalid_types = [node for node in rel_props_dic_types if node not in def_node_types]
        if len(invalid_types) > 0:
            msgs = []
            for item in invalid_types:
                msgs.append(f'“{file_info[FILE_NAME]}”: Related node "{item}" is not defined.')
            return False, msgs
       
        # check if relationship is valid
        invalid_parents = [node for node in rel_props_dic_types if node not in def_rel_nodes]
        if len(invalid_parents) > 0:
            msgs = []
            for item in invalid_parents:
                msgs.append(f'“{file_info[FILE_NAME]}”: Relationship to "{item}" node is not defined.')
            return False, msgs
        
        # check if parent id prop is valid
        for parent_type in rel_props_dic_types:
            if rel_props_dic[parent_type] != self.model.get_node_id(parent_type):
                return False, [f'“{file_info[FILE_NAME]}”: "{rel_props_dic[parent_type]}" is not Key property of "{parent_type}", please use "{self.model.get_node_id(parent_type)}" instead.']
        return True, None
    
    def close(self):
        if self.bucket:
            del self.bucket


