#!/usr/bin/env python3
import pandas as pd
import json
import os
from botocore.exceptions import ClientError
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import BATCH_STATUS, BATCH_TYPE_METADATA, DATA_COMMON_NAME, ERRORS, DB, \
    SUCCEEDED, ERRORS, S3_DOWNLOAD_DIR, SQS_NAME, BATCH_ID, BATCH_STATUS_LOADED, \
    BATCH_STATUS_REJECTED, ID, FILE_NAME, TYPE, STATUS_NEW, FILE_PREFIX
from common.utils import cleanup_s3_download_dir, get_exception_msg, dump_dict_to_json
from common.model_store import ModelFactory
from data_loader import DataLoader

VISIBILITY_TIMEOUT = 20
SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'

"""
Interface for essential validation of metadata via SQS
"""

def essentialValidate(configs, job_queue, mongo_dao):
    batches_processed = 0
    log = get_logger('Essential Validation Service')
    try:
        model_store = ModelFactory(configs) 
        # dump models to json files
        # dump_dict_to_json([model[MODEL] for model in model_store.models], f"tmp/data_models_dump.json")
        dump_dict_to_json(model_store.models, f"tmp/data_models_dump.json")
    except Exception as e:
        log.debug(e)
        log.exception(f'Error occurred when initialize essential validation service: {get_exception_msg()}')
        return 1
    validator = EssentialValidator(configs, mongo_dao, model_store)

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
                    if data.get(BATCH_ID):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        #1 call mongo_dao to get batch by batch_id
                        batch = mongo_dao.get_batch(data[BATCH_ID], configs[DB])
                        #2. validate batch and files.
                        
                        result = validator.validate(batch)
                        if result and len(validator.download_file_list) > 0:
                            #3. call mongo_dao to load data
                            data_loader = DataLoader(configs, model_store.get_model_by_data_common(validator.datacommon), mongo_dao)
                            result = data_loader.load_data(validator.download_file_list)
                            if result:
                                batch[BATCH_STATUS] = BATCH_STATUS_LOADED
                        else:
                            batch[BATCH_STATUS] = BATCH_STATUS_REJECTED
                       
                        #4. update batch
                        result = mongo_dao.update_batch( batch, configs[DB])
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
    
    def __init__(self, configs, mongo_dao, model_store):
        self.configs = configs
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('Essential Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.submission_id = None
        self.download_file_list= []

    def validate(self,batch):

        self.bucket = S3Bucket(batch.get("bucketName"))

        if not self.validate_batch(batch):
            return False

        for file_info in self.file_info_list: 
            #1. download the file in s3 and load tsv file into dataframe
            if not self.download_file(file_info):
                file_info[BATCH_STATUS] = "failed"
                return False
            #2. validate meatadata in self.df
            if not self.validate_data(file_info):
                file_info[BATCH_STATUS] = "failed"
                return False
        return True

    
    def validate_batch(self, batch):
        msg = None
        batch[ERRORS] = [] if not batch.get(ERRORS) else batch[ERRORS] 
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
            submission = self.mongo_dao.get_submission(batch.get("submissionID"), self.configs[DB])
            if not submission or not submission.get(DATA_COMMON_NAME):
                msg = f'Invalid batch, no datacommon found, {batch[ID]}!'
                self.log.error(msg)
                batch[ERRORS].append(msg)
                return False
            self.datacommon = submission.get(DATA_COMMON_NAME)
            self.submission_id  = submission[ID]
            return True
    
    def download_file(self, file_info):
        key = os.path.join(self.batch[FILE_PREFIX], file_info[FILE_NAME])
        # todo set download file 
        download_file = os.path.join(S3_DOWNLOAD_DIR, file_info[FILE_NAME])
        try:
            if self.bucket.file_exists_on_s3(key):
                self.bucket.download_file(key, download_file)
                if os.path.isfile(download_file):
                    df = pd.read_csv(download_file, sep=SEPARATOR_CHAR, header=0, encoding=UTF8_ENCODE)
                    self.df = df
                    self.download_file_list.append(download_file)

                return True # if no exception
        except ClientError as ce:
            self.df = None
            self.log.debug(ce)
            self.log.exception(f"Failed downloading file,{file_info.fileName} to {self.batch.bucketName}! {get_exception_msg()}.")
            file_info[ERRORS] = [f'Downloading file failed with S3 client error! {get_exception_msg()}.']
            return False
        except Exception as e:
            self.df = None
            self.log.debug(e)
            self.log.exception('Downloading file failed! Check debug log for detailed information.')
            file_info[ERRORS] = [f"Downloading file failed! {get_exception_msg()}."]
            return False
    
    def validate_data(self, file_info):
        """
        Metadata files must have a "type" column
        Metadata files must not have empty columns of empty rows
        Each row in a metadata file must have same number of columns as the header row
        When metadata intention is "New", all IDs must not exist in the database
        """
        msg = None
        file_info[ERRORS] = [] if not file_info.get(ERRORS) else file_info[ERRORS] 
        # check if missing "type" column
        if not TYPE in self.df.columns:
            msg = f'Invalid metadata, missing "type" column, {self.batch[ID]}!'
            self.log.error(msg)
            file_info[ERRORS].append(msg)
            self.batch[ERRORS].append(msg)
            return False
        
        # check if empty row.
        idx = self.df.index[self.df.isnull().all(1)]
        if not idx.empty: 
            msg = f'Invalid metadata, contains empty rows, {self.batch[ID]}!'
            self.log.error(msg)
            file_info[ERRORS].append(msg)
            return False
        
        # Each row in a metadata file must have same number of columns as the header row
        if '' in self.df.columns:
            msg = f'Invalid metadata, headers are match row columns, {self.batch[ID]}!'
            self.log.error(msg)
            file_info[ERRORS].append(msg)
            self.batch[ERRORS].append(msg)
            return False
        
        # When metadata intention is "New", all IDs must not exist in the database
        if self.batch['metadataIntention'] == STATUS_NEW:
            # verify if ids in the df in the mongo db.
            # get node type
            type = self.df[TYPE][0]

            # get id data fields for the type, the domain for mvp2/m3 is cds.
            id_field = self.model_store.get_node_id(self.datacommon , type)
            if not id_field: return True
            # extract ids from df.
            ids = self.df[id_field].tolist()  
            # query db.         
            if not self.mongo_dao.check_metadata_ids(type, ids, id_field, self.submission_id, self.configs[DB]):
                msg = f'Invalid metadata, identical data exists, {self.batch[ID]}!'
                self.log.error(msg)
                file_info[ERRORS].append(msg)
                self.batch[ERRORS].append(msg)
                return False

            return True


