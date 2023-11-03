#!/usr/bin/env python3

import pandas as pd
import io
import os
from botocore.exceptions import ClientError
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import BATCH_STATUS, BATCH_TYPE_METADATA, DATA_COMMON_NAME, ERRORS, BATCH_DB, \
    METADATA_DB, SUCCEEDED, ERRORS, S3_DOWNLOAD_DIR
from common.utils import clean_up_key_value, clean_up_strs, get_exception_msg



""" Requirement for the ticket crdcdh-343
For files: read manifest file and validate local files’ sizes and md5s
For metadata: validate data folder contains TSV or TXT files
Compose a list of files to be updated and their sizes (metadata or files)
"""

class MetaDataValidator:
    
    def __init__(self, configs, mongo_dao, model_store):
        self.configs = configs
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('Metadata_Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.data_frame_list = []

    def validate(self,batch):
        self.bucket = S3Bucket(batch.get("bucketName"))

        if not self.validate_batch(batch):
            return False

        for file_info in self.file_info_list: 
            #1. download the file in s3 and load tsv file into dataframe
            if not self.download_file(file_info):
                file_info[SUCCEEDED] = False
                return False
            #2. validate meatadata in self.df
            if not self.validate_data(file_info):
                file_info[SUCCEEDED] = False
                return False
        return True

    
    def validate_batch(self, batch):
        msg = None
        #This service only processes metadata batches, if a file batch is passed, it should be ignored (output an error message in the log).
        if batch.get("type") != BATCH_TYPE_METADATA:
            msg = f'Invalid batch type, only metadata allowed, {batch["_id"]}!'
            self.log.error(msg)
            return False

        if not batch.get("files") or len(batch["files"]) == 0:
            msg = f'Invalid batch, no files found, {batch["_id"]}!'
            self.log.error(msg)
            return False
        
        #Non-conformed metadata file Format, only TSV (.tsv or .txt) files are allowed
        file_info_list = [ file for file in batch["files"] if file.get("fileName") and file["fileName"].lower().endswith(".tsv") or file["fileName"].lower().endswith(".txt") ]
        if not file_info_list or len(file_info_list) == 0:
            msg = f'Invalid batch, no metadata files found, {batch["_id"]}!'
            self.log.error(msg)
            return False
        else:
            self.file_info_list = file_info_list
            self.batch = batch
            # get data common from submission
            submission = self.mongo_dao.get_submission(batch.get("submissionID"), self.configs[BATCH_DB])
            if not submission or not submission.get(DATA_COMMON_NAME):
                self.log.error(f'Invalid batch, no datacommon found, {batch["_id"]}!')
                return False
            self.datacommon = submission[DATA_COMMON_NAME]
            return None
    
    def download_file(self, file_info):
        key = f"{self.batch['filePrefix']}{file_info['fileName']}"
        # todo set download file 
        download_file = f"{S3_DOWNLOAD_DIR}/{file_info['fileName']}"
        try:
            if self.bucket.file_exists_on_s3(key):
                self.bucket.download_file(key, download_file)
                if os.path.isfile(download_file):
                    df = pd.read_csv(download_file, sep='\t', header=0, encoding='utf8')
                self.df = df
                self.data_frame_list.append(df)
                return True
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
        # check if missing "type" column
        if not 'type' in self.df.columns:
            msg = f'Invalid metadata, missing "type" column, {self.batch["_id"]}!'
            self.log.error(msg)
            file_info[ERRORS] = [msg]
            return False
        
        # check if empty row.
        idx = self.df.index[self.df.isnull().all(1)]
        if not idx.empty: 
            msg = f'Invalid metadata, contains empty rows, {self.batch["_id"]}!'
            self.log.error(msg)
            file_info[ERRORS] = [msg]
            return False
        
        # Each row in a metadata file must have same number of columns as the header row
        if '' in self.df.columns:
            msg = f'Invalid metadata, headers are match row columns, {self.batch["_id"]}!'
            self.log.error(msg)
            file_info[ERRORS] = [msg]
            return False
        
        # When metadata intention is "New", all IDs must not exist in the database
        if self.batch['metadataIntention'] == "New":
            # verify if ids in the df in the mongo db.
            # get node type
            type = self.df['type'][0]

            # get id data fields for the type, the domain for mvp2/m3 is cds.
            id_field = self.model_store.get_node_id(self.datacommon , type)
            if not id_field: return True
            # extract ids from df.
            ids = self.df[id_field].tolist()  
            # query db.         
            if not self.mongo_dao.check_metadata_ids(type, ids, id_field, self.configs[METADATA_DB]):
                msg = f'Invalid metadata, identical data exists, {self.batch["_id"]}!'
                self.log.error(msg)
                file_info[ERRORS] = [msg]
                return False

            return True


