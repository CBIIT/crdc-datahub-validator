#!/usr/bin/env python3

import json
import os
from datetime import datetime
from botocore.exceptions import ClientError
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger, get_md5
from bento.common.s3 import S3Bucket
from common.constants import ERRORS, WARNINGS, DB, FILE_STATUS, STATUS_NEW, S3_FILE_INFO, ID, SIZE, MD5, UPDATED_AT, \
    FILE_NAME, S3_DOWNLOAD_DIR, SQS_NAME, FILE_ID, STATUS_ERROR, STATUS_WARNING, STATUS_PASSED, SUBMISSION_ID, S3_BUCKET_DIR
from common.utils import cleanup_s3_download_dir, get_exception_msg

VISIBILITY_TIMEOUT = 30
"""
Interface for validate files via SQS
"""
def fileValidate(configs, job_queue, mongo_dao):
    file_processed = 0
    log = get_logger('File Validation Service')
    validator = FileValidator(configs, mongo_dao)

    #step 3: run validator as a service
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {configs[SQS_NAME]}, '
                            f'{file_processed} file(s) have been processed so far')
            
            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                extender = None
                data = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    # Make sure job is in correct format
                    if data.get(FILE_ID) or data.get(SUBMISSION_ID):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        if data[FILE_ID] :
                            #1 call mongo_dao to get batch by batch_id
                            fileRecord = mongo_dao.get_file(data[FILE_ID], configs[DB])
                            #2. validate batch and files.
                            status = validator.validate(fileRecord)
                            if status == STATUS_ERROR:
                                log.error(f'The file record is invalid, {data[FILE_ID]}!')
                            elif status == STATUS_WARNING:
                                log.error(f'The file record is valid but with warning, {data[FILE_ID]}!')
                            else:
                                log.info(f'The file record passed validation, {data[FILE_ID]}.')
                            #4. update batch
                            if not mongo_dao.update_file( fileRecord, configs[DB]):
                                log.error(f'Failed to update file record, {data[FILE_ID]}!')
                            else:
                                log.info(f'The file record is updated,{data[FILE_ID]}.')
                        else:
                            status = validator.validate_all_files(data[SUBMISSION_ID])
                            if status and status == STATUS_ERROR:
                                # todo update submission
                                a = "todo"
                            else:
                                if len(validator.invalid_file_list) > 0:
                                    # todo update files.
                                    b = "todo"

                    else:
                        log.error(f'Invalid message: {data}!')

                    file_processed +=1
                    
                except Exception as e:
                    log.debug(e)
                    log.critical(
                        f'Something wrong happened while processing file! Check debug log for details.')
                finally:
                    msg.delete()
                    if extender:
                        extender.stop()
                        extender = None
                    #cleanup contents in the s3 download dir
                    cleanup_s3_download_dir(S3_DOWNLOAD_DIR)
        except KeyboardInterrupt:
            log.info('Good bye!')
            return


"""
 Requirement for the ticket crdcdh-539
1. Missing File, validate if a file specified in a manifest exist in files folder of the submission (error)
2. File integrity check, file size and md5 will be validated based on the information in the manifest(s) (error)
3. Extra files, validate if there are files in files folder of the submission that are not specified in any manifests 
    of the submission. This may happen if submitter uploaded files (via CLI) but forgot to upload the manifest. (error) included in total count.
    * this requirement needs to validate all files in a submission, it is implemented in check_duplicates_in_submission(self, submissionId)
4. Duplication (Warning): 
    4-1. Same MD5 checksum and same filename 
    4-2. Same MD5 checksum but different filenames
    4-3. Same filename  but different MD5 checksum
    4-4. If the old file was in an earlier batch or submission, and If the submitter indicates this file is NEW, this should trigger an Error.  If the submitter has indicated this is a replacement, there's no error or warning. If this is part of the same batch, then the new file just overwrites the old file and is flagged as NEW.
"""
class FileValidator:
    
    def __init__(self, configs, mongo_dao):
        self.configs = configs
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('File Validator')
        self.mongo_dao = mongo_dao
        self.fileRecord = None
        self.bucketDir= configs[S3_BUCKET_DIR]
        self.rootPath = None
        self.file_errors = {}
        self.file_warnings = {}
        self.invalid_file_list = []

    def validate(self, fileRecord):

        fileRecord[ERRORS] =  fileRecord[ERRORS] if fileRecord.get(ERRORS) else []
        fileRecord[WARNINGS] =  fileRecord[WARNINGS] if fileRecord.get(WARNINGS) else []

        #check if the file record is valid
        if not self.validate_fileRecord(fileRecord):
            return False
        
       
        # validate individual file
        status, errors = self.validate_file(fileRecord)
        fileRecord[UPDATED_AT] = fileRecord[S3_FILE_INFO][UPDATED_AT] = datetime.now()
        if status == STATUS_ERROR:
            fileRecord[ERRORS].append(errors)
            fileRecord[FILE_STATUS] = STATUS_ERROR
            fileRecord[S3_FILE_INFO][ERRORS].append(errors)
            fileRecord[S3_FILE_INFO][FILE_STATUS] = STATUS_ERROR
        elif status == STATUS_WARNING:
            fileRecord[WARNINGS].append(errors)
            fileRecord[FILE_STATUS] = STATUS_WARNING
            fileRecord[S3_FILE_INFO][WARNINGS].append(errors)
            fileRecord[S3_FILE_INFO][FILE_STATUS] = STATUS_WARNING
        else:
            fileRecord[FILE_STATUS] = STATUS_PASSED
            fileRecord[S3_FILE_INFO][FILE_STATUS] = STATUS_PASSED
        return status
    
    def validate_fileRecord(self, fileRecord):
        msg = None
        #This service only processes metadata batches, if a file batch is passed, it should be ignored (output an error message in the log).
        if not fileRecord.get(S3_FILE_INFO):
            msg = f'Invalid file object, no s3 file info, {fileRecord[ID]}!'
            self.log.error(msg)
            fileRecord[ERRORS].append(msg)
            return False
        else:
            fileRecord[S3_FILE_INFO][ERRORS] =  fileRecord[S3_FILE_INFO][ERRORS] if fileRecord[S3_FILE_INFO].get(ERRORS) else []
            fileRecord[S3_FILE_INFO][WARNINGS] =  fileRecord[S3_FILE_INFO][WARNINGS] if fileRecord[S3_FILE_INFO].get(WARNINGS) else []
            if fileRecord[S3_FILE_INFO][FILE_STATUS] == STATUS_PASSED:
                msg = f'Invalid file object, invalid s3 file status, {fileRecord[ID]}/{fileRecord[S3_FILE_INFO][FILE_STATUS]}!'
                self.log.error(msg)
                fileRecord[ERRORS].append(msg)
                return False
            elif not fileRecord[S3_FILE_INFO][FILE_NAME] or not fileRecord[S3_FILE_INFO][SIZE] \
                    or not fileRecord[S3_FILE_INFO][MD5]:
                msg = f'Invalid file object, invalid s3 file info, {fileRecord[ID]}!'
                self.log.error(msg)
                fileRecord[S3_FILE_INFO][ERRORS].append(msg)
                fileRecord[S3_FILE_INFO][FILE_STATUS] = STATUS_ERROR
                fileRecord[ERRORS].append(msg)
                return False

        if not fileRecord.get(SUBMISSION_ID):
            msg = f'Invalid file object, no submission Id found, {fileRecord[ID]}!'
            self.log.error(msg)
            fileRecord[ERRORS].append(msg)
            return False
        
        # get submission by submissionId
        submission = self.mongo_dao.get_submission(fileRecord[SUBMISSION_ID], self.configs[DB])
        if not submission:
            msg = f'Invalid file object, no related submission object found, {fileRecord[ID]}/{fileRecord[SUBMISSION_ID]}!'
            self.log.error(msg)
            fileRecord[ERRORS].append(msg)
            return False

        if not submission.get("rootPath"):
            msg = f'Invalid submission object, no rootPath found, {fileRecord[ID]}/{fileRecord[SUBMISSION_ID]}!'
            self.log.error(msg)
            fileRecord[ERRORS].append(msg)
            return False
        
        self.rootPath= submission["rootPath"]

        self.fileRecord = fileRecord

        return True
    
    """
    This function is designed for validate individual file in s3 bucket that is mounted to /s3_bucket dir
    """
    def validate_file(self, fileRecord):
        msg = None
        file_info = fileRecord[S3_FILE_INFO]
        key = os.path.join(self.bucketDir, os.path.join(self.rootPath, f"file/{file_info[FILE_NAME]}"))
        org_size = file_info[SIZE]
        org_md5 = file_info[MD5]
        file_name = file_info[FILE_NAME]
        try:
            # 1. check if exists
            if not os.path.isfile(key):
                msg = f'The file does not exist in s3 bucket, {fileRecord[ID]}/{file_name}!'
                return STATUS_ERROR, msg
            
            # 2. check file integrity
            if org_size != os.path.getsize(key) or org_md5 != get_md5(key):
                msg = f'The file in s3 bucket does not matched with the file record, {fileRecord[ID]}/{file_name}!'
                return STATUS_ERROR, msg
            
            # check duplicates in manifest
            manifest_info_list = self.mongo_dao.get_files_by_submission(fileRecord[SUBMISSION_ID], self.configs[DB])
            if not manifest_info_list or  len(manifest_info_list) == 0:
                msg = f"No file records found for the submission, {SUBMISSION_ID}!"
                self.log.error(msg)
                return None, msg
            
            # 3. check if Same MD5 checksum and same filename 
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] == org_md5]
            if len(temp_list) > 1:
                msg = f'Duplicate files with the same name and md5 exist, {fileRecord[ID]}/{file_name}/{org_md5}!'
                return STATUS_WARNING, msg 
            
            # 4. check if Same filename but different MD5 checksum 
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] != org_md5]
            if len(temp_list) > 0:
                msg = f'Duplicate files with the same name but different md5 exist, {fileRecord[ID]}/{file_name}/{org_md5}!'
                return STATUS_WARNING, msg
            
            # 5. check if Same MD5 checksum but different filename
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] != file_name and file[S3_FILE_INFO][MD5] == org_md5]
            if len(temp_list) > 0:
                msg = f'Duplicate files with the same name but different md5 exist in s3 bucket, {fileRecord[ID]}/{file_name}/{org_md5}!'
                if fileRecord[FILE_STATUS] == STATUS_NEW:
                    return STATUS_ERROR, msg
                return STATUS_WARNING, msg 
              
            return STATUS_PASSED, None
        
        except ClientError as ce:
            self.df = None
            self.log.debug(ce)
            self.log.exception(f"Failed downloading file,{file_info.fileName} to {self.batch.bucketName}! {get_exception_msg()}.")
            msg = f'File validating failed with S3 client error! {get_exception_msg()}.'
            return STATUS_ERROR, msg
        except Exception as e:
            self.df = None
            self.log.debug(e)
            self.log.exception('Downloading file failed! Check debug log for detailed information.')
            msg = f"File validating file failed! {get_exception_msg()}."
            return STATUS_ERROR, msg
    
    """
    Validate all file in a submission:
    3. Extra files, validate if there are files in files folder of the submission that are not specified in any manifests of the submission. 
    This may happen if submitter uploaded files (via CLI) but forgot to upload the manifest. (error) included in total count.
    4. Duplication (Warning): 
        4-1. Same MD5 checksum and same filename 
        4-2. Same MD5 checksum but different filenames
        4-3. Same filename  but different MD5 checksum
        4-4. If the old file was in an earlier batch or submission, and If the submitter indicates this file is NEW, this should trigger an Error.  
    """
    def validate_all_files(self, submissionId):
        msg = None
        key = os.path.join(self.bucketDir, os.path.join(self.rootPath, f"file/"))
       
        try:
            # get manifest info for the submission
            manifest_info_list = self.mongo_dao.get_files_by_submission(submissionId)
            if not manifest_info_list or  len(manifest_info_list) == 0:
                msg = f"No file records found for the submission, {submissionId}!"
                self.log.error(msg)
                return STATUS_ERROR, msg
            
            # 1) check if Extra files, validate if there are files in files folder of the submission that are not specified 
            # in any manifests of the submission. This may happen if submitter uploaded files (via CLI) but forgot to upload 
            # the manifest. (error) included in total count.
            manifest_file_list = [{ID: manifest_info[ID], S3_FILE_INFO: manifest_info[S3_FILE_INFO]} for manifest_info in manifest_info_list]
            manifest_file_names = [manifest_info[S3_FILE_INFO][FILE_NAME] for manifest_info in manifest_info_list]

            # get file objects info in ths s3 bucket base on key
            # s3_fileList = self.bucket.bucket.objects.filter(Prefix=key)
            s3_fileList = os.listdir(key)
            for file in s3_fileList:
                file_name = os.path.basename(file)
                if file_name not in manifest_file_names:
                    msg = f"The file in s3 bucket is not specified by the manifests in the submission, {submissionId}!"
                    return STATUS_ERROR, msg
                
            # 2.  check if same MD5 checksum and same filename
            unique= set()
            f_m_duplicates = []
            name_md5_list = [{f'{dict[S3_FILE_INFO][FILE_NAME]}-{dict[S3_FILE_INFO][MD5]}': dict[ID]} for dict in manifest_file_list]
            for temp in name_md5_list:
                key = list(temp.keys())[0]
                if key not in unique:
                    unique.add[key]
                else:
                    f_m_duplicates.append(temp[key]) 
                   
            # process the same file name and md5 duplications
            if len(f_m_duplicates) > 0:
                msg1 = f"The same file name and md5 duplicates are found, {submissionId}!"
                self.log.warn(msg)
                self.process_invalid_files(self, manifest_file_list, f_m_duplicates, msg1)
                
                
            # 3. check if Same MD5 checksum but different filenames
            unique= set()
            md5_duplicates = []
            name_md5_list = [{dict[S3_FILE_INFO][MD5]: dict[ID]} for dict in manifest_file_list]
            for temp in name_md5_list:
                key = list(temp.keys())[0]
                if key not in unique:
                    unique.add[key]
                else:
                    md5_duplicates.append(temp[key]) 
                       
            if len(md5_duplicates) > len(f_m_duplicates):
                msg2 = f"The same md5 but different file name duplicates are found, {submissionId}!"
                self.log.warn(msg)
                md5_duplicates = filter(lambda i: i not in f_m_duplicates, md5_duplicates)
                self.process_invalid_files(self, manifest_file_list, md5_duplicates, msg2, True)
                    
            # 4. Same filename but different MD5 checksum
            unique= set()
            name_duplicates = []
            name_md5_list = [{dict[S3_FILE_INFO][FILE_NAME]: dict} for dict in manifest_file_list]
            for temp in name_md5_list:
                key = list(temp.keys())[0]
                if key not in unique:
                    unique.add[key]
                else:
                    name_duplicates.append(temp[key])     
            if len(name_duplicates) > len(f_m_duplicates):
                msg3 = f"The same file name but different md5 duplicates are found, {submissionId}!"
                self.log.warn(msg)
                name_duplicates = filter(lambda i: i not in f_m_duplicates, name_duplicates)
                self.process_invalid_files(self, manifest_file_list, md5_duplicates, msg3)
                
        except ClientError as ce:
            self.df = None
            self.log.debug(ce)
            self.log.exception(f"Failed get file info from bucket, {self.batch.bucketName}! {get_exception_msg()}!")
            msg = f'File validating failed with S3 client error! {get_exception_msg()}.'
            return STATUS_ERROR, msg
        except Exception as e:
            self.df = None
            self.log.debug(e)
            self.log.exception(f"Failed get file info from bucket, {self.batch.bucketName}! {get_exception_msg()}!")
            msg = f"Failed get file info from bucket, {self.batch.bucketName}! {get_exception_msg()}!"
            return STATUS_ERROR, msg
        return None, None
    
    def process_invalid_files(self, manifest_file_list, invalid_file_ids, msg, check_new = False):
        temp_list =  list(filter(lambda f: f.get(ID) in invalid_file_ids, manifest_file_list))
        for record in temp_list:
            if check_new and record[FILE_STATUS] == STATUS_NEW:
                record[FILE_STATUS] = STATUS_ERROR
                record[ERRORS] = record[ERRORS].append(msg) if record[ERRORS] and isinstance(record[ERRORS], list) else [msg]
                record[S3_FILE_INFO][FILE_STATUS] = STATUS_ERROR
                record[S3_FILE_INFO][ERRORS] = record[S3_FILE_INFO][ERRORS] .append(msg) if record[S3_FILE_INFO][ERRORS] \
                    and isinstance(record[S3_FILE_INFO][ERRORS], list) else [msg]
                self.invalid_file_list.append(record)
            else:
                record[FILE_STATUS] = STATUS_WARNING
                record[WARNINGS] = record[WARNINGS].append(msg) if record[WARNINGS] and isinstance(record[WARNINGS], list) else [msg]
                record[S3_FILE_INFO][FILE_STATUS] = STATUS_WARNING
                record[S3_FILE_INFO][WARNINGS] = record[S3_FILE_INFO][WARNINGS].append(msg) if record[S3_FILE_INFO][WARNINGS] \
                    and isinstance(record[S3_FILE_INFO][WARNINGS], list) else [msg]
                self.invalid_file_list.append(record)
        



