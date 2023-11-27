#!/usr/bin/env python3

import json
import os
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger, get_md5
from common.constants import ERRORS, WARNINGS, DB, FILE_STATUS, STATUS_NEW, S3_FILE_INFO, ID, SIZE, MD5, UPDATED_AT, \
    FILE_NAME, S3_DOWNLOAD_DIR, SQS_NAME, FILE_ID, STATUS_ERROR, STATUS_WARNING, STATUS_PASSED, SUBMISSION_ID, S3_BUCKET_DIR
from common.utils import cleanup_s3_download_dir, get_exception_msg, current_datetime_str

VISIBILITY_TIMEOUT = 20
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
                    if data.get(FILE_ID):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        #1 call mongo_dao to get batch by batch_id
                        fileRecord = mongo_dao.get_file(data[FILE_ID], configs[DB])
                        #2. validate file.
                        status = validator.validate(fileRecord)
                        if status == STATUS_ERROR:
                            log.error(f'The file record is invalid, {data[FILE_ID]}!')
                        elif status == STATUS_WARNING:
                            log.error(f'The file record is valid but with warning, {data[FILE_ID]}!')
                        else:
                            log.info(f'The file record passed validation, {data[FILE_ID]}.')
                        #4. update dataRecords
                        if not mongo_dao.update_file( fileRecord, configs[DB]):
                            log.error(f'Failed to update file record, {data[FILE_ID]}!')
                        else:
                            log.info(f'The file record is updated,{data[FILE_ID]}.')

                    elif data.get(SUBMISSION_ID):
                        submissionID = data[SUBMISSION_ID]
                        if not validator.get_root_path(submissionID):
                            log.error(f'Invalid submission, {submissionID}!')
                        else:
                            status, msgs = validator.validate_all_files(data[SUBMISSION_ID])
                            #update submission
                            if status and status == STATUS_ERROR:
                                mongo_dao.set_submission_error(validator.submission, msgs, configs[DB])
                            # update files
                            if len(validator.update_file_list) > 0:
                                mongo_dao.update_files(validator.update_file_list, configs[DB])

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
        self.update_file_list = []
        self.submission = None

    def validate(self, fileRecord):

        fileRecord[ERRORS] =  fileRecord[ERRORS] if fileRecord.get(ERRORS) else []
        fileRecord[WARNINGS] =  fileRecord[WARNINGS] if fileRecord.get(WARNINGS) else []

        #check if the file record is valid
        if not self.validate_fileRecord(fileRecord):
            return False
        
        # validate individual file
        status, error = self.validate_file(fileRecord)
        self.set_status(fileRecord, status, error)
        return status
    
    def validate_fileRecord(self, fileRecord):
        #This service only processes metadata batches, if a file batch is passed, it should be ignored (output an error message in the log).
        if not fileRecord.get(S3_FILE_INFO):
            msg = f'Invalid file object, no s3 file info, {fileRecord[ID]}!'
            self.log.error(msg)
            error = {"title": "Invalid dataRecord", "description": msg}
            fileRecord[ERRORS].append({error})
            fileRecord[FILE_STATUS] = STATUS_ERROR
            return False
        else:
            if not fileRecord[S3_FILE_INFO][FILE_NAME] or not fileRecord[S3_FILE_INFO][SIZE] \
                    or not fileRecord[S3_FILE_INFO][MD5]:
                msg = f'Invalid file object, invalid s3 file info, {fileRecord[ID]}!'
                self.log.error(msg)
                error = {"title": "Invalid file info", "description": msg}
                self.set_status(fileRecord, STATUS_ERROR, error)
                return False

        if not fileRecord.get(SUBMISSION_ID):
            msg = f'Invalid file object, no submission Id found, {fileRecord[ID]}!'
            self.log.error(msg)
            error = {"title": "Invalid submission Id", "description": msg}
            self.set_status(fileRecord, STATUS_ERROR, error)
            return False
        
        if not self.get_root_path(fileRecord[SUBMISSION_ID]):
            msg = f'Invalid submission object, no rootPath found, {fileRecord[ID]}/{fileRecord[SUBMISSION_ID]}!'
            self.log.error(msg)
            error = {"title": "Invalid submission", "description": msg}
            self.set_status(fileRecord, STATUS_ERROR, error)
            return False

        self.fileRecord = fileRecord

        return True
    
    def get_root_path(self, submissionID):
        submission = self.mongo_dao.get_submission(submissionID, self.configs[DB])
        if not submission:
            msg = f'Invalid submission object, no related submission object found, {submissionID}!'
            self.log.error(msg)
            return False
        self.submission = submission
        if not submission.get("rootPath"):
            msg = f'Invalid submission object, no rootPath found, {submissionID}!'
            self.log.error(msg)
            return False
        
        self.rootPath= submission["rootPath"]
        return True
    
    """
    This function is designed for validate individual file in s3 bucket that is mounted to /s3_bucket dir
    """
    def validate_file(self, fileRecord):

        file_info = fileRecord[S3_FILE_INFO]
        key = os.path.join(self.bucketDir, os.path.join(self.rootPath, f"file/{file_info[FILE_NAME]}"))
        org_size = file_info[SIZE]
        org_md5 = file_info[MD5]
        file_name = file_info[FILE_NAME]

        try:
            # 1. check if exists
            if not os.path.isfile(key):
                msg = f'The file does not exist in s3 bucket, {fileRecord[ID]}/{file_name}!'
                self.log.error(msg)
                error = {"title": "The file does not exist in s3 bucket", "description": msg}
                return STATUS_ERROR, error
            
            # 2. check file integrity
            if org_size != os.path.getsize(key) or org_md5 != get_md5(key):
                msg = f'The file in s3 bucket does not matched with the file record, {fileRecord[ID]}/{file_name}!'
                self.log.error(msg)
                error = {"title": "File is not integrity", "description": msg}
                return STATUS_ERROR, error
            
            # check duplicates in manifest
            manifest_info_list = self.mongo_dao.get_files_by_submission(fileRecord[SUBMISSION_ID], self.configs[DB])
            if not manifest_info_list or  len(manifest_info_list) == 0:
                msg = f"No file records found for the submission, {SUBMISSION_ID}!"
                self.log.error(msg)
                error = {"title": "No file records found", "description": msg}
                return STATUS_ERROR, error
            
            # 3. check if Same MD5 checksum and same filename 
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] == org_md5]
            if len(temp_list) > 1:
                msg = f'Duplicate files with the same name and md5 exist, {fileRecord[ID]}/{file_name}/{org_md5}!'
                self.log.warning(msg)
                error = {"title": "Duplicate files with the same name and but different md5", "description": msg}
                return STATUS_WARNING, error 
            
            # 4. check if Same filename but different MD5 checksum 
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] != org_md5]
            if len(temp_list) > 0:
                msg = f'Duplicate files with the same name but different md5 exist, {fileRecord[ID]}/{file_name}/{org_md5}!'
                self.log.warning(msg)
                error = {"title": "Duplicate files with the same name and but different md5", "description": msg}
                return STATUS_WARNING, error
            
            # 5. check if Same MD5 checksum but different filename
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] != file_name and file[S3_FILE_INFO][MD5] == org_md5]
            if len(temp_list) > 0:
                msg = f'Duplicate files with the same md5 but different name exist in s3 bucket, {fileRecord[ID]}/{file_name}/{org_md5}!'
                error = {"title": "Duplicate files with the same md5 but different name", "description": msg}
                if fileRecord[FILE_STATUS] == STATUS_NEW:
                    self.log.error(msg)
                    return STATUS_ERROR, error
                self.log.warning(msg)
                return STATUS_WARNING, error 
              
            return STATUS_PASSED, None
        
        except Exception as e:
            self.df = None
            self.log.debug(e)
            self.log.exception('Downloading file failed! Check debug log for detailed information.')
            msg = f"File validating file failed! {get_exception_msg()}."
            error = {"title": "Exception", "description": msg}
            return STATUS_ERROR, error
    
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
        errors = []
        missing_count = 0
        key = os.path.join(self.bucketDir, os.path.join(self.rootPath, f"file/"))
        invalid_ids = []
        try:
            # get manifest info for the submission
            manifest_info_list = self.mongo_dao.get_files_by_submission(submissionId, self.configs[DB])
            if not manifest_info_list or  len(manifest_info_list) == 0:
                msg = f"No file records found for the submission, {submissionId}!"
                self.log.error(msg)
                error = {"title": "No file records found for the submission", "description": msg}
                return STATUS_ERROR, [error]
            
            # 1: check if Extra files, validate if there are files in files folder of the submission that are not specified 
            # in any manifests of the submission. This may happen if submitter uploaded files (via CLI) but forgot to upload 
            # the manifest. (error) included in total count.
            manifest_file_list = [{ID: manifest_info[ID], S3_FILE_INFO: manifest_info[S3_FILE_INFO]} for manifest_info in manifest_info_list]
            manifest_file_names = [manifest_info[S3_FILE_INFO][FILE_NAME] for manifest_info in manifest_info_list]

            # get file objects info in mounted s3 bucket base on key
            root, dirs, files = next(os.walk(key))
            for file_name in files:
                if file_name.startswith('.'):
                    continue
                file = os.path.join(key, file_name)
                if os.path.isfile(file) and file_name not in manifest_file_names:
                    msg = f"File, {file_name}, in s3 bucket is not specified by the manifests in the submission, {submissionId}!"
                    self.log.error(msg)
                    error = {"title": "No file records found for the submission", "description": msg}
                    errors.append(error)
                    missing_count += 1
                else:
                    #2 check integrity of a file
                    record = next(filter(lambda i: i[S3_FILE_INFO][FILE_NAME] == file_name, manifest_info_list))
                    if record[S3_FILE_INFO][SIZE] != os.path.getsize(file) or record[S3_FILE_INFO][MD5] != get_md5(file):
                        msg = f"The file failed integrity checking, {record[ID]}"
                        self.log.error(msg)
                        error = {"title": "Not integrity", "description": msg}
                        self.set_status(record, STATUS_ERROR, error)
                        invalid_ids.append(record[ID])
                        self.update_file_list.append(record)

              
            # 3.  check if same MD5 checksum and same filename
            unique= set()
            f_m_duplicates = []
            name_md5_list = [{f'{dict[S3_FILE_INFO][FILE_NAME]}-{dict[S3_FILE_INFO][MD5]}': dict[ID]} for dict in manifest_file_list]
            for temp in name_md5_list:
                key = list(temp.keys())[0]
                if key not in unique:
                    unique.add(key)
                else:
                    f_m_duplicates.append(temp[key]) 
                   
            # process the same file name and md5 duplications
            if len(f_m_duplicates) > 0:
                msg = f"The same file name and md5 duplicates are found, {submissionId}!"
                self.log.warn(msg)
                invalid_ids += f_m_duplicates
                self.process_invalid_files(manifest_info_list, f_m_duplicates, "The same file name and md5 duplication", msg)
                
                
            # 4. check if Same MD5 checksum but different filenames
            unique = set()
            md5_duplicates = []
            name_md5_list = [{dict[S3_FILE_INFO][MD5]: dict[ID]} for dict in manifest_file_list]
            for temp in name_md5_list:
                key = list(temp.keys())[0]
                if key not in unique:
                    unique.add(key)
                else:
                    md5_duplicates.append(temp[key]) 
                       
            if len(md5_duplicates) > len(f_m_duplicates):
                msg = f"The same md5 but different file name duplicates are found, {submissionId}!"
                self.log.warn(msg)
                md5_duplicates = list(filter(lambda i: i not in f_m_duplicates, md5_duplicates))
                self.process_invalid_files(manifest_info_list, md5_duplicates, "The same md5 but different file name duplication", msg, True)
                invalid_ids +=  md5_duplicates   

            # 5. Same filename but different MD5 checksum
            unique= set()
            name_duplicates = []
            name_md5_list = [{dict[S3_FILE_INFO][FILE_NAME]: dict[ID]} for dict in manifest_file_list]
            for temp in name_md5_list:
                key = list(temp.keys())[0]
                if key not in unique:
                    unique.add(key)
                else:
                    name_duplicates.append(temp[key])     
            if len(name_duplicates) > len(f_m_duplicates):
                msg = f"The same file name but different md5 duplicates are found, {submissionId}!"
                self.log.warn(msg)
                name_duplicates = list(filter(lambda i: i not in f_m_duplicates, name_duplicates))
                self.process_invalid_files(manifest_info_list, name_duplicates, "The same file name but different md5 duplication", msg)
                invalid_ids += name_duplicates

            # process passed files
            self.process_passed_files(manifest_info_list, invalid_ids)

        except Exception as e:
            self.df = None
            self.log.debug(e)
            self.log.exception(f"Failed get file info from bucket, {self.batch.bucketName}! {get_exception_msg()}!")
            msg = f"Failed get file info from bucket, {self.batch.bucketName}! {get_exception_msg()}!"
            error = {"title": "Exception", "description": msg}
            return STATUS_ERROR, error
        
        if missing_count > 0 and len(errors) > 0:
            return STATUS_ERROR, errors
        return None, None
    
    def process_passed_files(self, manifest_info_list, invalid_file_ids):
        temp_list =  list(filter(lambda f: f.get(ID) not in invalid_file_ids, manifest_info_list))
        for record in temp_list:
            self.set_status(record, STATUS_PASSED, None)
            self.update_file_list.append(record)
    
    def process_invalid_files(self, manifest_info_list, invalid_file_ids, title, msg, check_new = False):
        temp_list =  list(filter(lambda f: f.get(ID) in invalid_file_ids, manifest_info_list))
        error = {"title": title, "description": msg}
        for record in temp_list:
            if check_new and record[FILE_STATUS] == STATUS_NEW:
                self.set_status(record, STATUS_ERROR, error)
            else:
                self.set_status(record, STATUS_WARNING, error)
                
            self.update_file_list.append(record)

    def set_status(self, record, status, error):
        record[UPDATED_AT] = record[S3_FILE_INFO][UPDATED_AT] = current_datetime_str()
        if status == STATUS_ERROR:
            record[FILE_STATUS] = STATUS_ERROR
            record[ERRORS] = record[ERRORS].append(error) if record[ERRORS] and isinstance(record[ERRORS], list) else [error]
            record[S3_FILE_INFO][FILE_STATUS] = STATUS_ERROR
            record[S3_FILE_INFO][ERRORS] = record[S3_FILE_INFO][ERRORS].append(error) if record[S3_FILE_INFO][ERRORS] \
                and isinstance(record[S3_FILE_INFO][ERRORS], list) else [error]
            
        elif status == STATUS_WARNING: 
            record[FILE_STATUS] = STATUS_WARNING
            record[WARNINGS] = record[WARNINGS].append(error) if record[WARNINGS] and isinstance(record[WARNINGS], list) else [error]
            record[S3_FILE_INFO][FILE_STATUS] = STATUS_WARNING
            record[S3_FILE_INFO][WARNINGS] = record[S3_FILE_INFO][WARNINGS].append(error) if record[S3_FILE_INFO][WARNINGS] \
                and isinstance(record[S3_FILE_INFO][WARNINGS], list) else [error]
            
        else:
            record[FILE_STATUS] = STATUS_PASSED
            record[S3_FILE_INFO][FILE_STATUS] = STATUS_PASSED



