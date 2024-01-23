#!/usr/bin/env python3

import json
import os
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import ERRORS, WARNINGS, STATUS, STATUS_NEW, S3_FILE_INFO, ID, SIZE, MD5, UPDATED_AT, \
    FILE_NAME, SQS_TYPE, SQS_NAME, FILE_ID, STATUS_ERROR, STATUS_WARNING, STATUS_PASSED, SUBMISSION_ID, \
    BATCH_BUCKET, LAST_MODIFIED, CREATED_AT
from common.utils import get_exception_msg, current_datetime, get_s3_file_info, get_s3_file_md5, create_error, get_uuid_str
from service.ecs_agent import set_scale_in_protection

VISIBILITY_TIMEOUT = 20
"""
Interface for validate files via SQS
"""
def fileValidate(configs, job_queue, mongo_dao):
    file_processed = 0
    log = get_logger('File Validation Service')
    validator = None
    # activate container protection
    set_scale_in_protection(True)
    #run file validator as a service
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {configs[SQS_NAME]}, '
                            f'{file_processed} file(s) have been processed so far')
            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                set_scale_in_protection(True)
                extender = None
                data = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    # Make sure job is in correct format
                    if data.get(SQS_TYPE) == "Validate File" and data.get(FILE_ID):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        #1 call mongo_dao to get batch by batch_id
                        fileRecord = mongo_dao.get_file(data[FILE_ID])
                        #2. validate file.
                        validator = FileValidator(mongo_dao)
                        status = validator.validate(fileRecord)
                        if status == STATUS_ERROR:
                            log.error(f'The file record is invalid, {data[FILE_ID]}!')
                        elif status == STATUS_WARNING:
                            log.error(f'The file record is valid but with warning, {data[FILE_ID]}!')
                        else:
                            log.info(f'The file record passed validation, {data[FILE_ID]}.')
                        #4. update dataRecords
                        if not mongo_dao.update_file(fileRecord):
                            log.error(f'Failed to update file record, {data[FILE_ID]}!')
                        else:
                            log.info(f'The file record is updated,{data[FILE_ID]}.')

                    elif data.get(SQS_TYPE) == "Validate Submission Files" and data.get(SUBMISSION_ID):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        submission_id = data[SUBMISSION_ID]
                        validator = FileValidator(mongo_dao)
                        status = None
                        msgs = []
                        if not validator.get_root_path(submission_id):
                            log.error(f'Invalid submission, {submission_id}!')
                            status = STATUS_ERROR
                        else:
                            status, msgs = validator.validate_all_files(data[SUBMISSION_ID])
                            #update submission
                        mongo_dao.set_submission_validation_status(validator.submission, status, None, msgs)
                    else:
                        log.error(f'Invalid message: {data}!')
                    file_processed += 1
                    set_scale_in_protection(False)
                    msg.delete()
                except Exception as e:
                    log.debug(e)
                    log.critical(
                        f'Something wrong happened while processing file! Check debug log for details.')
                finally:
                    if extender:
                        extender.stop()
                        extender = None
                    validator = None
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
    
    def __init__(self, mongo_dao):
        self.log = get_logger('File Validator')
        self.mongo_dao = mongo_dao
        self.bucket_name = None
        self.bucket = None
        self.rootPath = None
        self.update_file_list = None
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
            error = create_error("Invalid dataRecord", msg)
            fileRecord[ERRORS].append({error})
            fileRecord[STATUS] = STATUS_ERROR
            return False
        else:
            if not fileRecord[S3_FILE_INFO][FILE_NAME] or not fileRecord[S3_FILE_INFO][SIZE] \
                    or not fileRecord[S3_FILE_INFO][MD5]:
                msg = f'Invalid file object, invalid s3 file info, {fileRecord[ID]}!'
                self.log.error(msg)
                error = create_error("Invalid file info", msg)
                self.set_status(fileRecord, STATUS_ERROR, error)
                return False

        if not fileRecord.get(SUBMISSION_ID):
            msg = f'Invalid file object, no submission Id found, {fileRecord[ID]}!'
            self.log.error(msg)
            error = create_error("Invalid submission Id", msg)
            self.set_status(fileRecord, STATUS_ERROR, error)
            return False
        
        if not self.get_root_path(fileRecord[SUBMISSION_ID]):
            msg = f'Invalid submission object, no rootPath found, {fileRecord[ID]}/{fileRecord[SUBMISSION_ID]}!'
            self.log.error(msg)
            error = create_error("Invalid submission", msg)
            self.set_status(fileRecord, STATUS_ERROR, error)
            return False

        return True
    
    def get_root_path(self, submissionID):
        submission = self.mongo_dao.get_submission(submissionID)
        if not submission:
            msg = f'Invalid submission object, no related submission object found, {submissionID}!'
            self.log.error(msg)
            return False
        self.submission = submission
        if not submission.get("rootPath"):
            msg = f'Invalid submission object, no rootPath found, {submissionID}!'
            self.log.error(msg)
            return False
        
        if not submission.get(BATCH_BUCKET):
            msg = f'Invalid submission object, no bucket found, {submissionID}!'
            self.log.error(msg)
            return False
        
        self.rootPath= submission["rootPath"]
        self.bucket_name = submission[BATCH_BUCKET]
        self.bucket = S3Bucket(self.bucket_name)
        return True
    
    """
    This function is designed for validate individual file in s3 bucket that is mounted to /s3_bucket dir
    """
    def validate_file(self, fileRecord):

        file_info = fileRecord[S3_FILE_INFO]
        key = os.path.join(os.path.join(self.rootPath, f"file/{file_info[FILE_NAME]}"))
        org_size = file_info[SIZE]
        org_md5 = file_info[MD5]
        file_name = file_info[FILE_NAME]

        try:
            # 1. check if exists
            if not self.bucket.file_exists_on_s3(key):
                msg = f'The file does not exist in s3 bucket, {fileRecord[ID]}/{file_name}!'
                self.log.error(msg)
                error = create_error("The file does not exist in s3 bucket", msg)
                return STATUS_ERROR, error
            
            # 2. check file integrity
            size, last_updated = get_s3_file_info(self.bucket_name, key)
            #check cached md5
            cached_md5 = self.mongo_dao.get_file_md5(self.submission[ID], file_name)
            md5 = None
            if cached_md5 and last_updated.replace(tzinfo=None) <= cached_md5.get(LAST_MODIFIED).replace(tzinfo=None):
                md5 = cached_md5.get(MD5)
            else:
                md5 = get_s3_file_md5(self.bucket_name, key)
                current_date_time = current_datetime()
                md5_info = {
                    ID: get_uuid_str() if not cached_md5 else cached_md5[ID],
                    SUBMISSION_ID : self.submission[ID],
                    FILE_NAME: file_name,
                    MD5: md5,
                    LAST_MODIFIED: last_updated,
                    CREATED_AT: current_date_time if not cached_md5 else cached_md5[CREATED_AT],
                    UPDATED_AT: current_date_time
                }
                self.mongo_dao.save_file_md5(md5_info)

            if org_size != size or org_md5 != md5:
                msg = f'The file in s3 bucket does not matched with the file record, {fileRecord[ID]}/{file_name}!'
                self.log.error(msg)
                error = create_error("File is not integrity", msg)
                return STATUS_ERROR, error
            
            # check duplicates in manifest
            manifest_info_list = self.mongo_dao.get_files_by_submission(fileRecord[SUBMISSION_ID])
            if not manifest_info_list or  len(manifest_info_list) == 0:
                msg = f"No file records found for the submission, {SUBMISSION_ID}!"
                self.log.error(msg)
                error = create_error("No file records found", msg)
                return STATUS_ERROR, error
            
            # 3. check if Same MD5 checksum and same filename 
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] == org_md5]
            if len(temp_list) > 1:
                msg = f'Duplicate files with the same name and md5 exist, {fileRecord[ID]}/{file_name}/{org_md5}!'
                self.log.warning(msg)
                error = create_error("Duplicate files with the same name and md5", msg)
                return STATUS_WARNING, error 
            
            # 4. check if Same filename but different MD5 checksum 
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] != org_md5]
            if len(temp_list) > 0:
                msg = f'Duplicate files with the same name but different md5 exist, {fileRecord[ID]}/{file_name}/{org_md5}!'
                self.log.warning(msg)
                error = create_error("Duplicate files with the same name and but different md5", msg)
                return STATUS_WARNING, error
            
            # 5. check if Same MD5 checksum but different filename
            temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] != file_name and file[S3_FILE_INFO][MD5] == org_md5]
            if len(temp_list) > 0:
                msg = f'Duplicate files with the same md5 but different name exist in s3 bucket, {fileRecord[ID]}/{file_name}/{org_md5}!'
                error = create_error("Duplicate files with the same md5 but different name", msg)
                if fileRecord[STATUS] == STATUS_NEW:
                    self.log.error(msg)
                    return STATUS_ERROR, error
                self.log.warning(msg)
                return STATUS_WARNING, error 
              
            return STATUS_PASSED, None
        
        except Exception as e:
            self.log.debug(e)
            self.log.exception('Downloading file failed! Check debug log for detailed information.')
            msg = f"File validating file failed! {get_exception_msg()}."
            error = create_error("Exception", msg)
            return STATUS_ERROR, error
    
    """
    Validate all file in a submission:
    1. Extra files, validate if there are files in files folder of the submission that are not specified in any manifests of the submission. 
    This may happen if submitter uploaded files (via CLI) but forgot to upload the manifest. (error) included in total count.
    """
    def validate_all_files(self, submissionId):
        errors = []
        missing_count = 0
        if not self.get_root_path(submissionId):
            msg = f'Invalid submission object, no rootPath found, {submissionId}!'
            self.log.error(msg)
            error = create_error("Invalid submission", msg)
            return STATUS_ERROR, [error]
        key = os.path.join(os.path.join(self.rootPath, f"file/"))

        try:
            # get manifest info for the submission
            manifest_info_list = self.mongo_dao.get_files_by_submission(submissionId)
            if not manifest_info_list or  len(manifest_info_list) == 0:
                msg = f"No file records found for the submission, {submissionId}!"
                self.log.error(msg)
                error = create_error("No file records found for the submission", msg)
                return STATUS_ERROR, [error]
            
            # 1: check if Extra files, validate if there are files in files folder of the submission that are not specified 
            # in any manifests of the submission. This may happen if submitter uploaded files (via CLI) but forgot to upload 
            # the manifest. (error) included in total count.
            manifest_file_list = [{ID: manifest_info[ID], S3_FILE_INFO: manifest_info[S3_FILE_INFO]} for manifest_info in manifest_info_list]
            manifest_file_names = [manifest_info[S3_FILE_INFO][FILE_NAME] for manifest_info in manifest_info_list]

            # get file objects info in mounted s3 bucket base on key
            # root, dirs, files = next(os.walk(key))
            # get file info in the s3 bucket file folder
            files = self.bucket.bucket.objects.filter(Prefix=key)
            for file in files:
                # don't retrieve logs
                if '/log' in file.key:
                    break
                file_name = file.key.split('/')[-1]
                
                if file_name not in manifest_file_names:
                    msg = f"File, {file_name}, in s3 bucket is not specified by the manifests in the submission, {submissionId}!"
                    self.log.error(msg)
                    error = create_error("No file records found for the submission", msg)
                    errors.append(error)
                    missing_count += 1

            if missing_count > 0 and len(errors) > 0:
                return STATUS_ERROR, errors
            else:
                records =  next((file for file in manifest_file_list if file[S3_FILE_INFO][STATUS] == STATUS_ERROR), None)
                if records: 
                    return STATUS_ERROR, None
                
                records = next((file for file in manifest_file_list if file[S3_FILE_INFO][STATUS] == STATUS_WARNING), None)
                if records: 
                    return STATUS_WARNING, None
                
                return STATUS_PASSED, None
   
        except Exception as e:
            self.log.debug(e)
            msg = f"Failed to validate files! {get_exception_msg()}!"
            self.log.exception(msg)
            error = create_error("Exception", msg)
            return None, [error]

    def set_status(self, record, status, error):
        record[S3_FILE_INFO][UPDATED_AT] = current_datetime()
        if status == STATUS_ERROR:
            record[S3_FILE_INFO][STATUS] = STATUS_ERROR
            record[S3_FILE_INFO][ERRORS] = record[S3_FILE_INFO][ERRORS] + [error] if record[S3_FILE_INFO][ERRORS] \
                and isinstance(record[S3_FILE_INFO][ERRORS], list) else [error]
            
        elif status == STATUS_WARNING: 
            record[S3_FILE_INFO][STATUS] = STATUS_WARNING
            record[S3_FILE_INFO][WARNINGS] = record[S3_FILE_INFO][WARNINGS] + [error] if record[S3_FILE_INFO][WARNINGS] \
                and isinstance(record[S3_FILE_INFO][WARNINGS], list) else [error]
            
        else:
            record[S3_FILE_INFO][STATUS] = STATUS_PASSED
