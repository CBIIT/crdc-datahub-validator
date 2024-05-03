#!/usr/bin/env python3

import json
import os
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import ERRORS, WARNINGS, STATUS, STATUS_NEW, S3_FILE_INFO, ID, SIZE, MD5, UPDATED_AT, \
    FILE_NAME, SQS_TYPE, SQS_NAME, FILE_ID, STATUS_ERROR, STATUS_WARNING, STATUS_PASSED, SUBMISSION_ID, \
    BATCH_BUCKET, SERVICE_TYPE_FILE, LAST_MODIFIED, CREATED_AT, TYPE, SUBMISSION_INTENTION, SUBMISSION_INTENTION_DELETE, \
    ORIN_FILE_NAME
from common.utils import get_exception_msg, current_datetime, get_s3_file_info, get_s3_file_md5, create_error, get_uuid_str
from service.ecs_agent import set_scale_in_protection

VISIBILITY_TIMEOUT = 20
"""
Interface for validate files via SQS
"""
def fileValidate(configs, job_queue, mongo_dao):
    file_processed = 0
    log = get_logger('File Validation Service')
    #run file validator as a service
    scale_in_protection_flag = False
    log.info(f'{SERVICE_TYPE_FILE} service started')
    while True:
        try:
            msgs = job_queue.receiveMsgs(VISIBILITY_TIMEOUT)
            if len(msgs) > 0:
                log.info(f'New message is coming: {configs[SQS_NAME]}, '
                         f'{file_processed} file(s) have been processed so far')
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
                        if not mongo_dao.update_file_info(fileRecord):
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
                        mongo_dao.set_submission_validation_status(validator.submission, status, None, None, msgs)
                    else:
                        log.error(f'Invalid message: {data}!')
                    
                    log.info(f'Processed {SERVICE_TYPE_FILE} validation for the {"file, "+ data.get(FILE_ID) if data.get(FILE_ID) else "submission, " + data.get(SUBMISSION_ID)}!')
                    file_processed += 1
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
        try: 
            #check if the file record is valid
            if not self.validate_fileRecord(fileRecord):
                return STATUS_ERROR
            #escape file validation if submission intention is Delete
            if self.submission.get(SUBMISSION_INTENTION) == SUBMISSION_INTENTION_DELETE:
                return STATUS_PASSED
            # validate individual file
            status, error = self.validate_file(fileRecord)
            self.set_status(fileRecord, status, error)
            return status
        except Exception as e: #catch all unhandled exception
            self.log.debug(e)
            msg = f"{fileRecord.get(SUBMISSION_ID)}: Failed to validate file, {fileRecord.get(ID)}! {get_exception_msg()}!"
            self.log.exception(msg)
            error = create_error("Internal error", "File validation failed due to internal errors.  Please try again and contact the helpdesk if this error persists.")
            self.set_status(fileRecord, STATUS_ERROR, error)
            return STATUS_ERROR
        finally:
            if self.bucket:
                del self.bucket

    
    def validate_fileRecord(self, fileRecord):
        #This service only processes metadata batches, if a file batch is passed, it should be ignored (output an error message in the log).
        if not fileRecord.get(S3_FILE_INFO):
            msg = f'Invalid file object, no s3 file info, {fileRecord[ID]}!'
            self.log.error(msg)
            error = create_error("Invalid dataRecord", msg)
            self.set_status(fileRecord, STATUS_ERROR, error)
            return False
        else:
            if not fileRecord[S3_FILE_INFO].get(FILE_NAME) or not fileRecord[S3_FILE_INFO].get(SIZE) \
                    or not fileRecord[S3_FILE_INFO].get(MD5):
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

        # 1. check if exists
        if not self.bucket.file_exists_on_s3(key):
            msg = f'File “{file_name}” not found.'
            self.log.error(msg)
            error = create_error("Data File not found", msg)
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

        if int(org_size) != int(size) or org_md5 != md5:
            msg = f'File “{file_name}”: file integrity check failed.'
            self.log.error(msg)
            error = create_error("Data file Integrity check failed", msg)
            return STATUS_ERROR, error
        
        # check duplicates in manifest
        manifest_info_list = self.mongo_dao.get_files_by_submission(fileRecord[SUBMISSION_ID])
        if not manifest_info_list or  len(manifest_info_list) == 0:
            msg = f"No file records found for the submission."
            self.log.error(msg)
            error = create_error("File records not found", msg)
            return STATUS_ERROR, error
        
        # 3. check if Same MD5 checksum and same filename 
        temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] == org_md5]
        if len(temp_list) > 1:
            msg = f'File “{file_name}”: already exists with the same name and md5 value.'
            self.log.warning(msg)
            error = create_error("Duplicated file records detected", msg)
            return STATUS_WARNING, error 
        
        # 4. check if Same filename but different MD5 checksum 
        temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] != org_md5]
        if len(temp_list) > 0:
            msg = f'File “{file_name}”: A file with the same name but different md5 value was found.'
            self.log.warning(msg)
            error = create_error("Conflict file records detected", msg)
            return STATUS_WARNING, error
        
        # 5. check if Same MD5 checksum but different filename
        temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] != file_name and file[S3_FILE_INFO][MD5] == org_md5]
        if len(temp_list) > 0:
            msg = f'File “{file_name}”: another file with the same MD5 found.'
            error = create_error("Duplicated file content detected", msg)
            if fileRecord[STATUS] == STATUS_NEW:
                self.log.error(msg)
                return STATUS_ERROR, error
            self.log.warning(msg)
            return STATUS_WARNING, error 
            
        return STATUS_PASSED, None
    
    """
    Validate all file in a submission:
    1. Extra files, validate if there are files in files folder of the submission that are not specified in any manifests of the submission. 
    This may happen if submitter uploaded files (via CLI) but forgot to upload the manifest. (error) included in total count.
    """
    def validate_all_files(self, submission_id):
        errors = []
        missing_count = 0
        if not self.get_root_path(submission_id):
            msg = f'Invalid submission object, no rootPath found, {submission_id}!'
            self.log.error(msg)
            error = create_error("Invalid submission", msg)
            return STATUS_ERROR, [error]
        key = os.path.join(os.path.join(self.rootPath, f"file/"))

        try:
            if not self.submission:
                 self.submission = self.mongo_dao.get_submission(submission_id)
            if not self.submission:
                msg = f'Invalid submission object, no related submission object found, {submission_id}!'
                self.log.error(msg)
                return False
            
            submission_intention = self.submission.get(SUBMISSION_INTENTION)
            # get manifest info for the submission
            manifest_info_list = self.mongo_dao.get_files_by_submission(submission_id) if submission_intention != INTENTION_DELETE else []
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
                    file_batch = self.mongo_dao.find_batch_by_file_name(submission_id, "data file", file_name)
                    batchID = file_batch[ID] if file_batch else "-"
                    displayID = file_batch["displayID"] if file_batch else None
                    msg = f'File “{file_name}”: no record found.'
                    self.log.error(msg)
                    error = {
                        TYPE: "Data File",
                        "validationType": "data file",
                        "submittedID": file_name,
                        "batchID": batchID,
                        "displayID": displayID,
                        "severity": "Error",
                        "uploadedDate": file.last_modified,
                        "validatedDate": current_datetime(),
                        "errors": [create_error("Extra file found", msg)]
                    }
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
            msg = f"{submission_id}: Failed to validate files! {get_exception_msg()}!"
            self.log.exception(msg)
            error = create_error("Internal error", "File validation failed due to internal errors.  Please try again and contact the helpdesk if this error persists.")
            return None, [error]

    def set_status(self, record, status, error):
        record[S3_FILE_INFO][UPDATED_AT] = current_datetime()
        if status == STATUS_ERROR:
            record[S3_FILE_INFO][STATUS] = STATUS_ERROR
            record[S3_FILE_INFO][ERRORS] = [error]
            
        elif status == STATUS_WARNING: 
            record[S3_FILE_INFO][STATUS] = STATUS_WARNING
            record[S3_FILE_INFO][WARNINGS] = [error]
            
        else:
            record[S3_FILE_INFO][STATUS] = STATUS_PASSED