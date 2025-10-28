#!/usr/bin/env python3

import json
import os
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import ERRORS, WARNINGS, STATUS, S3_FILE_INFO, ID, SIZE, MD5, UPDATED_AT, \
    FILE_NAME, SQS_TYPE, SQS_NAME, FILE_ID, STATUS_ERROR, STATUS_WARNING, STATUS_PASSED, SUBMISSION_ID, \
    BATCH_BUCKET, SERVICE_TYPE_FILE, LAST_MODIFIED, CREATED_AT, TYPE, SUBMISSION_INTENTION, SUBMISSION_INTENTION_DELETE,\
    VALIDATION_ID, VALIDATION_ENDED, QC_RESULT_ID, VALIDATION_TYPE_FILE, QC_SEVERITY, QC_VALIDATE_DATE, FILE_VALIDATION
from common.utils import get_exception_msg, current_datetime, get_s3_file_info, get_s3_file_md5, create_error, get_uuid_str
from service.ecs_agent import set_scale_in_protection
from metadata_validator import get_qc_result

VISIBILITY_TIMEOUT = 20
"""
Interface for validate files via SQS
"""
def fileValidate(configs, job_queue, mongo_dao):
    file_processed = 0
    log = get_logger('Data file Validation Service')
    #run file validator as a service
    scale_in_protection_flag = False
    log.info(f'{SERVICE_TYPE_FILE} service started')
    while True:
        try:
            msgs = job_queue.receiveMsgs(VISIBILITY_TIMEOUT)
            if len(msgs) > 0:
                log.info(f'New message is coming: {configs[SQS_NAME]}, '
                         f'{file_processed} data file(s) have been processed so far')
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
                        if fileRecord is None: 
                            msg.delete()
                            continue
                        #2. validate file.
                        validator = FileValidator(mongo_dao)
                        status = validator.validate(fileRecord)
                        if status == STATUS_ERROR:
                            log.error(f'The data file record is invalid, {data[FILE_ID]}!')
                        elif status == STATUS_WARNING:
                            log.error(f'The data file record is valid but with warning, {data[FILE_ID]}!')
                        else:
                            log.info(f'The data file record passed validation, {data[FILE_ID]}.')
                        #4. update dataRecords
                        if not mongo_dao.update_file_info(fileRecord):
                            log.error(f'Failed to update data file record, {data[FILE_ID]}!')
                        else:
                            log.info(f'The data file record is updated,{data[FILE_ID]}.')

                    elif data.get(SQS_TYPE) == "Validate Submission Files" and data.get(SUBMISSION_ID) and data.get(VALIDATION_ID):
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

                        # update validation records
                        validation_id = data[VALIDATION_ID]
                        validation_end_at = current_datetime()
                        update_status = mongo_dao.update_validation_status(validation_id, status, validation_end_at, FILE_VALIDATION)
                        if update_status:
                            validator.submission[VALIDATION_ENDED] = validation_end_at
                        #update submission
                        mongo_dao.set_submission_validation_status(validator.submission, status if status else "None", None, None, msgs)
                    else:
                        log.error(f'Invalid message: {data}!')
                    
                    log.info(f'Processed {SERVICE_TYPE_FILE} validation for the {"data file, "+ data.get(FILE_ID) if data.get(FILE_ID) else "submission, " + data.get(SUBMISSION_ID)}!')
                    file_processed += 1
                    msg.delete()
                except Exception as e:
                    log.exception(e)
                    log.critical(
                        f'Something wrong happened while processing data file! Check debug log for details.')
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
        self.log = get_logger('Data file Validator')
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
            self.get_root_path(fileRecord[SUBMISSION_ID])
            #escape file validation if submission intention is Delete
            if self.submission.get(SUBMISSION_INTENTION) == SUBMISSION_INTENTION_DELETE:
                return STATUS_PASSED
            # validate individual file
            status, error = self.validate_file(fileRecord)
            self.save_qc_result(fileRecord, status, error)
            return status
        except Exception as e: #catch all unhandled exception
            self.log.exception(e)
            msg = f"{fileRecord.get(SUBMISSION_ID)}: Failed to validate data file, {fileRecord.get(ID)}! {get_exception_msg()}!"
            self.log.exception(msg)
            error = create_error("F011", [], "", "")
            self.save_qc_result(fileRecord, STATUS_ERROR, error)
            return STATUS_ERROR
        finally:
            if self.bucket:
                del self.bucket

    
    def validate_fileRecord(self, fileRecord):
        #This service only processes metadata batches, if a file batch is passed, it should be ignored (output an error message in the log).
        if not fileRecord.get(S3_FILE_INFO):
            msg = f'Invalid file object, no s3 file info, {fileRecord[ID]}!'
            self.log.error(msg)
            error = create_error("F009", [fileRecord[ID]], S3_FILE_INFO, "")
            self.save_qc_result(fileRecord, STATUS_ERROR, error)
            return False
        else:
            if not fileRecord[S3_FILE_INFO].get(FILE_NAME) or not fileRecord[S3_FILE_INFO].get(SIZE) \
                    or not fileRecord[S3_FILE_INFO].get(MD5):
                msg = f'Invalid data file object: invalid s3 data file info, {fileRecord[ID]}!'
                self.log.error(msg)
                error = create_error("F010", [fileRecord[ID]],  FILE_NAME, "")
                self.save_qc_result(fileRecord, STATUS_ERROR, error)
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
            msg = f'Data file “{file_name}” not found.'
            self.log.error(msg)
            error = create_error("F001", [file_name], "file", key)
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

        if int(org_size) != int(size):
            msg = f'Data file “{file_name}”: expected size: {org_size}, actual size: {size}.'
            self.log.error(msg)
            error = create_error("F003", [file_name, org_size, size], "file size", org_size)
            return STATUS_ERROR, error
        
        if org_md5 != md5:
            msg = f'Data file “{file_name}”: expected MD5: {org_md5}, actual MD5: {md5}.'
            self.log.error(msg)
            error = create_error("F004", [file_name, org_md5, md5],  "md5", org_md5)
            return STATUS_ERROR, error
        
        # check duplicates in manifest
        manifest_info_list = self.mongo_dao.get_files_by_submission(fileRecord[SUBMISSION_ID])
        if not manifest_info_list or  len(manifest_info_list) == 0:
            msg = f"No data file records found for the submission."
            self.log.error(msg)
            error = create_error("F002", [], "files", None)
            return STATUS_ERROR, error
        
        # 3. check if Same MD5 checksum and same filename 
        temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] == org_md5]
        if len(temp_list) > 1:
            msg = f'Data file “{file_name}”: already exists with the same name and md5 value.'
            self.log.warning(msg)
            error = create_error("F005", [file_name], "file name", file_name)
            return STATUS_WARNING, error 
        
        # 4. check if Same filename but different MD5 checksum 
        temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] == file_name and file[S3_FILE_INFO][MD5] != org_md5]
        if len(temp_list) > 0:
            msg = f'Data file “{file_name}”: A data file with the same name but different md5 value was found.'
            self.log.warning(msg)
            error = create_error("F006", [file_name], "file name", file_name)
            return STATUS_WARNING, error
        
        # 5. check if Same MD5 checksum but different filename
        temp_list = [file for file in manifest_info_list if file[S3_FILE_INFO][FILE_NAME] != file_name and file[S3_FILE_INFO][MD5] == org_md5]
        if len(temp_list) > 0:
            msg = f'Data file “{file_name}”: another data file with the same MD5 found.'
            error = create_error("F007", [file_name], "file name", file_name)
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
        # this error will not happen anymore
        # if not self.get_root_path(submission_id):
        #     msg = f'Invalid submission object, no rootPath found, {submission_id}!'
        #     self.log.error(msg)
        #     error = create_error("Invalid submission", msg, "", "Error", SUBMISSION_ID, submission_id)
        #     return STATUS_ERROR, [error]
        self.get_root_path(submission_id)
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
            manifest_info_list = self.mongo_dao.get_files_by_submission(submission_id) if submission_intention != SUBMISSION_INTENTION_DELETE else []
            if not manifest_info_list or len(manifest_info_list) == 0:
                msg = f"No data file records found for the submission."
                self.log.error(msg)
                return None, None
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
                    msg = f'Data file “{file_name}”: associated metadata not found. Please upload associated metadata (aka. manifest) file'
                    self.log.error(msg)
                    error = {
                        TYPE: "data file",
                        "validationType": "data file",
                        "submittedID": file_name,
                        "batchID": batchID,
                        "displayID": displayID,
                        "severity": "Error",
                        "uploadedDate": file.last_modified,
                        "validatedDate": current_datetime(),
                        "errors": [create_error("F008", [file_name], "file name", file_name)]
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
            self.log.exception(e)
            msg = f"{submission_id}: Failed to validate data files! {get_exception_msg()}!"
            self.log.exception(msg)
            error = create_error("F011", [], "", "")
            return None, [error]
    
    def set_status(self, record, qc_result, status, error):
        record[S3_FILE_INFO][UPDATED_AT] = current_datetime()
        if status == STATUS_ERROR:
            record[S3_FILE_INFO][STATUS] = STATUS_ERROR
            qc_result[ERRORS] = [error]
            qc_result[WARNINGS] = []
            qc_result[QC_SEVERITY] = STATUS_ERROR
            
        elif status == STATUS_WARNING: 
            record[S3_FILE_INFO][STATUS] = STATUS_WARNING
            qc_result[WARNINGS] = [error]
            qc_result[ERRORS] = []
            qc_result[QC_SEVERITY] = STATUS_WARNING
            
        else:
            record[S3_FILE_INFO][STATUS] = STATUS_PASSED
            record[S3_FILE_INFO][WARNINGS] = []
            record[S3_FILE_INFO][ERRORS] = []
            qc_result = None

    def save_qc_result(self, fileRecord, status, error):
        qc_result = None
        if not fileRecord.get(S3_FILE_INFO):
            fileRecord[S3_FILE_INFO] = {}
        if fileRecord[S3_FILE_INFO].get(QC_RESULT_ID):
            qc_result = self.mongo_dao.get_qcRecord(fileRecord[S3_FILE_INFO][QC_RESULT_ID])
        if status == STATUS_ERROR or status == STATUS_WARNING:
            if not qc_result:
                qc_result = get_qc_result(fileRecord, VALIDATION_TYPE_FILE, self.mongo_dao)
        self.set_status(fileRecord, qc_result, status, error)
        if status == STATUS_PASSED and qc_result:
            self.mongo_dao.delete_qcRecord(qc_result[ID])
            qc_result = None
            fileRecord[S3_FILE_INFO][QC_RESULT_ID] = None
        if qc_result: # save QC result
            fileRecord[S3_FILE_INFO][QC_RESULT_ID] = qc_result[ID]
            qc_result[QC_VALIDATE_DATE] = current_datetime()
            self.mongo_dao.save_qc_results([qc_result])