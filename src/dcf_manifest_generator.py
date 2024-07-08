#!/usr/bin/env python3
import pandas as pd
import json, os
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import S3_FILE_INFO, SUBMISSION_ID, BATCH_BUCKET, TYPE_EXPORT_METADATA, ID, NODE_TYPE, \
    RELEASE, ARCHIVE_RELEASE, EXPORT_METADATA, EXPORT_ROOT_PATH, SERVICE_TYPE_EXPORT, CRDC_ID, NODE_ID,\
    DATA_COMMON_NAME, CREATED_AT, MODEL_VERSION, MODEL_FILE_DIR, TIER_CONFIG, SQS_NAME, TYPE, UPDATED_AT, \
    PARENTS, PROPERTIES, SUBMISSION_REL_STATUS, SUBMISSION_REL_STATUS_RELEASED, SUBMISSION_INTENTION, \
    SUBMISSION_INTENTION_DELETE, SUBMISSION_REL_STATUS_DELETED, TYPE_COMPLETE_SUB, ORIN_FILE_NAME, \
    S3_FILE_INFO, ID, SIZE, MD5, UPDATED_AT, \
    FILE_NAME, STATUS_ERROR, STATUS_WARNING, STATUS_PASSED, SUBMISSION_ID, \
    BATCH_BUCKET, SERVICE_TYPE_FILE, LAST_MODIFIED, CREATED_AT, TYPE, SUBMISSION_INTENTION, SUBMISSION_INTENTION_DELETE,\
    VALIDATION_ID, VALIDATION_ENDED
from common.utils import current_datetime, get_uuid_str, dump_dict_to_json, get_exception_msg
from common.model_store import ModelFactory
import threading
import io

# Private class
class GenerateDCF:
    def __init__(self, mongo_dao, submission, s3_service, model_store):
        self.log = get_logger(TYPE_EXPORT_METADATA)
        self.model_store = model_store
        self.model = None
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service
        self.intention = submission.get(SUBMISSION_INTENTION)
        self.production_bucket_name = os.environ.get('DM_BUCKET_NAME')

    def close(self):
        if self.s3_service:
            self.s3_service.close(self.log)

    def generate(self):
        # 1) get file nodes by submission id
        file_nodes = self.mongo_dao.get_files_by_submission(self.submission[ID])
        if not file_nodes or len(file_nodes) == 0:
            return True  #if noting to generate
        manifest_file_list = [ manifest_info[S3_FILE_INFO] for manifest_info in file_nodes]
        #2) create df and add records for DCF manifest
        """GUID: file ID, in CDS it's value of property file_id
        md5: file's MD5
        size: file's size
        acl: if study has controlled data (Submission.controlledAccess == true), value should be ["<Submission.dbGaPID"], otherwise open data should have value ["*"]
        authz: if study has controlled data (Submission.controlledAccess == true), value should be ["/programs/<Submission.dbGaPID"], otherwise open data should have value ["/open"]
        urls: file's S3 URL, in the form of "S3://<data_management_bucket_name>/<Submission.dataCommons>/<Submission.studyID>/<file_name>
        """            
        rows = []
        columns = ["GUID", "md5", "size", "acl", "authz", "urls"]
        acl ='["*"]' if not self.submission.get("controlledAccess", False) else f'[{self.submission.get("dbGaPID")}]'
        authz = '["/open"]' if not self.submission.get("controlledAccess", False) else f'["/programs/{self.submission.get('dbGaPID')}"]'
        url =  f"S3://{self.config.get("production_bucket_name")}/{self.submission[DATA_COMMON_NAME]}/{self.submission["studyID"]}/"
        for r in manifest_file_list:
            row = {
                "GUID": r.get("file_id"),
                "md5": r.get(MD5),
                "size": r.get(SIZE),
                "acl": acl,
                "authz": authz,
                "urls": os.path.join(url, r.get(FILE_NAME))
            }
            rows.append(row)

        df = None
        buf = None
        try:
            df = pd.DataFrame(rows, columns = columns)
            buf = io.BytesIO()
            df.to_csv(buf, sep ='\t', index=False)
            buf.seek(0)
            self.upload_file(buf)
            self.log.info(f"{self.submission[ID]}: DCF manifest are exported.")
            return
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f'{self.submission[ID]}: Failed to generate DCF manifest: {get_exception_msg()}.')
        finally:
            if buf:
                del buf
                del df
                del rows
                del columns

        return True
    
    def upload_to_s3(self):
        return True
        