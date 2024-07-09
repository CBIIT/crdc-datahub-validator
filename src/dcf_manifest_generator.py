#!/usr/bin/env python3
import pandas as pd
import os, io
from bento.common.utils import get_logger
from common.constants import S3_FILE_INFO, ID, EXPORT_METADATA, DATA_COMMON_NAME,\
    S3_FILE_INFO, ID, SIZE, MD5, FILE_NAME
from common.utils import get_date_time, get_exception_msg

# Private class
class GenerateDCF:
    def __init__(self, mongo_dao, submission, s3_service):
        self.log = get_logger("Generate DCF manifest")
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service
        self.production_bucket_name = os.environ.get('DM_BUCKET_NAME')

    def close(self):
        if self.s3_service:
            self.s3_service.close(self.log)

    def generate_dcf(self):
        # 1) get file nodes by submission id
        file_nodes = self.mongo_dao.get_files_by_submission(self.submission[ID])
        if not file_nodes or len(file_nodes) == 0:
            return True  #if nothing to generate
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
    
    def upload_file(self, buf):
        id, root_path, bucket_name = self.get_submission_info()      
        full_name = f"{root_path}/{EXPORT_METADATA}/dcf_manifest/{id}-{get_date_time()}-indexd.tsv"
        self.s3_service.upload_file_to_s3(buf, bucket_name, full_name)
    
        