#!/usr/bin/env python3
import pandas as pd
import os, io, json
from bento.common.utils import get_logger
from common.constants import S3_FILE_INFO, ID, EXPORT_METADATA, S3_FILE_INFO, ID, SIZE, MD5, FILE_NAME, ROOT_PATH, BATCH_BUCKET, NODE_ID, PROD_BUCKET_CONFIG_NAME,\
    DCF_PREFIX, STUDY_ID, DBGA_PID, CONTROL_ACCESS, CONSENT_CODE

from common.utils import get_date_time, get_exception_msg, get_uuid_str

# Private class
class GenerateDCF:
    OPEN_DATA_CONSENT_CODE = "-1"
    def __init__(self, configs, mongo_dao, submission, s3_service, release_manifest_data):
        self.log = get_logger("Generate DCF manifest")
        self.config = configs
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service
        self.production_bucket_name = os.environ.get('DM_BUCKET_NAME')
        self.release_manifest_data = release_manifest_data

    def close(self):
        if self.s3_service:
            self.s3_service.close(self.log)

    def generate_dcf(self):
        # 1) get file nodes by submission id
        file_nodes = self.mongo_dao.get_files_by_submission(self.submission[ID])
        if not file_nodes or len(file_nodes) == 0:
            self.log.error(f'{self.submission[ID]}: No file nodes for the submission.')
            return False
        #2) create df and add records for DCF manifest
        """GUID: file ID, in CDS it's value of property file_id
        md5: file's MD5
        size: file's size
        acl: if study has controlled data (Submission.controlledAccess == true), value should be ["<Submission.dbGaPID"], otherwise open data should have value ["*"]
        authz: if study has controlled data (Submission.controlledAccess == true), value should be ["/programs/<Submission.dbGaPID"], otherwise open data should have value ["/open"]
        urls: file's S3 URL, in the form of "S3://<data_management_bucket_name>/<Submission.dataCommons>/<Submission.studyID>/<file_name>
        """            
        rows = []
        columns = ["guid", "md5", "size", "acl", "authz", "urls"]
        control_access = self.submission.get(CONTROL_ACCESS, False)
        dbGaPID = self.submission.get(DBGA_PID)
        if control_access == True and not dbGaPID:
            self.log.error(f'If control access is set true, dbGaPID is required!')
            return 
        dbGaPID = dbGaPID.split('.')[0] if dbGaPID else None
        url =  f's3://{self.config[PROD_BUCKET_CONFIG_NAME]}/{self.submission.get(STUDY_ID)}/'
        for r in file_nodes:
            node_id = r[NODE_ID] if r[NODE_ID].startswith(DCF_PREFIX) else DCF_PREFIX + r[NODE_ID]
            consent_code_list = r.get(CONSENT_CODE, None)
            if not control_access:
                acl ="['*']"
                authz = "['/open']"
            elif consent_code_list:
                acl_list = []
                authz_list = []
                for consent_code in consent_code_list:
                    if consent_code == self.OPEN_DATA_CONSENT_CODE:
                        acl_list.append("*")
                        authz_list.append("/open")
                    else:
                        acl_list.append(f"{dbGaPID}.c{consent_code}")
                        authz_list.append(f"/programs/{dbGaPID}.c{consent_code}")
                acl = json.dumps(acl_list)
                authz = json.dumps(authz_list)
                
            else:
                acl = f'["{dbGaPID}"]'
                authz = f'["/programs/{dbGaPID}"]'
            row = {
                "guid": node_id,
                "md5": r[S3_FILE_INFO].get(MD5),
                "size": r[S3_FILE_INFO].get(SIZE),
                "acl":  acl,
                "authz": authz,
                "urls": os.path.join(url, r[S3_FILE_INFO].get(FILE_NAME))
            }
            rows.append(row)
        df = None
        buf = None
        try:
            df = pd.DataFrame(rows, columns = columns)
            buf = io.BytesIO()
            df.to_csv(buf, sep ='\t', index=False)
            buf.seek(0)
            # 3) upload buffer to s3 bucket
            self.upload_file(buf)
            self.log.info(f"{self.submission[ID]}: DCF manifest are exported.")
            return True
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f'{self.submission[ID]}: Failed to generate DCF manifest: {get_exception_msg()}.')
            return False
        finally:
            if buf:
                del buf
                del df
                del rows
                del columns

        return True
    
    def upload_file(self, buf):
        id, root_path, bucket_name = self.submission[ID], self.submission[ROOT_PATH], self.submission[BATCH_BUCKET]   
        full_name = f"{root_path}/{EXPORT_METADATA}/release/dcf_manifest/{get_date_time()}-{id}-indexd.tsv"
        self.release_manifest_data["metadata files"]["dcf manifest file path"] = f"dcf_manifest/{get_date_time()}-{id}-indexd.tsv"
        self.s3_service.upload_file_to_s3(buf, bucket_name, full_name)
        