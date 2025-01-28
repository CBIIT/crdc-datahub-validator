import uuid, os, sys
from urllib.parse import urlparse

from common.mongo_dao import MongoDao

DH_PROD_URL = 'https://hub.datacommons.cancer.gov/'
DB_NAME = 'crdc-datahub'
DCF_PREFIX = 'dg.4DFC/'

# Following constants are model dependent, and currently set to match CDS data model
FILE_NODE_NAME = 'file'
FILE_NAME_FIELD = 'file_name'
FILE_ID_FIELD = 'file_id'
FILE_ID_INCLUDES_DCF_PREFIX = True

class ReplaeFileId():
    def __init__(self, mongo_con_str, db_name, submission_id):
        self.mongo_con_str = mongo_con_str
        self.submission_id = submission_id
        self.db_name = db_name
        self.mongo_dao = MongoDao(mongo_connnection_str, db_name)
        self.study_id = self.mongo_dao.get_submission(submission_id)['studyID']

    def print_info(self):
        print(f'DB address: {urlparse(self.mongo_con_str).hostname}')
        print(f'DB name: {self.db_name}')
        print(f'Submission ID: {self.submission_id}')
        print(f'Study ID: {self.study_id}')

    def replace_file_ids(self):
        all_files = self.mongo_dao.get_dataRecords_chunk_by_nodeType(self.submission_id, FILE_NODE_NAME, 0, 10000)
        touched_files = []
        for file in all_files:
            print(f'========== Processing file: {file["_id"]} ==============')
            drs_id = self._generate_drs_id(file['props'][FILE_NAME_FIELD])
            crdc_id = drs_id
            file_id = drs_id if FILE_ID_INCLUDES_DCF_PREFIX else drs_id.replace(DCF_PREFIX, '')
            if file['CRDC_ID'] != crdc_id:
                print(f'CRDC ID: {file["CRDC_ID"]} -> New: {crdc_id}')
            if file['nodeID'] != file_id:
                print(f'NODE ID: {file["nodeID"]} -> New: {file_id}')
                touched_files.append(file)
            if file["props"][FILE_ID_FIELD] != file_id:
                print(f'File ID: {file["props"][FILE_ID_FIELD]} -> New: {file_id}')
        print(f'{len(all_files)} records found')
        print(f'{len(touched_files)} records need to be updated')

    # Generate DRS ID based on CRDC DH rule
    # DH -> Study ID -> Folder (optional) -> file name
    def _generate_drs_id(self, file_name, folder=None):
        url_UUID = uuid.uuid5(uuid.NAMESPACE_URL, DH_PROD_URL)
        study_UUID = uuid.uuid5(url_UUID, self.study_id)
        file_path = file_name
        if folder is not None:
            file_path = os.path.join(folder, file_name)
        file_UUID = f'{DCF_PREFIX}{uuid.uuid5(study_UUID, file_path)}'
        # print(f'{os.path.join(DH_PROD_URL, self.study_id, file_path)} => {file_UUID}')
        return file_UUID



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'Usage: {os.path.basename(sys.argv[0])} <submission ID> <MongoDB connection string> [database name]')
        sys.exit(1)
    submission_id = sys.argv[1]
    mongo_connnection_str = sys.argv[2]
    db_name = sys.argv[3] if len(sys.argv) > 3 else DB_NAME

    replacer = ReplaeFileId(mongo_connnection_str, db_name, submission_id)
    replacer.print_info()
    replacer.replace_file_ids()
    # replacer.replace_file_ids()