import uuid, os, sys
from urllib.parse import urlparse

from common.mongo_dao import MongoDao

DH_PROD_URL = 'https://hub.datacommons.cancer.gov/'
DCF_PREFIX = 'dg.4DFC/'
# correct DB name for dev, qa, stage and prod
DB_NAME = 'crdc-datahub'
# correct DB name for dev2 and qa2
# DB_NAME = 'crdc-datahub2'

# Following constants are model dependent, and currently set to match CDS data model
FILE_NODE_NAME = 'file'
FILE_NAME_FIELD = 'file_name'
FILE_ID_FIELD = 'file_id'
FILE_ID_INCLUDES_DCF_PREFIX = True

class ReplaeFileId():
    '''
      The purpose of this script is finding all files within a data submission that don't have correct file_ids and
      replace the file_ids and crdc_ids with correct DRS IDs
    '''
    def __init__(self, mongo_con_str, db_name, submission_id):
        self.mongo_con_str = mongo_con_str
        self.submission_id = submission_id
        self.db_name = db_name
        self.mongo_dao = MongoDao(mongo_connnection_str, db_name)
        submission = self.mongo_dao.get_submission(submission_id)
        if not submission:
            print(f'Submission ID {submission_id} not found in DB')
            sys.exit(1)
        self.study_id = submission['studyID']

    def print_info(self):
        print(f'DB address: {urlparse(self.mongo_con_str).hostname}')
        print(f'DB name: {self.db_name}')
        print(f'Submission ID: {self.submission_id}')
        print(f'Study ID: {self.study_id}')

    def replace_file_ids(self, do_update=False):
        all_files = self.mongo_dao.get_dataRecords_chunk_by_nodeType(self.submission_id, FILE_NODE_NAME, 0, 10000)
        touched_files = []
        for file in all_files:
            print(f'========== Processing file: {file["_id"]} ==============')
            drs_id = self._generate_drs_id(file['props'][FILE_NAME_FIELD])
            crdc_id = drs_id
            file_id = drs_id if FILE_ID_INCLUDES_DCF_PREFIX else drs_id.replace(DCF_PREFIX, '')
            needs_update = False
            if file['CRDC_ID'] != crdc_id:
                print(f'CRDC ID: {file["CRDC_ID"]} -> New: {crdc_id}')
                file['CRDC_ID'] = crdc_id
                needs_update = True
            if file['nodeID'] != file_id:
                print(f'NODE ID: {file["nodeID"]} -> New: {file_id}')
                file['nodeID'] = file_id
                needs_update = True
                touched_files.append(file)
            if file["props"][FILE_ID_FIELD] != file_id:
                print(f'File ID: {file["props"][FILE_ID_FIELD]} -> New: {file_id}')
                file['props'][FILE_ID_FIELD] = file_id
                needs_update = True

            if do_update and needs_update:
                self.mongo_dao.update_file(file)

        print(f'{len(all_files)} records found')
        print(f'{len(touched_files)} records {"need to be" if not do_update else "have been"} updated')

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
    if len(sys.argv) < 3:
        print(f'Usage: {os.path.basename(sys.argv[0])} <MongoDB connection string> <submission ID>  [Do Update(true/false)]')
        sys.exit(1)
    mongo_connnection_str = sys.argv[1]
    submission_id = sys.argv[2]
    do_update = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else False

    replacer = ReplaeFileId(mongo_connnection_str, DB_NAME, submission_id)
    replacer.print_info()
    replacer.replace_file_ids(do_update)
    # replacer.replace_file_ids()