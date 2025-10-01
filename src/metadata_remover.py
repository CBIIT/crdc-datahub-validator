#!/usr/bin/env python3
import pandas as pd
import numpy as np
import re
import json
import os
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import DATA_COMMON_NAME,NODE_ID, FILE_NAME, MODEL_VERSION, ROOT_PATH, \
        SUBMISSION_ID,NODE_TYPE, S3_FILE_INFO, BATCH_BUCKET, PARENT_TYPE, PARENTS
from common.utils import get_exception_msg

"""
Process delete metadata requests.
"""
class MetadataRemover:
    
    def __init__(self, mongo_dao, model_store):
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('Essential Validator')
        self.mongo_dao = mongo_dao
        self.model_store = model_store
        self.datacommon = None
        self.model = None
        self.submission = None
        self.submission_id = None
        self.root_path = None
        self.bucket = None
        self.def_file_nodes = None

    def remove_metadata(self, submission_id, node_type, node_ids):
        msg = None
        try:
            #1 validate submission
            submission = self.mongo_dao.get_submission(submission_id)
            if not submission or not submission.get(DATA_COMMON_NAME):
                msg = f'Invalid submission, no record found, {submission_id}!'
                self.log.error(msg)
                return False
            if  not submission.get(DATA_COMMON_NAME):
                msg = f'Invalid submission, no datacommon found, {submission_id}!'
                self.log.error(msg)
                return False
            self.submission = submission
            self.datacommon = submission.get(DATA_COMMON_NAME)
            self.submission_id  = submission_id
            self.root_path = submission.get(ROOT_PATH)
            model_version = submission.get(MODEL_VERSION) 
            self.model = self.model_store.get_model_by_data_common_version(self.datacommon, model_version)
            if not self.model.model or not self.model.get_nodes():
                msg = f'{self.datacommon} model version "{model_version}" is not available.'
                self.log.error(msg)
                return False
            self.def_file_nodes = self.model.get_file_nodes()
            self.bucket = S3Bucket(submission.get(BATCH_BUCKET))
            #2. validate meatadata for the type and ids
            existed_nodes = self.validate_data(submission_id, node_type, node_ids)
            if not existed_nodes or len(existed_nodes) == 0:
                return False
            return self.delete_nodes(existed_nodes)
        except Exception as e:
            self.log.exception(e)
            msg = f'Failed to delete metadata, {get_exception_msg()}!'
            self.log.exception(msg)
            return False
    
    def validate_data(self, submission_id, node_type, node_ids):
        """
        1) verify node_type
        2) verify node_id exists
        """
        msg = None
        existed_nodes = None
                
        # query db to find existed nodes in current submission.  
        existed_nodes = self.mongo_dao.check_metadata_ids(node_type, node_ids, submission_id)  
        if not existed_nodes or len(existed_nodes) == 0:
            msg = f'No metadata found for “{node_type}: "{json.dumps(node_ids)}.'
            self.log.error(msg)
            return None
        
        existed_ids = [item[NODE_ID] for item in existed_nodes]   
        # When metadata intention is "Delete", all IDs must exist in the database 
        not_existed_ids = list(set(node_ids) - set(existed_ids))
        if len(not_existed_ids) > 0:
            msg = f'metadata not found: “{node_type}": "{json.dumps(not_existed_ids)}".'
            self.log.error(msg)

        return existed_nodes
    
    def delete_nodes(self, existed_nodes):
        """
        remove metadata
        """
        if len(existed_nodes) == 0:
            return True
        deleted_file_nodes = [node[S3_FILE_INFO] for node in existed_nodes if node.get(S3_FILE_INFO)]
        try:    
            if self.mongo_dao.delete_data_records(existed_nodes):
                return self.delete_files_in_s3(deleted_file_nodes) and self.process_children(existed_nodes) 
            else:
                self.errors.append(f'deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
                return False
        except Exception as e:
            msg = f'Failed to delete metadata data and data file, {get_exception_msg(e)}!'
            self.log.exception(msg)
            return False
       
    """
    process related children record in dataRecords
    """
    def process_children(self, deleted_nodes):
        # retrieve child nodes
        status, child_nodes = self.mongo_dao.get_nodes_by_parents(deleted_nodes, self.submission_id)
        if not status: # if exception occurred
            self.errors.append(f'deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
            return False

        if len(child_nodes) == 0: # if no child
            return True
        
        rtn_val = True
        deleted_child_nodes = []
        updated_child_nodes = []
        file_nodes = []
        parent_types = [item[NODE_TYPE] for item in deleted_nodes]
        file_def_types = self.def_file_nodes.keys()
        for node in child_nodes:
            parents = list(filter(lambda x: (x[PARENT_TYPE] not in parent_types), node.get(PARENTS)))
            if len(parents) == 0:  #delete if no other parents
                deleted_child_nodes.append(node)
                if node.get(NODE_TYPE) in file_def_types and node.get(S3_FILE_INFO):
                    file_nodes.append(node[S3_FILE_INFO])
            else: #remove deleted parent and update the node
                node[PARENTS] = parents
                updated_child_nodes.append(node)

        updated_results = True
        deleted_results = True
        if len(updated_child_nodes) > 0:
            result = updated_results = self.mongo_dao.update_data_records(updated_child_nodes)
            if not result:
                self.errors.append(f'deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
                rtn_val = rtn_val and False

        if len(deleted_child_nodes) > 0:
            deleted_results = self.mongo_dao.delete_data_records(deleted_child_nodes)
            if updated_results and deleted_results: 
                #delete files
                result = self.delete_files_in_s3(file_nodes)
                if result: # delete grand children...
                    if not self.process_children(deleted_child_nodes):
                        self.errors.append(f'deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
                        rtn_val = rtn_val and False
                else:
                    rtn_val = rtn_val and False
            else:
                self.errors.append(f'Deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
                rtn_val = rtn_val and False
        return rtn_val
    
    """
    delete files in s3 after deleted file nodes
    """
    def delete_files_in_s3(self, file_s3_infos):
        if not file_s3_infos or len(file_s3_infos) == 0:
            return True
        rtn_val = True
        for s3_info in file_s3_infos:
            if not s3_info or not s3_info.get(FILE_NAME):
                continue
            key = os.path.join(self.root_path, os.path.join("file", s3_info[FILE_NAME]))
            try:
                if self.bucket.file_exists_on_s3(key):
                    result = self.bucket.delete_file(key)
                    if not result:
                        self.errors.append(f'deleting data file “{s3_info[FILE_NAME]}” failed.  Please try again and contact the helpdesk if this error persists.')
                        rtn_val = rtn_val and False
                else:
                    self.log.info(f'data file "{s3_info[FILE_NAME]}" does not exit in s3 bucket!')
                    rtn_val = rtn_val and True
            except Exception  as e:
                self.log.exception(e)
                msg = f"Failed to delete file in s3 bucket, {key}! {get_exception_msg()}."
                self.log.exception(msg)
                self.errors.append(f'deleting data file “{s3_info[FILE_NAME]}” failed.  Please try again and contact the helpdesk if this error persists.')
                rtn_val = rtn_val and False
        return rtn_val
    
    def close(self):
        if self.bucket:
            del self.bucket

  