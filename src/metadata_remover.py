#!/usr/bin/env python3
import pandas as pd
import numpy as np
import re
import json
import os
from bento.common.utils import get_logger
from bento.common.s3 import S3Bucket
from common.constants import (
    DATA_COMMON_NAME, NODE_ID, FILE_NAME, MODEL_VERSION, ROOT_PATH,
    SUBMISSION_ID, NODE_TYPE, S3_FILE_INFO, BATCH_BUCKET, PARENT_TYPE, PARENT_ID_VAL, PARENTS, ID, TYPE,
    DATA_FILE_TYPE, S3_LIST_ORPHANS_PAGE_SIZE,
    SUBMITTED_ID, QC_VALIDATION_TYPE, BATCH_ID, DISPLAY_ID, QC_SEVERITY,
    UPLOADED_DATE, QC_VALIDATE_DATE, ERRORS, STATUS_ERROR,
)
from common.utils import get_exception_msg, create_error, current_datetime

"""
Process delete metadata requests.
"""
class MetadataRemover:
    
    def __init__(self, mongo_dao, model_store):
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.errors = []
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

    def remove_metadata(self, submission_id, node_type, node_ids, delete_orphaned_data_files=False):
        """
        Delete metadata dataRecords for the given submission / node type / ids.

        delete_orphaned_data_files:
            False (default): Remove Mongo dataRecords only (including cascaded children).
                Do not delete S3 objects for removed file nodes; orphan scan still emits F008
                for unreferenced keys and does not delete them from S3.
            True: Also delete S3 objects for removed file nodes during the cascade, delete
                orphan keys in the post-pass scan, and emit F008 as today.
        """
        msg = None
        try:
            #1 validate submission
            submission = self.mongo_dao.get_submission(submission_id)
            if not submission:
                msg = f'Invalid submission, no record found, {submission_id}!'
                self.log.error(msg)
                return (False, [])
            if not submission.get(DATA_COMMON_NAME):
                msg = f'Invalid submission, missing {DATA_COMMON_NAME}, {submission_id}!'
                self.log.error(msg)
                return (False, [])
            self.submission = submission
            self.datacommon = submission.get(DATA_COMMON_NAME)
            self.submission_id  = submission_id
            self.root_path = submission.get(ROOT_PATH)
            model_version = submission.get(MODEL_VERSION) 
            self.model = self.model_store.get_model_by_data_common_version(self.datacommon, model_version)
            if not self.model.model or not self.model.get_nodes():
                msg = f'{self.datacommon} model version "{model_version}" is not available.'
                self.log.error(msg)
                return (False, [])
            self.def_file_nodes = self.model.get_file_nodes()
            self.bucket = S3Bucket(submission.get(BATCH_BUCKET))
            #2. validate metadata for the type and ids
            existed_nodes = self.validate_data(submission_id, node_type, node_ids)
            if not existed_nodes or len(existed_nodes) == 0:
                return (False, [])
            if not self.delete_nodes(existed_nodes, delete_orphaned_data_files):
                return (False, [])
            #3. after successful delete: find orphaned files, optionally delete them, build F008 errors
            orphan_errors = self._find_orphaned_files_and_build_errors(submission_id, delete_orphaned_data_files)
            return True, orphan_errors
        except Exception:
            self.log.exception(f'Failed to delete metadata, {get_exception_msg()}!')
            return False, []
    
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
    
    def delete_nodes(self, existed_nodes, delete_orphaned_data_files=False):
        """
        Remove dataRecords for the given nodes. When delete_orphaned_data_files is True,
        also remove their S3 file objects; when False, skip S3 for this batch and rely on
        the orphan pass for F008 reporting.
        """
        if len(existed_nodes) == 0:
            return True
        deleted_file_nodes = [node[S3_FILE_INFO] for node in existed_nodes if node.get(S3_FILE_INFO)]
        try:    
            if self.mongo_dao.delete_data_records(existed_nodes):
                s3_ok = (
                    self.delete_files_in_s3(deleted_file_nodes)
                    if delete_orphaned_data_files
                    else True
                )
                return s3_ok and self.process_children(existed_nodes, delete_orphaned_data_files)
            else:
                self.errors.append(f'deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
                return False
        except Exception as e:
            msg = f'Failed to delete metadata data and data file, {get_exception_msg(e)}!'
            self.log.exception(msg)
            return False
       
    def process_children(self, deleted_nodes, delete_orphaned_data_files=False):
        """
        Update or delete child dataRecords after parents are removed.
        When delete_orphaned_data_files is False, child file nodes are removed from Mongo
        but their S3 objects are left in place (orphan scan reports F008).
        """
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
        deleted_parent_keys = {(item[NODE_TYPE], item[NODE_ID]) for item in deleted_nodes}
        file_def_types = self.def_file_nodes.keys()
        for node in child_nodes:
            parents = [
                p for p in (node.get(PARENTS) or [])
                if (p.get(PARENT_TYPE), p.get(PARENT_ID_VAL)) not in deleted_parent_keys
            ]
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
                result = (
                    self.delete_files_in_s3(file_nodes)
                    if delete_orphaned_data_files
                    else True
                )
                if result: # delete grand children...
                    if not self.process_children(deleted_child_nodes, delete_orphaned_data_files):
                        self.errors.append(f'deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
                        rtn_val = rtn_val and False
                else:
                    rtn_val = rtn_val and False
            else:
                self.errors.append(f'Deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
                rtn_val = rtn_val and False
        return rtn_val

    def _process_s3_list_page(self, response, manifest_file_names, orphan_s3_infos):
        """Process one page of list_objects_v2 response; append orphan items to orphan_s3_infos. Return NextContinuationToken."""
        for item in response.get("Contents") or []:
            obj_key = item.get("Key") or ""
            if "/log" in obj_key:
                continue
            file_name = obj_key.split("/")[-1]
            if not file_name or file_name in manifest_file_names:
                continue
            orphan_s3_infos.append({
                FILE_NAME: file_name,
                "last_modified": item.get("LastModified"),
            })
        return response.get("NextContinuationToken")

    def _find_orphaned_files_and_build_errors(self, submission_id, delete_orphaned_data_files):
        """
        After metadata deletion: find S3 keys under file/ not referenced by any remaining dataRecord.
        Always returns F008-shaped errors for those orphans.
        If delete_orphaned_data_files is True, also delete those orphan objects from S3.
        If False, only report F008 (objects remain in the bucket).
        """
        if not self.bucket or not self.root_path:
            return []
        orphan_errors = []
        try:
            manifest_info_list = self.mongo_dao.get_files_by_submission(submission_id) or []
            manifest_file_names = set()
            for manifest_info in manifest_info_list:
                if manifest_info.get(S3_FILE_INFO) and manifest_info[S3_FILE_INFO].get(FILE_NAME):
                    manifest_file_names.add(manifest_info[S3_FILE_INFO][FILE_NAME])

            # S3 keys use forward slashes; paginate list_objects_v2 (first page, then while token)
            key = (os.path.join(self.root_path, "file") + "/").replace("\\", "/")
            orphan_s3_infos = []

            response = self.bucket.client.list_objects_v2(
                Bucket=self.bucket.bucket_name,
                Prefix=key,
                MaxKeys=S3_LIST_ORPHANS_PAGE_SIZE,
            )
            continuation_token = self._process_s3_list_page(response, manifest_file_names, orphan_s3_infos)
            while continuation_token:
                response = self.bucket.client.list_objects_v2(
                    Bucket=self.bucket.bucket_name,
                    Prefix=key,
                    MaxKeys=S3_LIST_ORPHANS_PAGE_SIZE,
                    ContinuationToken=continuation_token,
                )
                continuation_token = self._process_s3_list_page(response, manifest_file_names, orphan_s3_infos)

            if delete_orphaned_data_files and orphan_s3_infos:
                self.delete_files_in_s3([{FILE_NAME: info[FILE_NAME]} for info in orphan_s3_infos])

            for info in orphan_s3_infos:
                file_name = info[FILE_NAME]
                file_batch = self.mongo_dao.find_batch_by_file_name(submission_id, DATA_FILE_TYPE, file_name)
                batch_id = file_batch[ID] if file_batch else "-"
                display_id = file_batch.get(DISPLAY_ID) if file_batch else None
                error = {
                    TYPE: DATA_FILE_TYPE,
                    QC_VALIDATION_TYPE: DATA_FILE_TYPE,
                    SUBMITTED_ID: file_name,
                    BATCH_ID: batch_id,
                    DISPLAY_ID: display_id,
                    QC_SEVERITY: STATUS_ERROR,
                    UPLOADED_DATE: info.get("last_modified"),
                    QC_VALIDATE_DATE: current_datetime(),
                    ERRORS: [create_error("F008", [file_name], "file name", file_name)],
                }
                orphan_errors.append(error)
        except Exception:
            self.log.exception(f"Failed to find orphaned files or build F008 errors: {get_exception_msg()}")
        return orphan_errors

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

  