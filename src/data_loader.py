#!/usr/bin/env python3
import os
import pandas as pd
from bento.common.utils import get_logger
from common.utils import get_uuid_str, current_datetime_str, get_exception_msg
from common.constants import  TYPE, ID, SUBMISSION_ID, STATUS, STATUS_NEW, \
    ERRORS, WARNINGS, CREATED_AT , UPDATED_AT, BATCH_INTENTION, S3_FILE_INFO, FILE_NAME, \
    MD5, INTENTION_NEW, INTENTION_UPDATE, INTENTION_DELETE, SIZE, PARENT_ID_VAL, PARENT_TYPE, \
    FILE_NAME_FIELD, FILE_SIZE_FIELD, FILE_MD5_FIELD, NODE_ID, NODE_TYPE, PARENTS
SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
BATCH_IDS = "batchIDs"

# This script load matadata files to database
# input: file info list
class DataLoader:
    def __init__(self, model, batch, mongo_dao, bucket, root_path):
        self.log = get_logger('Matedata loader')
        self.model = model
        self.mongo_dao =mongo_dao
        self.batch = batch
        self.bucket = bucket
        self.root_path = root_path
        self.file_nodes = self.model.get_file_nodes()
        self.errors = None

    """
    param: file_path_list downloaded from s3 bucket
    """
    def load_data(self, file_path_list):
        returnVal = True
        self.errors = []
        intention = self.batch.get(BATCH_INTENTION, INTENTION_NEW)
        file_types = None if intention == INTENTION_DELETE else [k for (k,v) in self.file_nodes.items()]
        deleted_nodes = [] if intention == INTENTION_DELETE else None
        deleted_file_nodes = [] if intention == INTENTION_DELETE else None
        for file in file_path_list:
            records = [] if intention != INTENTION_DELETE else None
            failed_at = 1
            # 1. read file to dataframe
            if not os.path.isfile(file):
                self.errors.append(f"File does not exist, {file}")
                continue
            try:
                df = pd.read_csv(file, sep=SEPARATOR_CHAR, header=0, encoding=UTF8_ENCODE)
                df = df.reset_index()  # make sure indexes pair with number of rows
                col_names =list(df.columns)
                
                for index, row in df.iterrows():
                    type = row[TYPE]
                    exist_node = None if intention == INTENTION_NEW else self.mongo_dao.get_dataRecord_by_node(node_id, type, self.batch[SUBMISSION_ID])
                    if intention == INTENTION_DELETE and exist_node:
                        deleted_nodes.append(exist_node)
                        if exist_node.get(NODE_TYPE) in self.file_nodes.keys() and exist_node.get(S3_FILE_INFO):
                            deleted_file_nodes.append(exist_node[S3_FILE_INFO])
                        continue
                    # 2. construct dataRecord
                    rawData = df.loc[index].to_dict()
                    del rawData['index'] #remove index column
                    relation_fields = [name for name in col_names if '.' in name]
                    prop_names = [name for name in col_names if not name in [TYPE, 'index'] + relation_fields]
                    node_id = self.get_node_id(type, row)
                    
                    batchIds = [self.batch[ID]] if intention == INTENTION_NEW or not exist_node else  exist_node[BATCH_IDS] + [self.batch[ID]]
                    current_date_time = current_datetime_str()
                    id = self.get_record_id(intention, exist_node)
                    dataRecord = {
                        ID: id,
                        "CRDC_ID": id,
                        SUBMISSION_ID: self.batch[SUBMISSION_ID],
                        BATCH_IDS: batchIds,
                        "latestBatchID": self.batch[ID],
                        "uploadedDate": current_date_time, 
                        STATUS: STATUS_NEW,
                        ERRORS: [],
                        WARNINGS: [],
                        CREATED_AT : current_date_time, 
                        UPDATED_AT: current_date_time, 
                        "orginalFileName": os.path.basename(file),
                        "lineNumber": index,
                        "nodeType": type,
                        "nodeID": node_id,
                        "props": {k: v for (k, v) in rawData.items() if k in prop_names},
                        "parents": self.get_parents(relation_fields, row),
                        "rawData":  rawData
                    }
                    if type in file_types:
                        dataRecord[S3_FILE_INFO] = self.get_file_info(type, prop_names, row)
                    records.append(dataRecord)
                    failed_at += 1

                # 3-1. insert data in a tsv file into mongo DB
                if intention == INTENTION_NEW:
                    returnVal = returnVal and self.mongo_dao.insert_data_records(records)
                elif intention == INTENTION_UPDATE:
                    returnVal = returnVal and self.mongo_dao.update_data_records(records)
                
            except Exception as e:
                    df = None
                    self.log.debug(e)
                    msg = f"Failed to load data in file, {file} at {failed_at + 1}! {get_exception_msg()}."
                    self.log.exception(msg)
                    self.errors.append(msg)
                    return False, self.errors
            
        #3-2. delete all records in deleted_ids
        if intention == INTENTION_DELETE:
            try:
                returnVal = self.delete_nodes(deleted_nodes, deleted_file_nodes)
            except Exception as e:
                    df = None
                    self.log.debug(e)
                    msg = f"Failed to delete metadata for the batch, {self.batch[ID]}! {get_exception_msg()}."
                    self.log.exception(msg)
                    self.errors.append(msg)
                    return False, self.errors

        return returnVal, self.errors
    """
    delete nodes
    """
    def delete_nodes(self, deleted_nodes, deleted_file_nodes):
        if len(deleted_nodes) == 0:
            return True
        if self.mongo_dao.delete_data_records(deleted_nodes):
                self.delete_files_in_s3(deleted_file_nodes)
                self.process_children(deleted_nodes) 
        else:
            self.errors.append(f"Failed to delete data records!")
            returnVal = False
        
    """
    process related children record in dataRecords
    """
    def process_children(self, deleted_nodes):
        # retrieve child nodes
        status, child_nodes = self.mongo_dao.get_nodes_by_parents(deleted_nodes, self.batch[SUBMISSION_ID])
        if not status: # if exception occurred
            self.errors.append(f"Failed to retrieve child nodes!")
            return False

        if len(child_nodes) == 0: # if no child
            return True
        
        rtn_val = True
        deleted_child_nodes = []
        updated_child_nodes = []
        file_nodes = []
        parent_types = [item[NODE_TYPE] for item in deleted_nodes]
        file_def_types = self.file_nodes.key()
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
                self.errors.append(f"Failed to update child nodes!")
                rtn_val = rtn_val and False

        if len(deleted_child_nodes) > 0:
            deleted_results = self.mongo_dao.delete_data_records(deleted_child_nodes, self.batch[SUBMISSION_ID])
            if updated_results and deleted_results: 
                #delete files
                result = self.delete_files_in_s3(file_nodes)
                if result: # delete grand children...
                    if not self.process_children(deleted_child_nodes):
                        self.errors.append(f"Failed to delete grand child nodes!")
                        rtn_val = rtn_val and False
                else:
                    self.errors.append(f"Failed to delete child files!")
                    rtn_val = rtn_val and False
            else:
                self.errors.append(f"Failed to delete child nodes!")
                rtn_val = rtn_val and False
        return rtn_val
    
    """
    delete files in s3 after deleted file nodes
    """
    def delete_files_in_s3(self, file_s3_infos):
        if len(file_s3_infos) == 0:
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
                        self.errors.append(f"Failed to delete file, {key}, in s3 bucket!")
                        rtn_val = rtn_val and False
                else:
                    self.errors.append(f"The file,{key}, does not exit in s3 bucket!")
                    rtn_val = rtn_val and False
            except Exception  as e:
                self.log.debug(e)
                msg = f"Failed to delete file in s3 bucket, {key}! {get_exception_msg()}."
                self.log.exception(msg)
                self.errors.append(msg)
                rtn_val = rtn_val and False
        return rtn_val
    
    """
    get node id defined in model dict
    """
    def get_record_id(self, intention, node):
        if intention == INTENTION_NEW:
            return get_uuid_str()
        else:
            return node[ID] if node else get_uuid_str()

    
    """
    get node id defined in model dict
    """
    def get_node_id(self, type, row):
        id_field = self.model.get_node_id(type)
        return row[id_field] if id_field else None
    
    """
    get parents based on relationship fields that in format of
    [parent node].parentNodeID
    """
    def get_parents(self, relation_fields, row):
        parents = []
        for relation in relation_fields:
            temp = relation.split('.')
            parents.append({"parentType": temp[0], "parentIDPropName": temp[1], "parentIDValue": row[relation]})
        return parents
    
    """
    get file information by a file node type
    """
    def get_file_info(self, type, prop_names, row):
        file_fields = self.file_nodes.get(type)
        file_name = row[file_fields[FILE_NAME_FIELD]] if file_fields[FILE_NAME_FIELD] in prop_names else None
        file_size = row[file_fields[FILE_SIZE_FIELD]] if file_fields[FILE_SIZE_FIELD] in prop_names else None
        file_md5 = row[file_fields[FILE_MD5_FIELD]] if file_fields[FILE_MD5_FIELD] in prop_names else None
        current_date_time = current_datetime_str()
        return {
            FILE_NAME: file_name,
            SIZE: file_size,
            MD5: file_md5,
            STATUS: STATUS_NEW,
            ERRORS: [],
            WARNINGS: [],
            CREATED_AT: current_date_time, 
            UPDATED_AT: current_date_time
        }