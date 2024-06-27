#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from bento.common.utils import get_logger
from common.utils import get_uuid_str, current_datetime, removeTailingEmptyColumnsAndRows
from common.constants import  TYPE, ID, SUBMISSION_ID, STATUS, STATUS_NEW, NODE_ID, \
    ERRORS, WARNINGS, CREATED_AT , UPDATED_AT, S3_FILE_INFO, FILE_NAME, \
    MD5, SIZE, PARENT_TYPE, DATA_COMMON_NAME,\
    FILE_NAME_FIELD, FILE_SIZE_FIELD, FILE_MD5_FIELD, NODE_TYPE, PARENTS, CRDC_ID, PROPERTIES, \
    ORIN_FILE_NAME, ADDITION_ERRORS, RAW_DATA
SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
BATCH_IDS = "batchIDs"

# This script load matadata files to database
# input: file info list
class DataLoader:
    def __init__(self, model, batch, mongo_dao, bucket, root_path, data_common, submission_intention):
        self.log = get_logger('Matedata loader')
        self.model = model
        self.mongo_dao =mongo_dao
        self.batch = batch
        self.bucket = bucket
        self.root_path = root_path
        self.data_common = data_common
        self.file_nodes = self.model.get_file_nodes()
        self.errors = None

    """
    param: file_path_list downloaded from s3 bucket
    """
    def load_data(self, file_path_list):
        returnVal = True
        self.errors = []
        file_types = [k for (k,v) in self.file_nodes.items()]
       
        for file in file_path_list:
            records = []
            failed_at = 1
            file_name = os.path.basename(file)
            # 1. read file to dataframe
            if not os.path.isfile(file):
                self.errors.append(f"File does not exist, {file}")
                continue
            try:
                df = pd.read_csv(file, sep=SEPARATOR_CHAR, header=0, dtype='str', encoding=UTF8_ENCODE,keep_default_na=False,na_values=[''])
                df = (df.rename(columns=lambda x: x.strip())).apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # stripe white space.
                df = removeTailingEmptyColumnsAndRows(df)
                df = df.replace({np.nan: None})  # replace Nan in dataframe with None
                df = df.reset_index()  # make sure indexes pair with number of rows
                col_names =list(df.columns)
                
                for index, row in df.iterrows():
                    type = row[TYPE]
                    node_id = self.get_node_id(type, row)
                    exist_node = self.mongo_dao.get_dataRecord_by_node(node_id, type, self.batch[SUBMISSION_ID])
                    # 2. construct dataRecord
                    rawData = df.loc[index].to_dict()
                    del rawData['index'] #remove index column
                    relation_fields = [name for name in col_names if '.' in name]
                    prop_names = [name for name in col_names if not name in [TYPE, 'index'] + relation_fields]
                    batchIds = [self.batch[ID]] if not exist_node else  exist_node[BATCH_IDS] + [self.batch[ID]]
                    current_date_time = current_datetime()
                    id = self.get_record_id(exist_node)
                    crdc_id = self.get_crdc_id(exist_node, type, node_id)
                    if index == 0 or not self.process_m2m_rel(records, node_id, rawData, relation_fields):
                        dataRecord = {
                            ID: id,
                            CRDC_ID: crdc_id if crdc_id else id,
                            SUBMISSION_ID: self.batch[SUBMISSION_ID],
                            DATA_COMMON_NAME: self.data_common,
                            BATCH_IDS: batchIds,
                            "latestBatchID": self.batch[ID],
                            "uploadedDate": current_date_time, 
                            STATUS: STATUS_NEW,
                            ERRORS: [],
                            WARNINGS: [],
                            CREATED_AT : current_date_time if not exist_node else exist_node[CREATED_AT], 
                            UPDATED_AT: current_date_time, 
                            ORIN_FILE_NAME: file_name,
                            "lineNumber":  index + 2,
                            NODE_TYPE: type,
                            NODE_ID: node_id,
                            PROPERTIES: {k: v for (k, v) in rawData.items() if k in prop_names},
                            PARENTS: self.get_parents(relation_fields, row),
                            RAW_DATA:  rawData,
                            ADDITION_ERRORS: []
                        }
                        if type in file_types:
                            dataRecord[S3_FILE_INFO] = self.get_file_info(type, prop_names, row)
                        records.append(dataRecord)
                    failed_at += 1

                # 3-1. upsert data in a tsv file into mongo DB
                result, error = self.mongo_dao.update_data_records(records)
                if error:
                    self.errors.append(f'“{file_name}”: updating metadata failed - database error.  Please try again and contact the helpdesk if this error persists.')
                returnVal = returnVal and result

            except Exception as e:
                    self.log.exception(e)
                    upload_type =  "Add/Update"
                    msg = f'“{file_name}”: {upload_type} metadata failed with internal error.  Please try again and contact the helpdesk if this error persists.'
                    self.log.exception(msg)
                    self.errors.append(msg)
                    return False, self.errors
            finally:
                del df
                del records

        del file_path_list
        return returnVal, self.errors
    """
    process_m2m_rel
     1) check if a node with the same nodeID exists in record list. 
     2) check if current row has many to many relationship by comparing with parents property of the existing node.
     3) update the parents property of the existing node in record list.
    """
    def process_m2m_rel(self, records, node_id, row, relation_fields):
        existed_node = next((record for record in records if record[NODE_ID] == node_id), None)
        if not existed_node:
            return False
        else:
            parents = self.get_parents(relation_fields, row)
            if len(parents) == 0:
                return True
            existed_parents = existed_node.get(PARENTS)
            if not existed_parents or len(existed_parents) == 0:
                existed_node[PARENTS] = parents
                return True
            for parent in parents:
                if not any( p.get(PARENT_TYPE) == parent.get(PARENT_TYPE) and p.get("parentIDPropName") ==  parent.get("parentIDPropName") \
                           and p.get("parentIDValue") ==  parent.get("parentIDValue")  for p in existed_parents):
                    existed_node[PARENTS].append(parent)
            return True
        
    # """
    # delete nodes
    # """
    # def delete_nodes(self, deleted_nodes, deleted_file_nodes, file_name):
    #     if len(deleted_nodes) == 0:
    #         return True
    #     if self.mongo_dao.delete_data_records(deleted_nodes):
    #         return self.delete_files_in_s3(deleted_file_nodes, file_name) and self.process_children(deleted_nodes, file_name) 
    #     else:
    #         self.errors.append(f'"{file_name}": deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
    #         return False
        
    # """
    # process related children record in dataRecords
    # """
    # def process_children(self, deleted_nodes, file_name):
    #     # retrieve child nodes
    #     status, child_nodes = self.mongo_dao.get_nodes_by_parents(deleted_nodes, self.batch[SUBMISSION_ID])
    #     if not status: # if exception occurred
    #         self.errors.append(f'"{file_name}": deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
    #         return False

    #     if len(child_nodes) == 0: # if no child
    #         return True
        
    #     rtn_val = True
    #     deleted_child_nodes = []
    #     updated_child_nodes = []
    #     file_nodes = []
    #     parent_types = [item[NODE_TYPE] for item in deleted_nodes]
    #     file_def_types = self.file_nodes.keys()
    #     for node in child_nodes:
    #         parents = list(filter(lambda x: (x[PARENT_TYPE] not in parent_types), node.get(PARENTS)))
    #         if len(parents) == 0:  #delete if no other parents
    #             deleted_child_nodes.append(node)
    #             if node.get(NODE_TYPE) in file_def_types and node.get(S3_FILE_INFO):
    #                 file_nodes.append(node[S3_FILE_INFO])
    #         else: #remove deleted parent and update the node
    #             node[PARENTS] = parents
    #             updated_child_nodes.append(node)

    #     updated_results = True
    #     deleted_results = True
    #     if len(updated_child_nodes) > 0:
    #         result = updated_results = self.mongo_dao.update_data_records(updated_child_nodes)
    #         if not result:
    #             self.errors.append(f'"{file_name}": deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
    #             rtn_val = rtn_val and False

    #     if len(deleted_child_nodes) > 0:
    #         deleted_results = self.mongo_dao.delete_data_records(deleted_child_nodes)
    #         if updated_results and deleted_results: 
    #             #delete files
    #             result = self.delete_files_in_s3(file_nodes, file_name)
    #             if result: # delete grand children...
    #                 if not self.process_children(deleted_child_nodes, file_name):
    #                     self.errors.append(f'"{file_name}": deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
    #                     rtn_val = rtn_val and False
    #             else:
    #                 rtn_val = rtn_val and False
    #         else:
    #             self.errors.append(f'Deleting metadata failed with database error.  Please try again and contact the helpdesk if this error persists.')
    #             rtn_val = rtn_val and False
    #     return rtn_val
    
    # """
    # delete files in s3 after deleted file nodes
    # """
    # def delete_files_in_s3(self, file_s3_infos, file_name):
    #     if not file_s3_infos or len(file_s3_infos) == 0:
    #         return True
    #     rtn_val = True
    #     for s3_info in file_s3_infos:
    #         if not s3_info or not s3_info.get(FILE_NAME):
    #             continue
    #         key = os.path.join(self.root_path, os.path.join("file", s3_info[FILE_NAME]))
    #         try:
    #             if self.bucket.file_exists_on_s3(key):
    #                 result = self.bucket.delete_file(key)
    #                 if not result:
    #                     self.errors.append(f'"{file_name}": deleting data file “{s3_info[FILE_NAME]}” failed.  Please try again and contact the helpdesk if this error persists.')
    #                     rtn_val = rtn_val and False
    #             else:
    #                 self.log.info(f'"{file_name}": data file "{s3_info[FILE_NAME]}" does not exit in s3 bucket!')
    #                 rtn_val = rtn_val and True
    #         except Exception  as e:
    #             self.log.exception(e)
    #             msg = f"Failed to delete file in s3 bucket, {key}! {get_exception_msg()}."
    #             self.log.exception(msg)
    #             self.errors.append(f'"{file_name}": deleting data file “{s3_info[FILE_NAME]}” failed.  Please try again and contact the helpdesk if this error persists.')
    #             rtn_val = rtn_val and False
    #     return rtn_val
    
    """
    get node id 
    """
    def get_record_id(self, node):
        return node[ID] if node else get_uuid_str()

    """
    get node crdc id
    """
    def get_crdc_id(self, exist_node, node_type, node_id):
        if not exist_node:
            if not self.data_common or not node_type or not node_id:
                return None
            else:
                result = self.mongo_dao.search_node(self.data_common, node_type, node_id)
                return None if not result else result[CRDC_ID]
        else:
            return exist_node.get(CRDC_ID)
    
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
    def get_parents(self, relation_fields, rawData):
        parents = []
        for relation in relation_fields:
            val = rawData.get(relation)
            if val:
                temp = relation.split('.')
                parents.append({"parentType": temp[0], "parentIDPropName": temp[1], "parentIDValue": val})
                rawData.update({relation.replace(".", "|"): val})
        return parents
    
    """
    get file information by a file node type
    """
    def get_file_info(self, type, prop_names, row):
        file_fields = self.file_nodes.get(type)
        file_name = row[file_fields[FILE_NAME_FIELD]] if file_fields[FILE_NAME_FIELD] in prop_names else None
        file_size = row[file_fields[FILE_SIZE_FIELD]] if file_fields[FILE_SIZE_FIELD] in prop_names else None
        file_md5 = row[file_fields[FILE_MD5_FIELD]] if file_fields[FILE_MD5_FIELD] in prop_names else None
        current_date_time = current_datetime()
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