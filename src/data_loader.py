#!/usr/bin/env python3
import os
import pandas as pd
from bento.common.utils import get_logger
from common.utils import get_uuid_str, current_datetime_str, get_exception_msg
from common.constants import MODEL, ID_PROPERTY, TYPE, ID, SUBMISSION_ID, FILE_STATUS, STATUS_NEW, \
    ERRORS, WARNINGS, BATCH_CREATED, UPDATED_AT, BATCH_INTENTION, S3_FILE_INFO, FILE_NAME, \
    MD5, INTENTION_NEW, INTENTION_UPDATE, INTENTION_DELETE, SIZE, \
    FILE_NAME_FIELD, FILE_SIZE_FIELD, FILE_MD5_FIELD 
SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
BATCH_IDS = "batchIDs"

# This script load matadata files to database
# input: file info list
class DataLoader:
    def __init__(self, model, batch, mongo_dao):
        self.log = get_logger('Matedata loader')
        self.model = model
        self.mongo_dao =mongo_dao
        self.batch = batch
        self.file_nodes = self.model.model[MODEL].get("file-nodes", {})
        self.errors = None

    """
    param: file_path_list downloaded from s3 bucket
    """
    def load_data(self, file_path_list):
        returnVal = True
        self.errors = []
        intention = self.batch.get(BATCH_INTENTION, INTENTION_NEW)
        file_types = None if intention == INTENTION_DELETE else [k for (k,v) in self.file_nodes.items()]
        deleted_ids = [] if intention == INTENTION_DELETE else None
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
                    if intention == INTENTION_DELETE:
                        deleted_ids.append({"nodeID": self.get_node_id(type, row), "nodeType": type})
                        continue
                    # 2. construct dataRecord
                    rawData = df.loc[index].to_dict()
                    del rawData['index'] #remove index column
                    relation_fields = [name for name in col_names if '.' in name]
                    prop_names = [name for name in col_names if not name in [TYPE, 'index'] + relation_fields]
                    node_id = self.get_node_id(type, row)
                    exist_node = None if intention == INTENTION_NEW else self.mongo_dao.get_dataRecord_nodeId(node_id)
                    batchIds = [self.batch[ID]] if intention == INTENTION_NEW or not exist_node else  exist_node[BATCH_IDS] + [self.batch[ID]]
                    dataRecord = {
                        ID: self.get_record_id(intention, exist_node),
                        SUBMISSION_ID: self.batch[SUBMISSION_ID],
                        BATCH_IDS: batchIds,
                        "latestBatchID": self.batch[ID],
                        "uploadedDate": current_datetime_str(), 
                        FILE_STATUS: STATUS_NEW,
                        ERRORS: [] if intention == INTENTION_NEW or not exist_node else exist_node[ERRORS],
                        WARNINGS: [] if intention == INTENTION_NEW or not exist_node else exist_node[WARNINGS],
                        BATCH_CREATED: current_datetime_str(), 
                        UPDATED_AT: current_datetime_str(), 
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
            returnVal = returnVal and self.mongo_dao.delete_data_records(deleted_ids)             
        return returnVal, self.errors
    
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
        return {
            FILE_NAME: file_name,
            SIZE: file_size,
            MD5: file_md5,
            FILE_STATUS: STATUS_NEW,
            ERRORS: [],
            WARNINGS: [],
            BATCH_CREATED: current_datetime_str(), 
            UPDATED_AT: current_datetime_str(), 
        }