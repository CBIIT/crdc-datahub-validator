#!/usr/bin/env python3
import os
import pandas as pd
from bento.common.utils import get_logger
from common.utils import get_uuid_str, current_datetime_str, get_exception_msg
from common.constants import MODEL, IDS, TYPE, ID, SUBMISSION_ID, BATCH_ID, FILE_STATUS, STATUS_NEW, \
    ERRORS, WARNINGS, BATCH_CREATED, UPDATED_AT, BATCH_INTENTION, S3_FILE_INFO, FILE_NAME, FILE_SIZE, \
    MD5, DB, DATA_COMMON, NODE_LABEL
SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
BATCH_IDS = "batchIDs"

# This script load matadata files to database
# input: file info list
class DataLoader:
    def __init__(self, configs, model, batch, mongo_dao):
        self.log = get_logger('Matedata loader')
        self.configs = configs
        self.model = model
        self.mongo_dao =mongo_dao
        self.batch = batch
        self.records = None
        self.file_nodes = model[MODEL].get("file-nodes", {})

    """
    param: file_path_list downloaded from s3 bucket
    """
    def load_data(self, file_path_list):
        returnVal = True
        data_common = self.model[MODEL][DATA_COMMON]
        intention = self.batch.get(BATCH_INTENTION, STATUS_NEW)
        file_types = None if intention == "delete" else [k for (k,v) in self.file_nodes.items()]
        deleted_ids = [] if intention == "delete" else None
        failed_at = 1
        
        for file in file_path_list:
            self.records = [] if intention != "delete" else None
            # 1. read file to dataframe
            if not os.path.isfile(file):
                continue
            try:
                df = pd.read_csv(file, sep=SEPARATOR_CHAR, header=0, encoding=UTF8_ENCODE)
                df = df.reset_index()  # make sure indexes pair with number of rows
                col_names =list(df.columns)
                
                for index, row in df.iterrows():
                    type = row[TYPE]
                    if intention == "delete":
                        deleted_ids.append(self.get_node_id(type, row))
                        continue
                    # 2. construct dataRecord
                    rawData = df.loc[index].to_dict()
                    del rawData['index'] #remove index column
                    relation_fields = [name for name in col_names if '.' in name]
                    prop_names = [name for name in col_names if not name in [TYPE, 'index'] + relation_fields]
                    dataRecord = {
                        ID: get_uuid_str(data_common, type),
                        SUBMISSION_ID: self.batch[SUBMISSION_ID],
                        BATCH_IDS: [self.batch[ID]],
                        FILE_STATUS: STATUS_NEW,
                        ERRORS: [],
                        WARNINGS: [],
                        BATCH_CREATED: current_datetime_str(), 
                        UPDATED_AT: current_datetime_str(), 
                        "orginalFileName": os.path.basename(file),
                        "lineNumber": index,
                        "nodeType": type,
                        "nodeID": self.get_node_id(type, row),
                        "props": {k: v for (k, v) in rawData.items() if k in prop_names},
                        "parents": self.get_parents(relation_fields, row),
                        "rawData":  rawData
                    }
                    if type in file_types:
                        dataRecord[S3_FILE_INFO] = self.get_file_info(type, prop_names, row)
                    self.records.append(dataRecord)
                    failed_at += 1
                
            except Exception as e:
                    df = None
                    self.log.debug(e)
                    msg = f"Failed to load data in file, {file} at {failed_at + 1}! {get_exception_msg()}."
                    self.log.exception(msg)
                    self.batch[ERRORS] = self.batch[ERRORS].append(msg) if self.batch[ERRORS] else [msg]
                    return False
            # 3-1. insert data in a tsv file into mongo DB
            if intention != "delete":
                returnVal = returnVal and self.mongo_dao.insert_data_records(self.records, self.configs[DB])

        #3-2. delete all records in deleted_ids
        if intention == "delete":
            returnVal = returnVal and self.mongo_dao.delete_data_records_by_node_ids(deleted_ids, self.configs[DB])

        return returnVal
    
    """.
    get node id defined in model dict
    """
    def get_node_id(self, type, row):
        id_field = next(id["key"] for id in self.model[IDS] if id[NODE_LABEL] == type)
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
        file_name = row[file_fields["name-field"]] if file_fields["name-field"] in prop_names else None
        file_size = row[file_fields["size-field"]] if file_fields["size-field"] in prop_names else None
        file_md5 = row[file_fields["md5-field"]] if file_fields["md5-field"] in prop_names else None
        return {
            FILE_NAME: file_name,
            FILE_SIZE: file_size,
            MD5: file_md5,
            FILE_STATUS: STATUS_NEW,
            ERRORS: [],
            WARNINGS: [],
            BATCH_CREATED: current_datetime_str(), 
            UPDATED_AT: current_datetime_str(), 
        }
    
    def delete_data(self, ids):
        return True