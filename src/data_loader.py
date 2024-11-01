#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from bento.common.utils import get_logger
from common.utils import get_uuid_str, current_datetime, removeTailingEmptyColumnsAndRows
from common.constants import TYPE, ID, SUBMISSION_ID, STATUS, STATUS_NEW, NODE_ID, \
    ERRORS, WARNINGS, CREATED_AT, UPDATED_AT, S3_FILE_INFO, FILE_NAME, \
    MD5, SIZE, PARENT_TYPE, DATA_COMMON_NAME, \
    FILE_NAME_FIELD, FILE_SIZE_FIELD, FILE_MD5_FIELD, NODE_TYPE, PARENTS, CRDC_ID, PROPERTIES, \
    ORIN_FILE_NAME, ADDITION_ERRORS, RAW_DATA, DCF_PREFIX, ID_FIELD, ORCID, ENTITY_TYPE, STUDY_ID

SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
BATCH_IDS = "batchIDs"
PRINCIPAL_INVESTIGATOR = "principal_investigator"


# This script load matadata files to database
# input: file info list
class DataLoader:
    def __init__(self, model, batch, mongo_dao, bucket, root_path, data_common, submission):
        self.log = get_logger('Matedata loader')
        self.model = model
        self.mongo_dao =mongo_dao
        self.batch = batch
        self.bucket = bucket
        self.root_path = root_path
        self.data_common = data_common
        self.file_nodes = self.model.get_file_nodes()
        self.main_nodes = self.model.get_main_nodes()
        self.errors = None
        self.submission = submission
        self.ORCID = submission.get(ORCID)

    """
    param: file_path_list downloaded from s3 bucket
    """
    def load_data(self, file_path_list):
        returnVal = True
        self.errors = []
        file_types = [k for (k,v) in self.file_nodes.items()]
        main_node_types = [k for (k,v) in self.main_nodes.items()]

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
                    crdc_id = None
                    exist_node = self.mongo_dao.get_dataRecord_by_node(node_id, type, self.batch[SUBMISSION_ID])
                    # 2. construct dataRecord
                    rawData = df.loc[index].to_dict()
                    del rawData['index'] #remove index column
                    relation_fields = [name for name in col_names if '.' in name]
                    prop_names = [name for name in col_names if not name in [TYPE, 'index'] + relation_fields]
                    batchIds = [self.batch[ID]] if not exist_node else  exist_node[BATCH_IDS] + [self.batch[ID]]
                    current_date_time = current_datetime()
                    id = self.get_record_id(exist_node)
                    # onlu generating CRDC ID for valid nodes
                    valid_crdc_id_nodes = type in main_node_types
                    # principal investigator node
                    if type == PRINCIPAL_INVESTIGATOR and PRINCIPAL_INVESTIGATOR in main_node_types:
                        crdc_id = self.ORCID
                    else:
                        crdc_id = self.get_crdc_id(exist_node, type, node_id, self.submission.get(STUDY_ID)) if valid_crdc_id_nodes else None
                    # file nodes
                    if valid_crdc_id_nodes and type in file_types:
                        id_field = self.file_nodes.get(type, {}).get(ID_FIELD)
                        file_id_val = row.get(id_field)
                        if file_id_val:
                            crdc_id = file_id_val if file_id_val.startswith(DCF_PREFIX) else DCF_PREFIX + file_id_val

                    if index == 0 or not self.process_m2m_rel(records, node_id, rawData, relation_fields):
                        dataRecord = {
                            ID: id,
                            SUBMISSION_ID: self.batch[SUBMISSION_ID],
                            DATA_COMMON_NAME: self.data_common,
                            BATCH_IDS: batchIds,
                            "latestBatchID": self.batch[ID],
                            "latestBatchDisplayID": self.batch.get("displayID"),
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
                            "IDPropName": self.model.get_node_id(type),
                            PROPERTIES: {k: v for (k, v) in rawData.items() if k in prop_names},
                            PARENTS: self.get_parents(relation_fields, row),
                            RAW_DATA:  rawData,
                            ADDITION_ERRORS: [],
                            ENTITY_TYPE: self.model.get_entity_type(type), 
                            STUDY_ID: self.submission.get(STUDY_ID)
                        }
                        if crdc_id:
                            dataRecord[CRDC_ID] = crdc_id
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
    
    """
    get node id 
    """
    def get_record_id(self, node):
        return node[ID] if node else get_uuid_str()

    """
    get node crdc id
    """
    def get_crdc_id(self, exist_node, node_type, node_id, studyID):
        crdc_id = None
        if not exist_node: 
            # find existing crdc_id by datacommon, nodeType and nodeID
            if self.data_common and node_type and node_id:
                result = self.mongo_dao.search_node(self.data_common, node_type, node_id)
                crdc_id = result.get(CRDC_ID) if result else None
            if not crdc_id:
                entity_type = self.model.get_entity_type(node_type)
                if studyID and node_id and entity_type:
                    result = self.mongo_dao.search_node_by_study(studyID, entity_type, node_id)
                    crdc_id = result.get(CRDC_ID) if result else None
        else:
            crdc_id = exist_node.get(CRDC_ID)
        return crdc_id
        
    
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