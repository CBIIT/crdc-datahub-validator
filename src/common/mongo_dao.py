from pymongo import MongoClient, errors, ReplaceOne, UpdateOne, DeleteOne, DESCENDING
from bento.common.utils import get_logger
from common.constants import BATCH_COLLECTION, SUBMISSION_COLLECTION, DATA_COLlECTION, ID, UPDATED_AT, \
    SUBMISSION_ID, NODE_ID, NODE_TYPE, S3_FILE_INFO, STATUS, FILE_ERRORS, STATUS_NEW, \
    PARENT_TYPE, PARENT_ID_VAL, PARENTS, FILE_VALIDATION_STATUS, METADATA_VALIDATION_STATUS, TYPE, \
    FILE_MD5_COLLECTION, FILE_NAME, CRDC_ID, RELEASE_COLLECTION, FAILED, DATA_COMMON_NAME, KEY, \
    VALUE_PROP, ERRORS, WARNINGS, VALIDATED_AT, STATUS_ERROR, STATUS_WARNING, PARENT_ID_NAME, \
    SUBMISSION_REL_STATUS, SUBMISSION_REL_STATUS_DELETED, STUDY_ABBREVIATION, SUBMISSION_STATUS, STUDY_ID, \
    CROSS_SUBMISSION_VALIDATION_STATUS, ADDITION_ERRORS, VALIDATION_COLLECTION, VALIDATION_ENDED, CONFIG_COLLECTION, \
    BATCH_BUCKET, CDE_COLLECTION, CDE_CODE, CDE_VERSION, ENTITY_TYPE, QC_COLLECTION, QC_RESULT_ID
from common.utils import get_exception_msg, current_datetime

MAX_SIZE = 10000

class MongoDao:
    def __init__(self, connectionStr, db_name):
      self.log = get_logger("Mongo DAO")
      self.client = MongoClient(connectionStr)
      self.db_name = db_name
    """
    get batch by id
    """
    def get_batch(self, batchId):
        db = self.client[self.db_name]
        batch_collection = db[BATCH_COLLECTION]
        try:
            return batch_collection.find_one({ID: batchId})
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find batch, {batchId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find batch, {batchId}: {get_exception_msg()}")
            return None
        
    """
    find batch for uploaded data file
    """
    def find_batch_by_file_name(self, submissionID, batch_type, file_name):
        db = self.client[self.db_name]
        batch_collection = db[BATCH_COLLECTION]
        query = {
            SUBMISSION_ID: submissionID, 
            TYPE: batch_type, 
            "files.fileName": file_name,
            STATUS: "Uploaded"
        }
        try:
            results = list(batch_collection.find(query).sort("displayID", DESCENDING).limit(1))
            return results[0] if results and len(results) > 0 else None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find batch by data file name, {submissionID}/{batch_type}/{file_name}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find batch by data file name, {submissionID}/{batch_type}/{file_name}: {get_exception_msg()}")
            return None
    """
    get submission by id
    """   
    def get_submission(self, submissionId):
        db = self.client[self.db_name]
        submission_collection = db[SUBMISSION_COLLECTION]
        try:
            return submission_collection.find_one({ID: submissionId})
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find submission, {submissionId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find submission, {submissionId}: {get_exception_msg()}")
            return None

    """
    check node exists by node name and its value
    """
    def search_nodes_by_type_and_value(self, nodes):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        node_set, query = set(), []
        for node in nodes:
            node_type, node_key, node_value = node.get(TYPE), node.get(KEY), node.get(VALUE_PROP)
            if node_type and node_key and node_value is not None \
                    and (node_type, node_key, node_value) not in node_set:
                node_set.add(tuple([node_type, node_key, node_value]))
                query.append({"nodeType": node_type, "props." + node_key: node_value})
        try:
            return list(data_collection.find({"$or": query})) if len(query) > 0 else []
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to search nodes: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to search nodes: {get_exception_msg()}")
            return None
        
    """
    check node exists by node name and its value
    """
    def search_nodes_by_index(self, nodes, submission_id):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        query = []
        for node in nodes:
            node_type, node_key, node_value = node.get(TYPE), node.get(KEY), node.get(VALUE_PROP)
            if node_type and node_key and node_value is not None: 
                query.append({SUBMISSION_ID: submission_id, NODE_TYPE: node_type, NODE_ID: node_value})
        try:
            return list(data_collection.find({"$or": query})) if len(query) > 0 else []
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to search nodes: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to search nodes: {get_exception_msg()}")
            return None
        
    """
    check node exists by dataCommons, nodeType and nodeID
    """
    def search_node_by_index_crdc(self, data_commons, node_type, node_id, excluded_submission_ids):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        try:
            
            result = data_collection.find_one({DATA_COMMON_NAME: data_commons, NODE_TYPE: node_type, NODE_ID: node_id, SUBMISSION_ID: {"$nin": excluded_submission_ids}}) 
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to search node for crdc_id: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to search node for crdc_id {get_exception_msg()}")
            return None

    """
    check node exists by dataCommons, nodeType and nodeID
    """
        
    """
    get file in dataRecord collection by fileId
    """ 
    def get_file(self, fileId):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            return file_collection.find_one({ID: fileId})
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find data file, {fileId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find data file, {fileId}: {get_exception_msg()}")
            return None
    """
    get file in dataRecord collection by fileName
    """   
    def get_file_by_name(self, fileName):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            return file_collection.find_one({"S3FileInfo.fileName": fileName})
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find data file, {fileName}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find data file, {fileName}: {get_exception_msg()}")
            return None    
    """
    get file records in dataRecords collection by submissionID
    """
    def get_files_by_submission(self, submission_id):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            return list(file_collection.find({SUBMISSION_ID: submission_id, S3_FILE_INFO: {"$nin": [None, ""]}}))
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find data file for the submission, {submission_id}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find data file for the submission, {submission_id}: {get_exception_msg()}")
            return None
    
    def update_batch(self, batch):
        db = self.client[self.db_name]
        batch_collection = db[BATCH_COLLECTION]
        #update the batch 
        batch[UPDATED_AT] = current_datetime()
        # Using update_one() method for single updating.
        try:
            result = batch_collection.replace_one({ID : batch[ID]}, batch, False) 
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to update batch, {batch[ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to update batch, {batch[ID]}: {get_exception_msg()}")
            return False
    """
    check if not duplications exist in dataRecords collection
    """    
    def check_metadata_ids(self, nodeType, ids, submission_id):
        #1. check if collection exist
        db = self.client[self.db_name]
        collection = db[DATA_COLlECTION]
        try:
            #2 check if keys existing in the collection
            result = list(collection.find({NODE_ID: {'$in': ids}, SUBMISSION_ID: submission_id, NODE_TYPE: nodeType}))
            return result 
        except errors.OperationFailure as oe: 
            self.log.exception(oe)
            self.log.exception(f"{submission_id}: Failed to query DB, {nodeType}: {get_exception_msg()}!")
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to query DB, {nodeType}: {get_exception_msg()}!")
        return True
    
    """
    update a file record in dataRecords collection
    """
    def update_file (self, file_record):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.replace_one({ID : file_record[ID]}, file_record, False)
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to update data file, {file_record[ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to update data file, {file_record[ID]}: {get_exception_msg()}")
            return False  
        
    """
    update a s3 file info in dataRecords collection
    """
    def update_file_info(self, file_record):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.update_one({ID : file_record[ID]}, {"$set": {S3_FILE_INFO: file_record[S3_FILE_INFO]}})
            return result.modified_count > 0 
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to update data file, {file_record[ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to update data file, {file_record[ID]}: {get_exception_msg()}")
            return False  
    """
    update errors in submissions collection
    """   
    def set_submission_validation_status(self, submission, file_status, metadata_status, cross_submission_status, fileErrors, is_delete = False):
        updated_submission = {UPDATED_AT: current_datetime()}
        db = self.client[self.db_name]
        file_collection = db[SUBMISSION_COLLECTION]
        overall_metadata_status = None
        try:
            if file_status:
                updated_submission[FILE_VALIDATION_STATUS] = file_status if file_status != "None" else None
                if fileErrors and len(fileErrors) > 0:
                    updated_submission[FILE_ERRORS] = fileErrors
                else:
                    updated_submission[FILE_ERRORS] = []
                updated_submission[VALIDATION_ENDED] = submission[VALIDATION_ENDED]
            if metadata_status:
                if not ((is_delete and self.count_docs(DATA_COLlECTION, {SUBMISSION_ID: submission[ID]}) == 0)):
                    if metadata_status == STATUS_ERROR or metadata_status == STATUS_NEW: 
                        overall_metadata_status = metadata_status
                    else:
                        error_nodes = self.count_docs(DATA_COLlECTION, {SUBMISSION_ID: submission[ID], STATUS: STATUS_ERROR})
                        if error_nodes > 0:
                            overall_metadata_status = STATUS_ERROR
                        else:
                            warning_nodes = self.count_docs(DATA_COLlECTION, {SUBMISSION_ID: submission[ID], STATUS: STATUS_WARNING})
                            if warning_nodes > 0: 
                                overall_metadata_status = STATUS_WARNING
                            else:
                                overall_metadata_status = metadata_status
                # check if all file nodes are deleted
                if is_delete and (self.count_docs(DATA_COLlECTION, {SUBMISSION_ID: submission[ID], S3_FILE_INFO: {"$exists": True}}) == 0):
                    updated_submission[FILE_VALIDATION_STATUS] = None
                if is_delete:
                    updated_submission["deletingData"] = False
                updated_submission[METADATA_VALIDATION_STATUS] = overall_metadata_status
                updated_submission[VALIDATION_ENDED] = submission.get(VALIDATION_ENDED)
                
            if cross_submission_status:
                updated_submission[CROSS_SUBMISSION_VALIDATION_STATUS] = cross_submission_status
            result = file_collection.update_one({ID : submission[ID]}, {"$set": updated_submission}, False)
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to update submission, {submission[ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to update submission, {submission[ID]}: {get_exception_msg()}")
            return False
        
    """
    update data records based on node ID in dataRecords
    """
    def update_data_records(self, data_records):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                ReplaceOne( {ID: m[ID]}, remove_id(m),  upsert=True)
                    for m in list(data_records)
                ])
            self.log.info(f'Total {result.upserted_count} dataRecords are upserted!')
            return True, None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            msg = f"Failed to update metadata."
            self.log.exception(msg)
            return False, msg
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to update metadata, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg 
        
    """
    update record's status, errors and warnings based on node ID in dataRecords
    """
    def update_data_records_status(self, data_records):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                UpdateOne( {ID: m[ID]}, 
                    {"$set": {STATUS: m[STATUS], UPDATED_AT: m[UPDATED_AT], VALIDATED_AT: m[UPDATED_AT], QC_RESULT_ID: m.get(QC_RESULT_ID)}})
                    for m in list(data_records)
                ])
            self.log.info(f'Total {result.modified_count} dataRecords are updated!')
            return True, None
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            msg = f"Failed to update metadata."
            self.log.exception(msg)
            return False, msg
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to update metadata, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg 
    """
    update record's status, errors by additional error
    """
    def update_data_records_addition_error(self, data_records):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                UpdateOne( {ID: m[ID]}, 
                    {"$set": {UPDATED_AT: m[UPDATED_AT], VALIDATED_AT: m[UPDATED_AT], ADDITION_ERRORS: m.get(ADDITION_ERRORS, [])}})
                    for m in list(data_records)
                ])
            self.log.info(f'Total {result.modified_count} dataRecords are updated!')
            return True, None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            msg = f"Failed to update metadata."
            self.log.exception(msg)
            return False, msg
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to update metadata, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg 
    """
    delete dataRecords by nodeIDs
    """  
    def delete_data_records(self, nodes):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                DeleteOne( { SUBMISSION_ID: m[SUBMISSION_ID], NODE_ID: m[NODE_ID], NODE_TYPE: m[NODE_TYPE] })
                    for m in list(nodes)
                ])
            self.log.info(f'Total {result.deleted_count} dataRecords are deleted!')
            # delete related qcResults
            qc_ids = [node[QC_RESULT_ID] for node in nodes if node.get(QC_RESULT_ID)]
            qc_ids.extend([node[S3_FILE_INFO][QC_RESULT_ID]for node in nodes if node.get(S3_FILE_INFO) and node[S3_FILE_INFO].get(QC_RESULT_ID)])
            if qc_ids and len(qc_ids) > 0:
                self.delete_qcRecords(qc_ids)
            return True, None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            msg = f"Failed to delete metadata, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to delete metadata, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg
    """
    insert batch dataRecords
    """ 
    def insert_data_records (self, file_records):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.insert_many(file_records)
            count = len(result.inserted_ids)
            self.log.info(f'Total {count} dataRecords are inserted!')
            return count > 0, None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            msg = f"Failed to insert data records, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to insert data records, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg
    """
    retrieve dataRecords by submissionID and scope either New dataRecords or All
    """
    def get_dataRecords(self, submission_id, scope):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            query = {'submissionID': {'$eq': submission_id}} 
            if scope == STATUS_NEW:
                query[STATUS] = STATUS_NEW
            result = list(file_collection.find(query))
            count = len(result)
            self.log.info(f'Total {count} dataRecords are found for the submission, {submission_id} and scope of {scope}!')
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve data records, {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve data records, {get_exception_msg()}")
            return None 

    """
    retrieve dataRecord by submissionID and scope either New dataRecords or All in batch
    """
    def get_dataRecords_chunk(self, submission_id, scope, start, size):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            query = {SUBMISSION_ID: {'$eq': submission_id}} 
            if scope == STATUS_NEW:
                query[STATUS] = STATUS_NEW
                result = list(file_collection.find(query).sort({SUBMISSION_ID: 1, "nodeType": 1, "nodeID": 1}).limit(size))
            else:
                result = list(file_collection.find(query).sort({SUBMISSION_ID: 1, "nodeType": 1, "nodeID": 1}).skip(start).limit(size))
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve data records, {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve data records, {get_exception_msg()}")
            return None 
        
    """
    retrieve dataRecord by submissionID and nodeType
    """
    def get_dataRecords_chunk_by_nodeType(self, submission_id, node_type, start, size):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            query = {SUBMISSION_ID: {'$eq': submission_id}, NODE_TYPE: {'$eq': node_type}} 
            result = list(file_collection.find(query).sort({SUBMISSION_ID: 1, "nodeType": 1, "nodeID": 1}).skip(start).limit(size))
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve data records, {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve data records, {get_exception_msg()}")
            return None 

    """
    retrieve dataRecord by nodeID
    """
    def get_dataRecord_by_node(self, nodeID, nodeType, submission_id):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.find_one({SUBMISSION_ID: submission_id, NODE_ID: nodeID, NODE_TYPE: nodeType})
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve data record, {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve data record, {get_exception_msg()}")
            return None   
    """
    find child node by type and id
    """
    def get_nodes_by_parents(self, parent_ids, submission_id):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        query = []
        for id in parent_ids:
            node_type, node_id = id.get(NODE_TYPE), id.get(NODE_ID)
            query.append({SUBMISSION_ID: submission_id, PARENTS: {"$elemMatch": {PARENT_TYPE: node_type, PARENT_ID_VAL: node_id}}})
        try:
            results = list(data_collection.find({"$or": query})) if len(query) > 0 else []
            return True, results
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve child nodes: {get_exception_msg()}")
            return False, None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve child nodes: {get_exception_msg()}")
            return False, None
    
    """
    find child nodes by nodeType, parentType and parentIDProperty and parentID
    """
    def get_nodes_by_parent_prop(self, node_type, parent_prop, submission_id):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        query = {SUBMISSION_ID: submission_id, NODE_TYPE: node_type, PARENTS: {"$elemMatch": {PARENT_TYPE: parent_prop[PARENT_TYPE], 
                        PARENT_ID_NAME: parent_prop[PARENT_ID_NAME], PARENT_ID_VAL: parent_prop[PARENT_ID_VAL]}}}
        try:
            return list(data_collection.find(query))
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve child nodes: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve child nodes: {get_exception_msg()}")
            return None
        
    """
    find node in other submission with the same study
    """   
    def find_node_in_other_submissions_in_status(self, submission_id, study, data_common, node_type, nodeId, status_list):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        try:
            submissions = None
            other_submissions = self.find_submissions({STUDY_ABBREVIATION: study, SUBMISSION_STATUS: {"$in": status_list}, ID: {"$ne": submission_id}})
            if len(other_submissions) > 0:
                other_submission_ids = [item[ID] for item in other_submissions]
                duplicate_nodes = list(data_collection.find({DATA_COMMON_NAME: data_common, NODE_TYPE: node_type, NODE_ID: nodeId, SUBMISSION_ID: {"$in": other_submission_ids}}))
                if len(duplicate_nodes) > 0:
                    other_submission_ids = [item[SUBMISSION_ID] for item in duplicate_nodes]
                    submissions = [item for item in other_submissions if item[ID] in other_submission_ids]
            return True, submissions
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve child nodes: {get_exception_msg()}")
            return False, None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve child nodes: {get_exception_msg()}")
            return False, None
    """
    find submission by query
    """
    def find_submissions(self, query):
        db = self.client[self.db_name]
        data_collection = db[SUBMISSION_COLLECTION]
        try:
            return list(data_collection.find(query))
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to retrieve submissions: {get_exception_msg()}")
            return False, None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to retrieve submissions:: {get_exception_msg()}")
            return False, None  
    """
    set dataRecords search index, 'submissionID_nodeType_nodeID'
    """
    def set_search_index_dataRecords(self, submission_index, crdc_index, study_entity_type_index):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        try:
            index_dict = data_collection.index_information()
            if not index_dict.get(submission_index):
                result = data_collection.create_index([(SUBMISSION_ID), (NODE_TYPE),(NODE_ID)], \
                            name=submission_index)
            if not index_dict.get(crdc_index):
                result = data_collection.create_index([(DATA_COMMON_NAME), (NODE_TYPE),(NODE_ID)], \
                            name=crdc_index)
            if not index_dict.get(study_entity_type_index):
                result = data_collection.create_index([(STUDY_ID), (ENTITY_TYPE),(NODE_ID)], \
                            name=study_entity_type_index)
            return True
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to set search index: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to set search index: {get_exception_msg()}")
            return False
    
    """
    set release search index, 'dataCommons_nodeType_nodeID'
    """
    def set_search_release_index(self, dataCommon_index, crdcID_index):
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            index_dict = data_collection.index_information()
            if not index_dict or not index_dict.get(dataCommon_index):
                result = data_collection.create_index([(DATA_COMMON_NAME), (NODE_TYPE),(NODE_ID)], \
                            name=dataCommon_index)
            if not index_dict or not index_dict.get(crdcID_index):
                result = data_collection.create_index([(CRDC_ID)], \
                            name=crdcID_index)
            return True
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to set search index in release collection: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to set search index in release collection: {get_exception_msg()}")
            return False
        
    """
    find cached file md5 by submissionID and fileName
    """
    def get_file_md5(self, submission_id, file_name):
        db = self.client[self.db_name]
        data_collection = db[FILE_MD5_COLLECTION]
        try:
            md5_info = data_collection.find_one({SUBMISSION_ID: submission_id, FILE_NAME: file_name})
            return md5_info
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve data file md5: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve data file md5: {get_exception_msg()}")
            return None
        
    """
    save file md5 info to fileMD5 collection
    """
    def save_file_md5(self, md5_info):
        db = self.client[self.db_name]
        data_collection = db[FILE_MD5_COLLECTION]
        try:
            result = data_collection.replace_one({ID: md5_info[ID]}, md5_info,  upsert=True)
            return (result and result.upserted_id)
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{md5_info[SUBMISSION_ID]}: Failed to save data file md5: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{md5_info[SUBMISSION_ID]}: Failed to save data file md5: {get_exception_msg()}")
            return False
        
    """
    get release by CRDC_ID
    """
    def get_release(self, crdc_id):
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            result = data_collection.find_one({CRDC_ID: crdc_id})
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find release record for {crdc_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find release record for {crdc_id}: {get_exception_msg()}")
            return False
        
    """
    get release by dataCommon, nodeType and nodeId
    """
    def search_release(self, dataCommon, node_type, node_id):
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            result = data_collection.find_one({DATA_COMMON_NAME: dataCommon, NODE_TYPE: node_type, NODE_ID: node_id})
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find release record for {dataCommon}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find release record for {dataCommon}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
    
    
    """
    insert release 
    """
    def insert_release(self, release):
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            result = data_collection.insert_one(release)
            return (result and result.inserted_id)
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to insert crdcID record: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to insert crdcID record: {get_exception_msg()}")
            return False
    """
    update release 
    """
    def update_release(self, release):
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            result = data_collection.replace_one({ID: release[ID]}, release)
            return True
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to update release record: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to update release record: {get_exception_msg()}")
            return False

    def search_node(self, data_commons, node_type, node_id):
        """
        Search release collection for given node, if not found, search it in dataRecord collection
        :param data_commons:
        :param node_type:
        :param node_id:
        :return:
        """
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        rtn_val = None
        try:
            
            results = list(data_collection.find({DATA_COMMON_NAME: data_commons, NODE_TYPE: node_type, NODE_ID: node_id}))
            released_nodes = [node for node in results if node.get(SUBMISSION_REL_STATUS) != SUBMISSION_REL_STATUS_DELETED ]
            if len(released_nodes) == 0:
                # search dataRecords
                deleted_submission_ids = [rel[SUBMISSION_ID] for rel in results if rel.get(SUBMISSION_REL_STATUS) == SUBMISSION_REL_STATUS_DELETED ]
                rtn_val = self.search_node_by_index_crdc(data_commons, node_type, node_id, deleted_submission_ids)
            else:
                rtn_val = released_nodes[0]
            return rtn_val
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find release record for {data_commons}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find release record for {data_commons}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
        
    def search_node_by_study(self, studyID, entity_type, node_id):
        """
        Search release collection for given node, if not found, search it in dataRecord collection
        :param studyID:
        :param node_type:
        :param node_id:
        :return:
        """
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        try:
            return data_collection.find_one({STUDY_ID: studyID, ENTITY_TYPE: entity_type, NODE_ID: node_id})
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to search node for study: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to search node for study {get_exception_msg()}")
            return None

    def search_released_node(self, data_commons, node_type, node_id):
        """
        Search release collection for given node
        :param data_commons:
        :param node_type:
        :param node_id:
        :return:
        """
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            result = data_collection.find_one({DATA_COMMON_NAME: data_commons, NODE_TYPE: node_type, NODE_ID: node_id})
            return result
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find release record for {data_commons}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find release record for {data_commons}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
   
    def search_released_node_with_status(self, data_commons, node_type, node_id, status):
        """
        Search release collection for given node with status
        :param data_commons:
        :param node_type:
        :param node_id:
        :return:
        """
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            result = data_collection.find_one({DATA_COMMON_NAME: data_commons, NODE_TYPE: node_type, NODE_ID: node_id, SUBMISSION_REL_STATUS: {"$in": status}})
            return list(result) if result else None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find release record for {data_commons}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find release record for {data_commons}/{node_type}/{node_id}: {get_exception_msg()}")
            return False
    
    """
    find child node by type and id
    """
    def get_released_nodes_by_parent_with_status(self, datacommon, parent, status, submission_id):
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        query = []
        node_type, node_id = parent.get(NODE_TYPE), parent.get(NODE_ID)
        query.append({DATA_COMMON_NAME: datacommon, PARENTS: {"$elemMatch": {PARENT_TYPE: node_type, PARENT_ID_VAL: node_id}}, SUBMISSION_REL_STATUS : {"$in": status}})
        try:
            results = list(data_collection.find({"$or": query})) if len(query) > 0 else []
            return True, results
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"{submission_id}: Failed to retrieve child releases: {get_exception_msg()}")
            return False, None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"{submission_id}: Failed to retrieve child releases: {get_exception_msg()}")
            return False, None
    
    def find_released_nodes_by_parent(self, node_type, data_commons, parent_node):
        """
        find released certain type children nodes by parent
        :param node_type:
        :param data_commons:
        :param parent_node:
        :return:
        """
        db = self.client[self.db_name]
        data_collection = db[RELEASE_COLLECTION]
        try:
            return list(data_collection.find({DATA_COMMON_NAME: data_commons, NODE_TYPE: node_type, PARENTS: {"$elemMatch": {PARENT_TYPE: parent_node[PARENT_TYPE], 
                        PARENT_ID_NAME: parent_node[PARENT_ID_NAME], PARENT_ID_VAL: parent_node[PARENT_ID_VAL]}}}))

        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to find release record for {data_commons}/{parent_node[PARENT_TYPE]}/{parent_node[PARENT_ID_VAL]}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to find release record for {data_commons}/{parent_node[PARENT_TYPE]}/{parent_node[PARENT_ID_VAL]}: {get_exception_msg()}")
            return None
        
    """
    count documents in a given collection and conditions 
    """  
    def count_docs(self, collection, query):
        db = self.client[self.db_name]
        data_collection = db[collection]
        try:
            return data_collection.count_documents(query)
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to count documents for collection, {collection} at conditions {query}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to count documents for collection, {collection} at conditions {query}")
            return False
    """
    update validation status
    """   
    def update_validation_status(self, validation_id, status, validation_end_at):
        db = self.client[self.db_name]
        data_collection = db[VALIDATION_COLLECTION]
        try:
            result = data_collection.update_one({ID: validation_id}, {"$set": {STATUS: status, "ended": validation_end_at}})
            return True if result.modified_count > 0 else False
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to update validation status for {validation_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to update validation status for {validation_id}: {get_exception_msg()}")
            return False
    """
    get bucket name based on dataCommons and type
    """   
    def get_bucket_name(self, type, dataCommon):
        db = self.client[self.db_name]
        data_collection = db[CONFIG_COLLECTION]
        try:
            result = data_collection.find_one({TYPE: type, DATA_COMMON_NAME: dataCommon})
            if result:
                return result.get(BATCH_BUCKET)
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to update validation status for {validation_id}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to update validation status for {validation_id}: {get_exception_msg()}")
            return None 

    def insert_cde(self, cde_list):
        db = self.client[self.db_name]
        data_collection = db[CDE_COLLECTION]
        try:
            result = data_collection.bulk_write([
                ReplaceOne( {ID: m[ID]}, remove_id(m),  upsert=True)
                    for m in list(cde_list)
                ])
            self.log.info(f'Total {result.upserted_count} CDE PV are upserted!')
            return True, None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            msg = f"Failed to upsert CDE PV ."
            self.log.exception(msg)
            return False, msg
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to upsert mCDE PV, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg 
    """
    set CDE search index, 'CDECode_1_CDEVersion_1'
    """
    def set_search_cde_index(self, cde_search_index):
        db = self.client[self.db_name]
        data_collection = db[CDE_COLLECTION]
        try:
            index_dict = data_collection.index_information()
            if not index_dict or not index_dict.get(cde_search_index):
                result = data_collection.create_index([(CDE_CODE), (CDE_VERSION)], \
                            name=cde_search_index)
            return True
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to set search index in CDE collection: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to set search index in CDE collection: {get_exception_msg()}")
            return False
    """
    get CDE permissible values
    """    
    def get_cde_permissible_values(self, cde_code, cde_version):
        db = self.client[self.db_name]
        data_collection = db[CDE_COLLECTION]
        query = {CDE_CODE: cde_code}
        if cde_version:
            query[CDE_VERSION] = cde_version
        try:
            return data_collection.find_one(query, sort=[( CDE_VERSION, DESCENDING )])  #find latest version 
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to get permissible values for {cde_code}/{cde_version}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to get permissible values for {cde_code}/{cde_version}: {get_exception_msg()}")
            return None
    """
    get qc record by qc_id
    :param qc_id:
    """
    def get_qcRecord(self, qc_id):
        db = self.client[self.db_name]
        data_collection = db[QC_COLLECTION]
        try:
            return data_collection.find_one({ID: qc_id})
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to get qc record for {qc_id}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to get qc record for {qc_id}: {get_exception_msg()}")
            return None

    """
    delete qc record by qc_id
    :param qc_id:
    """   
    def delete_qcRecord(self, qc_id):
        db = self.client[self.db_name]
        data_collection = db[QC_COLLECTION]
        try:
            result = data_collection.delete_one({ID: qc_id})
            return True if result.deleted_count > 0 else False
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to delete qc record for {qc_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to delete qc record for {qc_id}: {get_exception_msg()}")
            return False
    """
    delete qc records by qc_id list
    :param qc_id:
    """   
    def delete_qcRecords(self, qc_ids):
        db = self.client[self.db_name]
        data_collection = db[QC_COLLECTION]
        try:
            result = data_collection.delete_many({ID: {"$in": qc_ids}})
            return True if result.deleted_count > 0 else False
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            self.log.exception(f"Failed to delete qc record for {qc_id}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to delete qc record for {qc_id}: {get_exception_msg()}")
            return False

    """
    save  rt qc records
    :param qc_list:
    """   
    def save_qc_results(self, qc_list):
        db = self.client[self.db_name]
        data_collection = db[QC_COLLECTION]
        try:
            result = data_collection.bulk_write([
                ReplaceOne({ID: m[ID]}, remove_id(m), upsert=True)
                    for m in list(qc_list)
                ])
            self.log.info(f'Total {result.upserted_count} QC records are upserted!')
            return True, None
        except errors.PyMongoError as pe:
            self.log.exception(pe)
            msg = f"Failed to upsert QC records ."
            self.log.exception(msg)
            return False, msg
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to upsert QC records, {get_exception_msg()}"
            self.log.exception(msg)
            return False, msg
    
"""
remove _id from records for update
"""   
def remove_id (data_record):
    data = {}
    for k in data_record.keys():
        if k == ID:
            continue
        data[k] = data_record[k]
    return data