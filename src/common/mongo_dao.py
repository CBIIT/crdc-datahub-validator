from pymongo import MongoClient, errors, ReplaceOne, DeleteOne
from bento.common.utils import get_logger
from common.constants import BATCH_COLLECTION, SUBMISSION_COLLECTION, DATA_COLlECTION, ID, UPDATED_AT, \
    SUBMISSION_ID, NODE_ID, NODE_TYPE, S3_FILE_INFO, STATUS, FILE_ERRORS, STATUS_NEW
from common.utils import get_exception_msg, current_datetime_str

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
            self.log.debug(pe)
            self.log.exception(f"Failed to find batch, {batchId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to find batch, {batchId}: {get_exception_msg()}")
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
            self.log.debug(pe)
            self.log.exception(f"Failed to find submission, {submissionId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to find submission, {submissionId}: {get_exception_msg()}")
            return None

    """
    check node exists by node name and its value
    """
    # def validate_node(self, db_name, submission_id, node_type, node_key, node_value):
    def search_nodes_by_type_and_value(self, nodes):
        db = self.client[self.db_name]
        data_collection = db[DATA_COLlECTION]
        try:
            node_set, query = set(), []
            for node in nodes:
                node_type, node_key, node_value = node.get("type"), node.get("key"), node.get("value")
                if node_type and node_key and node_value is not None \
                        and (node_type, node_key, node_value) not in node_set:
                    node_set.add(tuple([node_type, node_key, node_value]))
                    query.append({"$and": [{"nodeType": node_type, "props." + node_key: node_value}]})
            return list(data_collection.find({"$or": query})) if len(query) > 0 else []
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to search nodes: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to search nodes: {get_exception_msg()}")
            return None
    """
    get file in dataRecord collection by fileId
    """ 
    def get_file(self, fileId):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            return file_collection.find_one({ID: fileId})
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to find file, {fileId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to find file, {fileId}: {get_exception_msg()}")
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
            self.log.debug(pe)
            self.log.exception(f"Failed to find file, {fileName}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to find file, {fileName}: {get_exception_msg()}")
            return None    
    """
    get file records in dataRecords collection by submissionID
    """
    def get_files_by_submission(self, submissionID):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            return list(file_collection.find({SUBMISSION_ID: submissionID, S3_FILE_INFO: {"$nin": [None, ""]}}))
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to find file for the submission, {submissionID}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to find file for the submission, {submissionID}: {get_exception_msg()}")
            return None
    
    def update_batch(self, batch):
        db = self.client[self.db_name]
        batch_collection = db[BATCH_COLLECTION]
        #update the batch 
        batch[UPDATED_AT] = current_datetime_str()
        # Using update_one() method for single updating.
        try:
            result = batch_collection.replace_one({ID : batch[ID]}, batch, False) 
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to update batch, {batch[ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update batch, {batch[ID]}: {get_exception_msg()}")
            return False
    """
    check if not duplications exist in dataRecords collection
    """    
    def check_metadata_ids(self, nodeType, ids, submission_id):
        #1. check if collection exist
        db = self.client[self.db_name]
        try:
            collection = db[DATA_COLlECTION]
            #2 check if keys existing in the collection
            result = collection.find_one({NODE_ID: {'$in': ids}, SUBMISSION_ID: submission_id, NODE_TYPE: nodeType})
            return False if result else True
        except errors.OperationFailure as oe: 
            self.log.debug(oe)
            self.log.exception(f"Failed to query DB, {metadata_db}, {nodeType}: {get_exception_msg()}!")
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to query DB, {metadata_db}, {nodeType}: {get_exception_msg()}!")
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
            self.log.debug(pe)
            self.log.exception(f"Failed to update file, {file_record[ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update file, {file_record[ID]}: {get_exception_msg()}")
            return False  
    """
    update errors in submissions collection
    """   
    def set_submission_error(self, submission, status, msgs, isFile=True):
        db = self.client[self.db_name]
        file_collection = db[SUBMISSION_COLLECTION]
        try:
            if msgs and len(msgs) > 0:
                submission[FILE_ERRORS] =  list(submission[FILE_ERRORS]).extend(msgs) if submission.get(FILE_ERRORS) \
                        and isinstance(submission[FILE_ERRORS], list) else msgs
            if status:
                if isFile:
                    submission["fileValidationStatus"] = status
                else:
                    submission["metadataValidationStatus"] = status
            submission[UPDATED_AT] = current_datetime_str()
            result = file_collection.replace_one({ID : submission[ID]}, submission, False)
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to update submission, {submission[ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update file, {submission[ID]}: {get_exception_msg()}")
            return False  

    """
    update data records based on _id in dataRecords
    """
    def update_files(self, file_records):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                ReplaceOne( { ID: m[ID] },  m,  False)
                    for m in list(file_records)
                ])
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to update file records, {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update file records, {get_exception_msg()}")
            return False 
    """
    update data records based on node ID in dataRecords
    """
    def update_data_records(self, data_records):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                ReplaceOne( { "nodeID": m["nodeID"] },  m,  True)
                    for m in list(data_records)
                ])
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to update file records, {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update file records, {get_exception_msg()}")
            return False  
    """
    delete dataRecords by nodeIDs
    """  
    def delete_data_records(self, nodes):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                DeleteOne( { "nodeID": str(m["nodeID"]), "nodeType": m["nodeType"] })
                    for m in list(nodes)
                ])
            self.log.info(f'Total {result.deleted_count} dataRecords are deleted!')
            return result.deleted_count > 0
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to delete file records, {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to delete file records, {get_exception_msg()}")
            return False 
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
            return count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to insert data records, {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to insert data records, {get_exception_msg()}")
            return False 
    """
    retrieve dataRecords by submissionID and scope either New dataRecords or All
    """
    def get_dataRecords(self, submissionID, scope):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            query = {'submissionID': {'$eq': submissionID}} 
            if scope == STATUS_NEW:
                query[STATUS] = STATUS_NEW
            result = list(file_collection.find(query))
            count = len(result)
            self.log.info(f'Total {count} dataRecords are found for the submission, {submissionID} and scope of {scope}!')
            return result
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to retrieve data records, {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to retrieve data records, {get_exception_msg()}")
            return None 

    """
    retrieve dataRecord nby nodeID
    """
    def get_dataRecord_nodeId(self, nodeID):
        db = self.client[self.db_name]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.find_one({"nodeID": nodeID})
            return result
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to retrieve data record, {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to retrieve data record, {get_exception_msg()}")
            return None 
