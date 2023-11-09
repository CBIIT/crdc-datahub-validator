from pymongo import MongoClient, errors
from datetime import datetime
from dateutil import parser
from bento.common.utils import get_logger
from common.constants import MONGO_DB, BATCH_COLLECTION, SUBMISSION_COLLECTION, DATA_COLlECTION
from common.utils import get_exception_msg

class MongoDao:
    def __init__(self, configs):
      self.log = get_logger("Mongo DAO")
      self.config = configs
      self.client = MongoClient(configs[MONGO_DB])

    def get_batch(self, batchId, batch_db):
        db = self.client[batch_db]
        batch_collection = db[BATCH_COLLECTION]
        try:
            return batch_collection.find_one({"_id": batchId})
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to find batch, {batchId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to find batch, {batchId}: {get_exception_msg()}")
            return None
        
    def get_submission(self, submissionId, batch_db):
        db = self.client[batch_db]
        batch_collection = db[SUBMISSION_COLLECTION]
        try:
            return batch_collection.find_one({"_id": submissionId})
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to find submission, {submissionId}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to find submission, {submissionId}: {get_exception_msg()}")
            return None
    
    def update_batch(self, batch, batch_db):
        db = self.client[batch_db]
        batch_collection = db[BATCH_COLLECTION]
        #update the batch 
        batch['updatedAt'] = datetime.now()
        # Using update_one() method for single updating.
        try:
            result = batch_collection.replace_one({'_id' : batch['_id']}, batch, False) 
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to update batch, {batch['_id']}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update batch, {batch['_id']}: {get_exception_msg()}")
            return False
        
    def check_metadata_ids(self, nodeType, ids, id_field, submission_id, metadata_db):
        #1. check if collection exist
        db = self.client[metadata_db]
        try:
            
            collection = db[DATA_COLlECTION]
            #2 check if keys existing in the collection
            result = collection.find_one({"nodeID": {'$in': ids}, "submissionID": submission_id, "nodeType": nodeType})
            if result:
                return False
        except errors.OperationFailure as oe: 
            self.log.debug(oe)
            self.log.exception(f"Failed to query DB, {metadata_db}, {nodeType}: {get_exception_msg()}!")
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to query DB, {metadata_db}, {nodeType}: {get_exception_msg()}!")
        return True



  
