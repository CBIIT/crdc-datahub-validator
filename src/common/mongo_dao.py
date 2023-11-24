from pymongo import MongoClient, errors, ReplaceOne
from bento.common.utils import get_logger
from common.constants import MONGO_DB, BATCH_COLLECTION, SUBMISSION_COLLECTION, DATA_COLlECTION, ID, UPDATED_AT, \
    SUBMISSION_ID, NODE_ID, NODE_TYPE, S3_FILE_INFO, ERRORS
from common.utils import get_exception_msg, current_datetime_str

class MongoDao:
    def __init__(self, configs):
      self.log = get_logger("Mongo DAO")
      self.config = configs
      self.client = MongoClient(configs[MONGO_DB])

    def get_batch(self, batchId, batch_db):
        db = self.client[batch_db]
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
        
    def get_submission(self, submissionId, batch_db):
        db = self.client[batch_db]
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

    def get_file(self, fileId, db):
        db = self.client[db]
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
        
    def get_file_by_name(self, fileName, db):
        db = self.client[db]
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
    
    def get_files_by_submission(self, submissionID, db):
        db = self.client[db]
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
    
    def update_batch(self, batch, batch_db):
        db = self.client[batch_db]
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
        
    def check_metadata_ids(self, nodeType, ids, id_field, submission_id, metadata_db):
        #1. check if collection exist
        db = self.client[metadata_db]
        try:
            collection = db[DATA_COLlECTION]
            #2 check if keys existing in the collection
            result = collection.find_one({NODE_ID: {'$in': ids}, SUBMISSION_ID: submission_id, NODE_TYPE: nodeType})
            if result:
                return False
        except errors.OperationFailure as oe: 
            self.log.debug(oe)
            self.log.exception(f"Failed to query DB, {metadata_db}, {nodeType}: {get_exception_msg()}!")
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to query DB, {metadata_db}, {nodeType}: {get_exception_msg()}!")
        return True
    
    def update_file (self, file_record, db):
        db = self.client[db]
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
        
    def set_submission_error(self, submission, msgs, db):
        db = self.client[db]
        file_collection = db[SUBMISSION_COLLECTION]
        try:
            submission[ERRORS] =  list(submission[ERRORS]).extend(msgs) if submission.get(ERRORS) and isinstance(submission[ERRORS], list) else msgs
            submission[UPDATED_AT] = current_datetime_str()
            result = file_collection.replace_one({ID : submission[SUBMISSION_ID]}, submission, False)
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to update submission, {submission[SUBMISSION_ID]}: {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update file, {submission[SUBMISSION_ID]}: {get_exception_msg()}")
            return False  

    def update_files (self, file_records, db):
        db = self.client[db]
        file_collection = db[DATA_COLlECTION]
        try:
            result = file_collection.bulk_write([
                ReplaceOne( { ID: m[ID] },  m,  False)
                    for m in list(file_records)
                ])
            # for record in file_records:
            #     result = file_collection.replace_one({ ID: record[ID] },  record,  False)
            return result.matched_count > 0 
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to update file records for the submission, {get_exception_msg()}")
            return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to update file records for the submission, {get_exception_msg()}")
            return False  



  
