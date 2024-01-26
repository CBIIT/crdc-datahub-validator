from pymongo import MongoClient, errors, ReplaceOne, DeleteOne, TEXT
from bento.common.utils import get_logger
from common.constants import BATCH_COLLECTION, SUBMISSION_COLLECTION, DATA_COLlECTION, ID, UPDATED_AT, \
    SUBMISSION_ID, NODE_ID, NODE_TYPE, S3_FILE_INFO, STATUS, FILE_ERRORS, STATUS_NEW, NODE_ID, NODE_TYPE, \
    PARENT_TYPE, PARENT_ID_VAL, PARENTS, FILE_VALIDATION_STATUS, METADATA_VALIDATION_STATUS, \
    FILE_MD5_COLLECTION, FILE_NAME, UPDATED_AT
from common.utils import get_exception_msg, current_datetime, get_uuid_str
# client_level: 0 default level for most db calls
# client_level: 1 for parental processes
# client_level: 2 for grand parental processes
class DBClientFactory:
    def __init__(self, connectionStr):
      self.log = get_logger("Mongo DAO")
      self.client_dict = {}
      self.connectionStr = connectionStr

    def create_mongo_client(self, client_level = 0):
        key = client_key(client_level)
        if self.client_dict.get(key):
            return self.client_dict[key]
        try:
            client = MongoClient(self.connectionStr)
            self.client_dict[client_key(client_level)] = client
            return client
        except errors.PyMongoError as pe:
            self.log.debug(pe)
            self.log.exception(f"Failed to create mongo DB client at level, {client_level}: {get_exception_msg()}")
            return None
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to create mongo DB client, {client_level}: {get_exception_msg()}")
            return None
        
    def close_mongo_client(self, generation_level = 0):
        key = client_key(generation_level)
        if self.client_dict.get(key):
            client = self.client_dict[key]
            client.close()
            client = None
    def close(self):
        for clint in self.client_dict.items():
            clint.close()
            clint = None

def client_key(level):
   return f"gen_{level}"
