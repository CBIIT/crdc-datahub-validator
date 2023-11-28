import os
from pymongo import MongoClient
from metadata_config import Config
from common.constants import DATABASE

config = Config()

def mongo_connect():    
 
    CONNECTION_STRING = config.data['mongo']

 
    client = MongoClient(CONNECTION_STRING)
 
    return client[DATABASE]
  


if __name__ == '__main__':
    mongo_connect()