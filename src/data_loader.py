#!/usr/bin/env python3
import pandas as pd
from bento.common.utils import get_logger
import os
import csv
import json
import uuid
from datetime import datetime
from metadata_config import Config
from mongo_connect import mongo_connect
from common.constants import DATA_COLlECTION
# This script load matadata files to database
# input: file info list
class DataLoader:

    def get_time_stamp():
        return datetime.now()
    
    config = Config()

    dbname = mongo_connect()
    mongo_Collection = dbname[DATA_COLlECTION]
    DATETIME_FORMAT = '%Y%m%d-%H%M%S'
    file_name = config.data['data']

    fd = open(file_name)


    if ".csv" in file_name:
        file_delimiter = ","
    elif ".tsv" in file_name:
        file_delimiter = "\t"
    reader = csv.DictReader(fd, delimiter=file_delimiter)
    importdataset = json.loads(json.dumps(list(reader), indent=2))
    row_num = len(importdataset) +1

    real_import = [
        {
            # "submissionID":str(uuid.uuid5(uuid.NAMESPACE_URL, 'submissionID')),
            "submissionID":config.data['submission'],
            "batchIDs":config.data['batch'],
            "status":"Loaded",
            "errors": None,
            "warnings":None,
            "createdAt":get_time_stamp(),
            "updatedAt":get_time_stamp(),
            "orginalFileName":file_name,
            "lineNumber":row_num,
            "nodeType":"",
            "IDPropertyName":"",
            "props":"",
            "parents":"",
            "relationshipProps":"",
            "rawData": importdataset,
            "s3FileInfo": ""
        }
    ]



    record_count = 0
    if config.data['intention'] == "insert":
        mongo_Collection.insert_many(real_import)
        print('1 documents has been inserted. ')
    if config.data['intention'] == "delete":
        mongo_query = {
            "submissionID": config.data['submission'],
            "batchIDs":config.data['batch']
        }
        result = mongo_Collection.find(mongo_query)
        for i_result in result:
            mongo_Collection.delete_one({'_id': i_result['_id']})
            record_count = record_count+1
        print(str(record_count) + ' documents has been deleted. ')


    fd.close()


    def __init__(self, configs):
        self.log = get_logger('Matedata loader')


    def load_data(self, data_frame_list):
        return True