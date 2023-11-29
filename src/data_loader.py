#!/usr/bin/env python3
import pandas as pd
import csv
import json


from datetime import datetime
from metadata_config import Config

# from metadata_config import Config


# from bento.common.utils import get_logger
# This script load matadata files to database
# input: file info list
class DataLoader:
    def __init__(self, configs, model, mongo_dao):
        # self.log = get_logger('Matedata loader')
        self.configs = configs
        self.model = model
        self.mongo_dao =mongo_dao

    """
    param: file_path_list downloaded from s3 bucket
    """
    def load_data(self, file_path_list):

        config = Config()
        file_name = config.data['data']
        fd = open(file_name)

        if ".csv" in file_name:
            print("csv")
            file_delimiter = ","
        elif ".tsv" in file_name:
            print("tsv")
            file_delimiter = "\t"
        reader = csv.DictReader(fd, delimiter=file_delimiter)
        importdataset = json.loads(json.dumps(list(reader), indent=2))
        row_num = len(importdataset) +1

        real_import = [
            {
                "submissionID":config.data['submission'],
                "batchIDs":config.data['batch'],
                "status":"New",
                "errors": None,
                "warnings":None,
                "createdAt":datetime.now(),
                "updatedAt":datetime.now(),
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

        return True