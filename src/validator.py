#!/usr/bin/env python3
#########Uploader.py#########
#The entry point of the cli, it control the workflows.
#############################
import os
import json
from collections import deque
from bento.common.utils import get_logger, LOG_PREFIX, get_time_stamp
from bento.common.sqs import Queue
from common.constants import SQS_NAME, MONGO_DB, DB, SERVICE_TYPE, SERVICE_TYPE_ESSENTIAL,\
    SERVICE_TYPE_FILE, SERVICE_TYPE_METADATA
from common.utils import dump_dict_to_json, get_exception_msg, cleanup_s3_download_dir
from common.mongo_dao import MongoDao
from config import Config
from essential_validator import essentialValidate
from file_validator import fileValidate
from metadata_validator import metadataValidate

log = get_logger('Validator')
# public function to received args and dispatch to different modules for different uploading types, file or metadata
def controller():

    #step 1: process args, configuration file
    config = Config()
    if not config.validate():
        log.error("Failed to start the service: missing required valid parameter(s)!")
        print("Failed to start the service: invalid parameter(s)!  Please check log file in tmp folder for details.")
        return 1
    
    configs = config.data
   
    #step 2 initialize sqs queue, mongo db access object and model store
    try:
        job_queue = Queue(configs[SQS_NAME])
        mongo_dao = MongoDao(configs[MONGO_DB], configs[DB])
    except Exception as e:
        log.debug(e)
        log.exception(f'Error occurred when initialize the application: {get_exception_msg()}')
        return 1

    #step 3: switch different validation types and call related validator.
    if configs[SERVICE_TYPE] == SERVICE_TYPE_ESSENTIAL:
        essentialValidate(configs, job_queue, mongo_dao)
    elif configs[SERVICE_TYPE] == SERVICE_TYPE_FILE:
        fileValidate(configs, job_queue, mongo_dao)
    elif configs[SERVICE_TYPE] == SERVICE_TYPE_METADATA:
        metadataValidate(configs, job_queue, mongo_dao)
    else:
        log.error(f'Invalid service type: {configs[SERVICE_TYPE]}!')
        return 1
if __name__ == '__main__':
    controller()