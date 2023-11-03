#!/usr/bin/env python3
#########Uploader.py#########
#The entry point of the cli, it control the workflows based on the upload type, file or metadata.
#############################
import os
import json
from collections import deque

from bento.common.utils import get_logger, LOG_PREFIX, get_time_stamp
from bento.common.sqs import Queue, VisibilityExtender
from common.constants import SQS_NAME, BATCH_ID, BATCH_DB, S3_DOWNLOAD_DIR, BATCH_STATUS, \
    BATCH_STATUS_LOADED, BATCH_STATUS_REJECTED
from common.utils import dump_dict_to_json, get_exception_msg, cleanup_s3_download_dir
from common.mongo_dao import MongoDao
from common.model_store import ModelFactory
from config import Config
from essential_validator import MetaDataValidator
from data_loader import DataLoader

VISIBILITY_TIMEOUT = 30

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Validator Main'

log = get_logger('Validator')
# public function to received args and dispatch to different modules for different uploading types, file or metadata
def controller():

    #step 1: process args, configuration file
    config = Config()
    if not config.validate():
        log.error("Failed to upload files: missing required valid parameter(s)!")
        print("Failed to upload files: invalid parameter(s)!  Please check log file in tmp folder for details.")
        return 1
    
    configs = config.data
    files_processed = 0

    #step 2 initialize sqs queue, mongo db access object model store and validator
    try:
        job_queue = Queue(configs[SQS_NAME])
        mongo_dao = MongoDao(configs)
        model_store = ModelFactory(configs) 
        # dump models to json
        dump_dict_to_json([model['model'] for model in model_store.models], f"tmp/data_models_dump.json")
        validator = MetaDataValidator(configs, mongo_dao, model_store)
    except Exception as e:
        log.debug(e)
        log.exception(f'Error occurred when initialize the application: {get_exception_msg()}')
        return 1

    #step 3: run validator as a service
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {configs[SQS_NAME]}, '
                            f'{files_processed} files have been processed so far')
            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                extender = None
                data = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    # Make sure job is in correct format
                    if data.get(BATCH_ID):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        #1. call mongo_dao to get batch by batch_id
                        batch = mongo_dao.get_batch(data[BATCH_ID], configs[BATCH_DB])
                        #2. get file list from the batch anf filter tsv or txt file
                        result = validator.validate(batch)
                        if result and len(validator.data_frame_list) > 0:
                            #4. call mongo_dao to load data
                            data_loader = DataLoader(configs)
                            result = data_loader.load(validator.data_frame_list)
                            batch[BATCH_STATUS] = BATCH_STATUS_LOADED if result else BATCH_STATUS_REJECTED
                        else:
                            batch[BATCH_STATUS] = BATCH_STATUS_REJECTED
                        #5. update batch
                        result = mongo_dao.update_batch( batch, configs[BATCH_DB])
                    else:
                        log.error(f'Invalid message: {data}!')
                    
                except Exception as e:
                    log.debug(e)
                    log.critical(
                        f'Something wrong happened while processing file! Check debug log for details.')
                finally:
                    msg.delete()
                    if extender:
                        extender.stop()
                        extender = None
                    #cleanup contents in the s3 download dir
                    cleanup_s3_download_dir(S3_DOWNLOAD_DIR)
        except KeyboardInterrupt:
            log.info('Good bye!')
            return

if __name__ == '__main__':
    controller()