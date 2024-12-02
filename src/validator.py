#!/usr/bin/env python3
#########Uploader.py#########
#The entry point of the cli, it control the workflows.
#############################
import os
import json
from collections import deque
from bento.common.utils import get_logger, LOG_PREFIX, get_time_stamp
from bento.common.sqs import Queue
from common.constants import SQS_NAME, MONGO_DB, DB, SERVICE_TYPE, SERVICE_TYPE_ESSENTIAL, \
    SERVICE_TYPE_FILE, SERVICE_TYPE_METADATA, SERVICE_TYPE_EXPORT, SERVICE_TYPE_PV_PULLER
from common.utils import dump_dict_to_json, get_exception_msg, cleanup_s3_download_dir
from common.mongo_dao import MongoDao
from config import Config
from essential_validator import essentialValidate
from file_validator import fileValidate
from metadata_validator import metadataValidate
from metadata_export import metadata_export
from pv_puller import pull_pv_lists

DATA_RECORDS_SEARCH_INDEX = "submissionID_nodeType_nodeID"
DATA_RECORDS_CRDC_SEARCH_INDEX = "dataCommons_nodeType_nodeID"
DATA_RECORDS_STUDY_ENTITY_INDEX = 'studyID_entityType_nodeID'
RELEASE_SEARCH_INDEX = "dataCommons_nodeType_nodeID"
CRDCID_SEARCH_INDEX = "CRDC_ID"
CDE_SEARCH_INDEX = 'CDECode_1_CDEVersion_1'

#Set log file prefix for bento logger
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Validation Service'
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
        job_queue = None
        if  configs[SERVICE_TYPE] not in [SERVICE_TYPE_PV_PULLER]:
            job_queue = Queue(configs[SQS_NAME])
        mongo_dao = MongoDao(configs[MONGO_DB], configs[DB])
        # set dataRecord search index
        if not mongo_dao.set_search_index_dataRecords(DATA_RECORDS_SEARCH_INDEX, DATA_RECORDS_CRDC_SEARCH_INDEX, DATA_RECORDS_STUDY_ENTITY_INDEX):
            log.error("Failed to set dataRecords search index!")
            return 1
        # set release search index
        if not mongo_dao.set_search_release_index(RELEASE_SEARCH_INDEX, CRDCID_SEARCH_INDEX, DATA_RECORDS_STUDY_ENTITY_INDEX):
            log.error("Failed to set release search index!")
            return 1
        
        if configs[SERVICE_TYPE] == SERVICE_TYPE_PV_PULLER and not mongo_dao.set_search_cde_index(CDE_SEARCH_INDEX):
            log.error("Failed to set cde index!")
            return 1   

    except Exception as e:
        log.exception(e)
        log.exception(f'Error occurred when initialize the application: {get_exception_msg()}')
        return 1

    #step 3: switch different validation types and call related validator.
    if configs[SERVICE_TYPE] == SERVICE_TYPE_ESSENTIAL:
        essentialValidate(configs, job_queue, mongo_dao)
    elif configs[SERVICE_TYPE] == SERVICE_TYPE_FILE:
        fileValidate(configs, job_queue, mongo_dao)
    elif configs[SERVICE_TYPE] == SERVICE_TYPE_METADATA:
        metadataValidate(configs, job_queue, mongo_dao)
    elif configs[SERVICE_TYPE] == SERVICE_TYPE_EXPORT:
        metadata_export(configs, job_queue, mongo_dao)
    elif configs[SERVICE_TYPE] == SERVICE_TYPE_PV_PULLER:
        pull_pv_lists(configs, mongo_dao)
    else:
        log.error(f'Invalid service type: {configs[SERVICE_TYPE]}!')
        return 1
if __name__ == '__main__':
    controller()