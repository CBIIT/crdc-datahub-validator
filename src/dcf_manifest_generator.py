#!/usr/bin/env python3
import pandas as pd
import json
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import SQS_TYPE, SUBMISSION_ID, BATCH_BUCKET, TYPE_EXPORT_METADATA, ID, NODE_TYPE, \
    RELEASE, ARCHIVE_RELEASE, EXPORT_METADATA, EXPORT_ROOT_PATH, SERVICE_TYPE_EXPORT, CRDC_ID, NODE_ID,\
    DATA_COMMON_NAME, CREATED_AT, MODEL_VERSION, MODEL_FILE_DIR, TIER_CONFIG, SQS_NAME, TYPE, UPDATED_AT, \
    PARENTS, PROPERTIES, SUBMISSION_REL_STATUS, SUBMISSION_REL_STATUS_RELEASED, SUBMISSION_INTENTION, \
    SUBMISSION_INTENTION_DELETE, SUBMISSION_REL_STATUS_DELETED, TYPE_COMPLETE_SUB, ORIN_FILE_NAME, TYPE_GENERATE_DCF
from common.utils import current_datetime, get_uuid_str, dump_dict_to_json, get_exception_msg
from common.model_store import ModelFactory
import threading
import io

# Private class
class GenerateDCF:
    def __init__(self, mongo_dao, submission, s3_service, model_store):
        self.log = get_logger(TYPE_EXPORT_METADATA)
        self.model_store = model_store
        self.model = None
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service
        self.intention = submission.get(SUBMISSION_INTENTION)
    def close(self):
        if self.s3_service:
            self.s3_service.close(self.log)

    def generate(self):
        # 1) get s3file
        return True
    
    def upload_to_s3(self):
        return True
        