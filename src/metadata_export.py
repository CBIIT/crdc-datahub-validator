#!/usr/bin/env python3

import json
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import SQS_TYPE, SUBMISSION_ID, BATCH_BUCKET, TYPE_EXPORT_METADATA, ID, NODE_TYPE, \
    RELEASE, ARCHIVE_RELEASE, RAW_DATA, EXPORT_METADATA, EXPORT_ROOT_PATH
import threading
import boto3
import io
from service.ecs_agent import set_scale_in_protection

VISIBILITY_TIMEOUT = 20
"""
Interface for validate files via SQS
"""


def metadata_export(sqs_name, job_queue, mongo_dao):
    export_processed = 0
    export_validator = None
    log = get_logger(TYPE_EXPORT_METADATA)
    # activate container protection
    set_scale_in_protection(True)
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {sqs_name}, '
                     f'{export_processed} metadata export have been processed so far')

            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                set_scale_in_protection(True)
                extender = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    if not data.get(SQS_TYPE) == TYPE_EXPORT_METADATA or not data.get(SUBMISSION_ID):
                        pass

                    extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                    submission_id = data[SUBMISSION_ID]
                    submission = mongo_dao.get_submission(submission_id)
                    export_validator = ExportMetadata(mongo_dao, submission, S3Service())
                    export_validator.export_data_to_file()
                    export_processed += 1
                    msg.delete()
                    set_scale_in_protection(False)
                except Exception as e:
                    log.debug(e)
                    log.critical(
                        f'Something wrong happened while exporting file! Check debug log for details.')
                finally:
                    # De-allocation memory
                    export_validator.close()
                    export_validator = None

                    if extender:
                        extender.stop()
        except KeyboardInterrupt:
            log.info('Good bye!')
            return


# Private class
class S3Service:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def close(self, log):
        try:
            self.s3_client.close()
        except Exception as e1:
            log.debug(e1)
            log.critical(
                f'An error occurred while attempting to close the s3 client! Check debug log for details.')


    def archive_s3_if_exists(self, bucket_name, rootpath):
        prev_directory = ValidationDirectory.get_release(rootpath)
        new_directory = ValidationDirectory.get_archive(rootpath)
        # 1. List all objects in the old folder
        paginator = self.s3_client.get_paginator('list_objects_v2')

        # Iterate over each object in the source directory
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prev_directory):
            if "Contents" in page:
                for obj in page["Contents"]:
                    copy_source = {'Bucket': bucket_name, 'Key': obj['Key']}
                    new_key = obj['Key'].replace(prev_directory, new_directory, 1)

                    # Copy object to the target directory
                    self.s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=new_key)

                    # Delete the original object
                    self.s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

    def upload_file_to_s3(self, data, bucket_name, file_name):
        self.s3_client.upload_fileobj(data, bucket_name, file_name)

# Private class
class ExportMetadata:
    def __init__(self, mongo_dao, submission, s3_service):
        self.log = get_logger(TYPE_EXPORT_METADATA)
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service

    def close(self):
        self.s3_service.close(self.log)

    def export_data_to_file(self):
        submission_id, rootpath, bucket_name = self.get_submission_info()
        if not rootpath or not submission_id or not bucket_name:
            self.log("The process of exporting metadata stopped due to incomplete data in the submission.")

        records = self.mongo_dao.get_dataRecords(submission_id, None)
        #  group by node_type
        nodes = {}
        for r in records:
            node_type = r.get(NODE_TYPE)
            node_raw_data = r.get(RAW_DATA)
            header = list(node_raw_data.keys())
            if not header:
                continue

            values = list(node_raw_data.values())
            if not nodes.get(node_type):
                file_name = f"{submission_id}-{node_type}"
                nodes[node_type] = [file_name, header, [values]]
                continue
            # append values for the node values
            nodes[node_type][2].append(values)

        # create file data and file name
        files_to_export = []
        for node in nodes.values():
            # each file name, header, values
            validation_file = self.create_file(node[0], node[1], node[2])
            files_to_export.append(validation_file)

        if len(files_to_export) > 0:
            self.s3_service.archive_s3_if_exists(bucket_name, rootpath)

        self.parallel_upload(files_to_export)

    def parallel_upload(self, files):
        _, rootpath, bucket_name = self.get_submission_info()
        threads = []
        for data in files:
            full_name = f"{ValidationDirectory.get_release(rootpath)}/{data.name}"
            thread = threading.Thread(target=self.s3_service.upload_file_to_s3, args=(data, bucket_name, full_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def get_submission_info(self):
        return [self.submission.get(ID), self.submission.get(EXPORT_ROOT_PATH), self.submission.get(BATCH_BUCKET)]

    def create_file(self, file_name, header, values, file_type="tsv"):
        """
        Generates a file in TSV format from given data record. The file name is derived from submission_id and
        node_type. The method writes headers and data rows based on the 'PROPERTIES' key in data_record.

        :param file_name: String file_name.
        :param header: a list string.
        :param values: a list of list[string].
        :param file_type: Format of the file, default is 'tsv'.
        :return: A list with StringIO object of file content and the file name.
        """

        buf = io.BytesIO()
        # Headers
        buf.write('\t'.join(map(str, header)).encode() + b'\n')
        # Values
        for val in values:
            buf.write('\t'.join(map(str, val)).encode() + b'\n')
        buf.seek(0)
        buf.name = f"{file_name}.{file_type}"
        return buf

# Private class
class ValidationDirectory:
    @staticmethod
    def get_archive(rootpath):
        return f"{rootpath}/{EXPORT_METADATA}/{ARCHIVE_RELEASE}"

    @staticmethod
    def get_release(rootpath):
        return f"{rootpath}/{EXPORT_METADATA}/{RELEASE}"
