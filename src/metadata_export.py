#!/usr/bin/env python3

import json
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import SQS_TYPE, SUBMISSION_ID, PROPERTIES, BATCH_BUCKET, TYPE_EXPORT_METADATA, ID, NODE_TYPE, \
    RELEASE, ARCHIVE_RELEASE, PARENTS, PARENT_TYPE
import threading
import boto3
import io

s3_client = boto3.client('s3')
VISIBILITY_TIMEOUT = 20
"""
Interface for validate files via SQS
"""


def metadata_export(sqs_name, job_queue, mongo_dao):
    log = get_logger(TYPE_EXPORT_METADATA)
    s3_service = S3Service(s3_client)
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {sqs_name}')

            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                extender = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    if not data.get(SQS_TYPE) == TYPE_EXPORT_METADATA or not data.get(SUBMISSION_ID):
                        pass

                    extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                    submission_id = data[SUBMISSION_ID]
                    submission = mongo_dao.get_submission(submission_id)
                    export_validator = ExportMetadata(mongo_dao, submission, s3_service)
                    export_validator.export_data_to_file()

                except Exception as e:
                    log.debug(e)
                    log.critical(
                        f'Something wrong happened while exporting file! Check debug log for details.')
                finally:
                    try:
                        msg.delete()
                    except Exception as e1:
                        log.debug(e1)
                        log.critical(
                            f'Something wrong happened while exporting file sqs message! Check debug log for details.')
                    if extender:
                        extender.stop()
        except KeyboardInterrupt:
            log.info('Good bye!')
            return


# Private class
class S3Service:
    def __init__(self, client):
        self.s3_client = client

    def archive_s3_if_exists(self, bucket_name, submission_id):
        prev_directory = ValidationDirectory.get_release(submission_id)
        new_directory = ValidationDirectory.get_archive(submission_id)
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
class ValidationFile:

    @staticmethod
    def create_file(file_name, record_file, file_type="tsv"):
        """
        Generates a file in TSV format from given data record. The file name is derived from submission_id and
        node_type. The method writes headers and data rows based on the 'PROPERTIES' key in data_record.

        :param file_name: String file_name.
        :param record_file: a parsed file object.
        :param file_type: Format of the file, default is 'tsv'.
        :return: A list with StringIO object of file content and the file name.
        """

        buf = io.BytesIO()
        # Headers
        buf.write('\t'.join(map(str, record_file.header())).encode() + b'\n')
        # Values
        buf.write('\t'.join(map(str, record_file.values())).encode() + b'\n')
        buf.seek(0)
        buf.name = f"{file_name}.{file_type}"
        return buf


class FileData:
    def __init__(self, data_record):
        self.props = data_record.get(PROPERTIES)
        self.parents = data_record.get(PARENTS)

    def header(self):
        node_header = list(self.props.keys())
        for node in self.parents:
            parent_type = node.get(PARENT_TYPE)
            if parent_type:
                for parent in node.keys():
                    node_header.append(f"{parent_type}.{parent}")
        return node_header

    def values(self):
        node_values = list(self.props.values())
        for node in self.parents:
            if node.get(PARENT_TYPE):
                for parent_key in node.values():
                    node_values.append(parent_key)
        return node_values


# Private class
class ExportMetadata:
    def __init__(self, mongo_dao, submission, s3_service):
        self.log = get_logger(TYPE_EXPORT_METADATA)
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service

    def export_data_to_file(self):
        submission_id, bucket_name = self.get_submission_info()
        records = self.mongo_dao.get_dataRecords(submission_id, None)
        # create file data and file name
        files_to_export = []
        for r in records:
            formatted_file = FileData(r)
            filename = f"{submission_id}-{r.get(NODE_TYPE)}"
            validation_file = ValidationFile.create_file(filename, formatted_file)
            files_to_export.append(validation_file)

        if files_to_export:
            self.s3_service.archive_s3_if_exists(bucket_name, submission_id)

        self.parallel_upload(files_to_export)

    def parallel_upload(self, files):
        submission_id, bucket_name = self.get_submission_info()
        threads = []
        for data in files:
            full_name = f"{ValidationDirectory.get_release(submission_id)}/{data.name}"
            thread = threading.Thread(target=self.s3_service.upload_file_to_s3, args=(data, bucket_name, full_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def get_submission_info(self):
        return [self.submission.get(ID), self.submission.get(BATCH_BUCKET)]


# Private class
class ValidationDirectory:
    @staticmethod
    def get_archive(submission_id):
        return f"submission/{submission_id}/{ARCHIVE_RELEASE}"

    @staticmethod
    def get_release(submission_id):
        return f"submission/{submission_id}/{RELEASE}"
