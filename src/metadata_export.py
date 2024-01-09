#!/usr/bin/env python3

import json
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import SQS_TYPE, SUBMISSION_ID, PROPERTIES, BATCH_BUCKET, TYPE_EXPORT_METADATA, ID
import threading
import boto3
import csv
from io import StringIO

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
                    export_validator = ExportValidator(mongo_dao, submission, s3_service)
                    export_validator.validate()

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
                            f'Something wrong happened while delete sqs message! Check debug log for details.')
                    if extender:
                        extender.stop()
        except KeyboardInterrupt:
            log.info('Good bye!')
            return


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

    def upload_file_to_s3(self, bucket_name, key, data):
        self.s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)


class ValidationFile:

    @staticmethod
    def create_file(submission_id, node_type, data_record, file_type="tsv"):
        with StringIO() as output:
            writer = csv.writer(output, delimiter='\t')
            props = data_record.get(PROPERTIES)
            if props:
                # Write header
                headers = []
                for key in props.items():
                    headers.append(key)
                writer.writerow(headers)

                # Write data rows
                for _, value in props.items():
                    writer.writerow(value)  # Replace with actual fields
            # Reset buffer position to the beginning
            output.seek(0)
            tsv_file_name = f"{submission_id}-{node_type}.{file_type}"
            return [output.getvalue(), tsv_file_name]


class ExportValidator:
    def __init__(self, mongo_dao, submission, s3_service):
        self.log = get_logger(TYPE_EXPORT_METADATA)
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service

    def validate(self):
        submission_id, bucket_name = self.get_submission_info()
        # look up the db
        records = self.mongo_dao.get_dataRecords(submission_id, None)
        files_to_export = [ValidationFile.create_file(submission_id, r.get("type"), r) for r in records]

        for aRecord in records:
            output = ValidationFile.create_file(submission_id, aRecord.get("type"), aRecord)
            files_to_export.append(output)
        # move the existing folder
        if files_to_export:
            self.s3_service.archive_s3_if_exists(bucket_name, submission_id)

        self.parallel_upload(files_to_export)

    def parallel_upload(self, files):
        submission_id, bucket_name = self.get_submission_info()
        threads = []
        for data, file_name in files:
            key = f"{ValidationDirectory.get_release(submission_id)}/{file_name}"
            thread = threading.Thread(target=self.s3_service.upload_file_to_s3, args=(bucket_name, key, data))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def get_submission_info(self):
        return [self.submission.get(ID), self.submission.get(BATCH_BUCKET)]


class ValidationDirectory:
    @staticmethod
    def get_archive(submission_id):
        return f"submission/{submission_id}/archive_release"

    @staticmethod
    def get_release(submission_id):
        return f"submission/{submission_id}/release"
