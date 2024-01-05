#!/usr/bin/env python3

import json
import os
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import SQS_TYPE, SQS_NAME, SUBMISSION_ID, PROPERTIES
import threading
import boto3

s3 = boto3.client('s3')
VISIBILITY_TIMEOUT = 20
"""
Interface for validate files via SQS
"""
import csv
from io import StringIO

# TODO
bucket_name = "bucket_name"


class S3directory:
    @staticmethod
    def get_archive(submission_id):
        return f"submission/{submission_id}/archive_release"

    @staticmethod
    def get_release(submission_id):
        return f"submission/{submission_id}/release"


def metadata_export(configs, job_queue, mongo_dao):
    log = get_logger('File Validation Service')
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {configs[SQS_NAME]}')

            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                extender = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    if not data.get(SQS_TYPE) == "Export Metadata" or not data.get(SUBMISSION_ID):
                        pass

                    extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                    submission_id = data[SUBMISSION_ID]
                    # look up the db
                    records = mongo_dao.get_dataRecords(submission_id)
                    files_to_export = [File.create_file(submission_id, r.get("type"), r) for r in records]

                    for aRecord in records:
                        output = File.create_file(submission_id, aRecord.get("type"), aRecord)
                        files_to_export.append(output)
                    # move the existing folder
                    if files_to_export:
                        archive_s3_if_exists(submission_id)

                    parallel_upload(submission_id, files_to_export)

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


def upload_file_to_s3(s3_client, key, data):
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)


def parallel_upload(submission_id, files_to_export):
    s3_client = boto3.client('s3')
    threads = []
    for data, file_name in files_to_export:
        key = f"{S3directory.get_release(submission_id)}/{file_name}"
        thread = threading.Thread(target=upload_file_to_s3, args=(s3_client, bucket_name, key, data))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


# TODO should I create a s3 service?
def archive_s3_if_exists(submission_id):
    s3_client = boto3.client('s3')
    prev_directory = S3directory.get_release(submission_id)
    new_directory = S3directory.get_archive(submission_id)
    # 1. List all objects in the old folder
    objects_to_rename = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prev_directory)

    if 'Contents' in objects_to_rename:
        for obj in objects_to_rename['Contents']:
            old_key = obj['Key']
            new_key = new_directory + old_key[len(prev_directory):]

            # 2. Copy each object to the new folder
            s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': old_key}, Key=new_key)

            # 3. Delete the old object
            s3_client.delete_object(Bucket=bucket_name, Key=old_key)
        print("Folder renamed successfully.")


class File:
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
            return [output, tsv_file_name]
