#!/usr/bin/env python3

import json
import os
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import ERRORS, WARNINGS, STATUS, STATUS_NEW, S3_FILE_INFO, ID, SIZE, MD5, UPDATED_AT, \
    FILE_NAME, SQS_TYPE, SQS_NAME, FILE_ID, STATUS_ERROR, STATUS_WARNING, STATUS_PASSED, SUBMISSION_ID, BATCH_BUCKET, \
    PROPERTIES

s3 = boto3.client('s3')
VISIBILITY_TIMEOUT = 20
"""
Interface for validate files via SQS
"""
import csv
from io import StringIO




def metadata_export(configs, job_queue, mongo_dao):
    export_processed = 0
    # TODO
    log = get_logger('File Validation Service')
    # validator = FileValidator(mongo_dao)

    # run file validator as a service

    # step 3: run validator as a service
    while True:
        try:
            log.info(f'Waiting for jobs on queue: {configs[SQS_NAME]}, '
                     f'{export_processed} batches have been processed so far')

            for msg in job_queue.receiveMsgs(VISIBILITY_TIMEOUT):
                log.info(f'Received a job!')
                extender = None
                data = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    # Make sure job is in correct format
                    if not data.get(SQS_TYPE) == "Export Metadata" and not data.get(SUBMISSION_ID):
                        # TODO update message
                        log.error(f'Invalid message: {data}!')

                    # TODO what is extender
                    extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)

                    # TODO get the datarecord by submission id
                    # mongo_dao
                    submission_ID = data[SUBMISSION_ID]

                    Export.create_file(submission_ID)

                    # Upload to S3
                    # TODO upload a file

                    tsv_file_name = f"{submission_id}-{node_type}.tsv"

                    s3.put_object(Bucket='your-bucket-name', Key=f"submission/{submission_id}/release/{tsv_file_name}",
                                  Body=output.getvalue())

                    export_processed += 1

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
                        extender = None
        except KeyboardInterrupt:
            log.info('Good bye!')
            return


class Export:

    def __init__(self, mongo_dao):
        pass

    @staticmethod
    def create_file(submission_id, node_type, data_record):
        data = json.loads({"message": "test"})


        with StringIO() as output:
            writer = csv.writer(output, delimiter='\t')
            # Write header

            # TODO from the datarecord,props & store the header
            # All data records' properties will be saved in columns with the header of the property name
            # For example, "sample_type" property will be saved into column with header "sample_type
            props = data_record.get(PROPERTIES)
            if props:
                columns = []
                for item in props.items():
                    columns.append(item)
            writer.writerow(columns)
            # Write data rows
            for record in data:
                writer.writerow([record['field1'], record['field2']])  # Replace with actual fields

            # Reset buffer position to the beginning
            output.seek(0)

            return output








