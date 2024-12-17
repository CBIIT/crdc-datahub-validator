#!/usr/bin/env python3
import pandas as pd
import json
import boto3
import time
import threading
from botocore.exceptions import ClientError
from bento.common.sqs import VisibilityExtender
from bento.common.utils import get_logger
from common.constants import SQS_TYPE, SUBMISSION_ID, BATCH_BUCKET, TYPE_EXPORT_METADATA, ID, NODE_TYPE, \
    RELEASE, ARCHIVE_RELEASE, EXPORT_METADATA, EXPORT_ROOT_PATH, SERVICE_TYPE_EXPORT, CRDC_ID, NODE_ID,\
    DATA_COMMON_NAME, CREATED_AT, MODEL_VERSION, MODEL_FILE_DIR, TIER_CONFIG, SQS_NAME, TYPE, UPDATED_AT, \
    PARENTS, PROPERTIES, SUBMISSION_REL_STATUS, SUBMISSION_REL_STATUS_RELEASED, SUBMISSION_INTENTION, \
    SUBMISSION_INTENTION_DELETE, SUBMISSION_REL_STATUS_DELETED, TYPE_COMPLETE_SUB, ORIN_FILE_NAME, TYPE_GENERATE_DCF,\
    STUDY_ID, DM_BUCKET_CONFIG_NAME, DATASYNC_ROLE_ARN_CONFIG, ENTITY_TYPE, SUBMISSION_HISTORY, RELEASE_AT, \
    SUBMISSION_INTENTION_NEW_UPDATE, SUBMISSION_DATA_TYPE, SUBMISSION_DATA_TYPE_METADATA_ONLY
from common.utils import current_datetime, get_uuid_str, dump_dict_to_json, get_exception_msg, get_date_time, dict_exists_in_list
from common.model_store import ModelFactory
from dcf_manifest_generator import GenerateDCF
import threading
import boto3
import io
from service.ecs_agent import set_scale_in_protection
import os
import os

VISIBILITY_TIMEOUT = 20
BATCH_SIZE = 1000

"""
Interface for validate files via SQS
"""
def metadata_export(configs, job_queue, mongo_dao):
    export_processed = 0
    export_validator = None
    log = get_logger(TYPE_EXPORT_METADATA)
    try:
        model_store = ModelFactory(configs[MODEL_FILE_DIR], configs[TIER_CONFIG]) 
        # dump models to json files
        dump_dict_to_json(model_store.models, f"models/data_model.json")
    except Exception as e:
        log.exception(e)
        log.exception(f'Error occurred when initialize metadata validation service: {get_exception_msg()}')
        return 1
    scale_in_protection_flag = False
    log.info(f'{SERVICE_TYPE_EXPORT} service started')
    while True:
        try:
            msgs = job_queue.receiveMsgs(VISIBILITY_TIMEOUT)
            if len(msgs) > 0:
                log.info(f'New message is coming: {configs[SQS_NAME]}, '
                         f'{export_processed} {SERVICE_TYPE_EXPORT} validation(s) have been processed so far')
                scale_in_protection_flag = True
                set_scale_in_protection(True)
            else:
                if scale_in_protection_flag is True:
                    scale_in_protection_flag = False
                    set_scale_in_protection(False)

            for msg in msgs:
                log.info(f'Received a job!')
                extender = None
                try:
                    data = json.loads(msg.body)
                    log.debug(data)
                    if not data.get(SQS_TYPE) in [TYPE_EXPORT_METADATA, TYPE_COMPLETE_SUB, TYPE_GENERATE_DCF] or not data.get(SUBMISSION_ID):
                        pass
                    
                    extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                    submission_id = data[SUBMISSION_ID]
                    submission = mongo_dao.get_submission(submission_id)
                    if not submission:
                        log.error(f'Submission {submission_id} does not exist!')
                        continue
                    if data.get(SQS_TYPE) == TYPE_EXPORT_METADATA: 
                        export_validator = ExportMetadata(mongo_dao, submission, S3Service(), model_store, configs)
                        export_validator.export_data_to_file()
                        # transfer metadata to destination s3 bucket if error occurred.
                        export_validator.transfer_release_metadata()
                    elif data.get(SQS_TYPE) == TYPE_COMPLETE_SUB:
                        export_validator = ExportMetadata(mongo_dao, submission, None, model_store, configs)
                        if export_validator.release_data():
                            if not submission.get(SUBMISSION_DATA_TYPE) or (submission[SUBMISSION_DATA_TYPE] != SUBMISSION_DATA_TYPE_METADATA_ONLY): 
                                export_validator.transfer_released_files()
                    elif data.get(SQS_TYPE) == TYPE_GENERATE_DCF:
                        export_validator = GenerateDCF(configs, mongo_dao, submission, S3Service())
                        export_validator.generate_dcf()
                    else:
                        pass
                    export_processed += 1
                    msg.delete()
                except Exception as e:
                    log.critical(e)
                    log.critical(
                        f'Something wrong happened while exporting data! Check debug log for details.')
                finally:
                    # De-allocation memory
                    if export_validator:
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
            log.exception(e1)
            log.critical(
                f'An error occurred while attempting to close the s3 client! Check debug log for details.')

    def archive_s3_if_exists(self, bucket_name, root_path):
        prev_directory = ValidationDirectory.get_release(root_path)
        new_directory = ValidationDirectory.get_archive(root_path)
        # 1. List all objects in the old folder
        paginator = self.s3_client.get_paginator('list_objects_v2')

        # Iterate over each object in the source directory
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prev_directory):
            if "Contents" in page:
                for obj in page["Contents"]:
                    copy_source = {'Bucket': bucket_name, 'Key': obj['Key']}
                    new_key = obj['Key'].replace(prev_directory, new_directory, 1)
                    if not copy_source  or not new_key:
                        continue
                    # Copy object to the target directory
                    self.s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=new_key)

                    # Delete the original object
                    self.s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

    def upload_file_to_s3(self, data, bucket_name, file_name):
        self.s3_client.upload_fileobj(data, bucket_name, file_name)

# Private class
class ExportMetadata:
    def __init__(self, mongo_dao, submission, s3_service, model_store, configs):
        self.log = get_logger(TYPE_EXPORT_METADATA)
        self.configs = configs
        self.model_store = model_store
        self.model = None
        self.mongo_dao = mongo_dao
        self.submission = submission
        self.s3_service = s3_service
        self.intention = submission.get(SUBMISSION_INTENTION)

    def close(self):
        if self.s3_service:
            self.s3_service.close(self.log)

    def export_data_to_file(self):
        submission_id, root_path, bucket_name, _, _ = self.get_submission_info()
        if not root_path or not submission_id or not bucket_name:
            self.log(f"{submission_id}: The process of exporting metadata stopped due to incomplete data in the submission.")

        datacommon = self.submission.get(DATA_COMMON_NAME)
        model_version = self.submission.get(MODEL_VERSION)
        #1 get data model based on datacommon and version
        self.model = self.model_store.get_model_by_data_common_version(datacommon, model_version)
        if not self.model.model or not self.model.get_nodes():
            msg = f'{submission_id}: {self.datacommon} model version "{model_version}" is not available.'
            self.log.error(msg)
            return 
        #2 archive existing release if exists
        try:
            self.s3_service.archive_s3_if_exists(bucket_name, root_path)
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f'{submission_id}: Failed to archive existed release: {get_exception_msg()}.')
            return

        node_types = self.model.get_node_keys()
        threads = []
        #3 retrieve data for nodeType and export to s3 bucket
        for node_type in node_types:
            thread = threading.Thread(target=self.export, args=(submission_id, node_type))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        
    def export(self, submission_id, node_type):
        start_index = 0
        rows = []
        columns = set()
        file_name = ""
        main_nodes = self.model.get_main_nodes()
        while True:
            # get nodes by submissionID and nodeType
            data_records = self.mongo_dao.get_dataRecords_chunk_by_nodeType(submission_id, node_type, start_index, BATCH_SIZE)
            if start_index == 0 and (not data_records or len(data_records) == 0):
                return
            
            for r in data_records:
                # node_id = r.get(NODE_ID)
                crdc_id = r.get(CRDC_ID) if node_type in main_nodes.keys() else None
                row_list = self.convert_2_row(r, node_type, crdc_id)
                rows.extend(row_list)
                if r.get(ORIN_FILE_NAME) != file_name:
                    columns.update(row_list[0].keys())
                    file_name = r.get(ORIN_FILE_NAME) 

            count = len(data_records) 
            if count < BATCH_SIZE: 
                df = None
                buf = None
                try:
                    df = pd.DataFrame(rows, columns = self.sort_columns(columns, node_type))
                    buf = io.BytesIO()
                    df.to_csv(buf, sep ='\t', index=False)
                    buf.seek(0)
                    self.upload_file(buf, node_type)
                    self.log.info(f"{submission_id}: {count + start_index} {node_type} nodes are exported.")
                    return
                except Exception as e:
                    self.log.exception(e)
                    self.log.exception(f'{submission_id}: Failed to export {node_type} data: {get_exception_msg()}.')
                finally:
                    if buf:
                        del buf
                        del df
                        del rows
                        del columns

            start_index += count 

    def convert_2_row(self, data_record, node_type, crdc_id):
        rows = []
        row = data_record.get(PROPERTIES)
        row[TYPE] = node_type
        if crdc_id is not None:
            row[CRDC_ID.lower()] = crdc_id
        else:
            if CRDC_ID.lower() in row.keys():
                del row[CRDC_ID.lower()]
        rows.append(row)
        parents = data_record.get("parents", None)
        if parents: 
            parent_types = list(set([item["parentType"] for item in parents]))
            for type in parent_types:
                same_type_parents = [item for item in parents if item["parentType"] == type]
                rel_name = f'{same_type_parents[0].get("parentType")}.{same_type_parents[0].get("parentIDPropName")}'
                if len(same_type_parents) == 1:
                    for item in rows:
                        item[rel_name] = same_type_parents[0].get("parentIDValue")
                else:
                    index = 0
                    for parent in same_type_parents:
                        if index == 0:
                            row[rel_name] = parent.get("parentIDValue")
                        else:
                            m2m_row = row.copy()
                            m2m_row[rel_name] = parent.get("parentIDValue")
                            rows.append(m2m_row)
                        index += 1
        return rows
    
    def upload_file(self, buf, node_type):
        id, root_path, bucket_name,_,_ = self.get_submission_info()      
        full_name = f"{ValidationDirectory.get_release(root_path)}/{id}-{node_type}.tsv"
        self.s3_service.upload_file_to_s3(buf, bucket_name, full_name)

    def release_data(self):
        submission_id = self.submission[ID]
        datacommon = self.submission.get(DATA_COMMON_NAME)
        model_version = self.submission.get(MODEL_VERSION)
        #1 get data model based on datacommon and version
        self.model = self.model_store.get_model_by_data_common_version(datacommon, model_version)
        if not self.model.model or not self.model.get_nodes():
            msg = f'{submission_id}: {self.datacommon} model version "{model_version}" is not available.'
            self.log.error(msg)
            return False
        
        node_types = self.model.get_node_keys()
        try:
            #2 retrieve data for nodeType and save data to release collection
            for node_type in list(node_types)[::-1]:
                self.save_releases(submission_id, node_type)
            return True
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f'{submission_id}: Failed to release data: {get_exception_msg()}.')
            return False

    def save_releases(self, submission_id, node_type):
        start_index = 0
        while True:
            # get nodes by submissionID and nodeType
            data_records = self.mongo_dao.get_dataRecords_chunk_by_nodeType(submission_id, node_type, start_index, BATCH_SIZE)
            if start_index == 0 and (not data_records or len(data_records) == 0):
                return
            
            for r in data_records:
                node_id = r.get(NODE_ID)
                crdc_id = r.get(CRDC_ID)
                self.save_release(r, node_type, node_id, crdc_id)

            count = len(data_records) 
            if count < BATCH_SIZE: 
                self.log.info(f"{submission_id}: {count + start_index} {node_type} nodes are {'released' if self.intention != SUBMISSION_INTENTION_DELETE else 'deleted'}.")
                return

            start_index += count 

    def save_release(self, data_record, node_type, node_id, crdc_id):
        if not node_type or not node_id: 
             self.log.error(f"{self.submission[ID]}: Invalid data to export: {node_type}/{node_id}/{crdc_id}!")
             return
        existed_crdc_record = self.mongo_dao.search_release(self.submission.get(DATA_COMMON_NAME), node_type, node_id)
        current_date = current_datetime()
        if not existed_crdc_record:
            if self.submission.get(SUBMISSION_INTENTION) == SUBMISSION_INTENTION_DELETE:
                self.log.error(f"{self.submission[ID]}: No data found for delete: {self.submission.get(DATA_COMMON_NAME)}/{node_type}/{node_id}/{crdc_id}!")
                return
            # create new crdc_record
            crdc_record = {
                ID: get_uuid_str(),
                CRDC_ID: crdc_id,
                SUBMISSION_ID: self.submission[ID],
                SUBMISSION_REL_STATUS: SUBMISSION_REL_STATUS_RELEASED, 
                DATA_COMMON_NAME: self.submission.get(DATA_COMMON_NAME),
                NODE_TYPE: node_type,
                NODE_ID: node_id,
                PROPERTIES: data_record.get(PROPERTIES),
                PARENTS: data_record.get(PARENTS, None),
                CREATED_AT: current_date,
                ENTITY_TYPE: data_record.get(ENTITY_TYPE),
                SUBMISSION_HISTORY: [{SUBMISSION_ID: self.submission[ID],
                             SUBMISSION_INTENTION: self.submission.get(SUBMISSION_INTENTION),
                             RELEASE_AT: current_date,
                             PROPERTIES: data_record.get(PROPERTIES),
                             PARENTS: data_record.get(PARENTS, None)
                             }], 
                STUDY_ID: data_record.get(STUDY_ID) or self.submission.get(STUDY_ID)
            }

            result = self.mongo_dao.insert_release(crdc_record)
            if not result:
                self.log.error(f"{self.submission[ID]}: Failed to insert release for {node_type}/{node_id}/{crdc_id}!")
        else: 
            existed_crdc_record[UPDATED_AT] = current_date
            if self.intention == SUBMISSION_INTENTION_DELETE:
                existed_crdc_record[SUBMISSION_REL_STATUS] = SUBMISSION_REL_STATUS_DELETED
            else: 
                history = existed_crdc_record.get(SUBMISSION_HISTORY)
                # if the existing release has no history, need add current one to the history list before updating
                if not history or len(history) == 0:
                    # make a copy before updating
                    copy = existed_crdc_record.copy()
                    history = [{
                        SUBMISSION_ID: copy[SUBMISSION_ID],
                        SUBMISSION_INTENTION: copy.get(SUBMISSION_INTENTION, SUBMISSION_INTENTION_NEW_UPDATE),
                        RELEASE_AT: copy.get(UPDATED_AT),
                        PROPERTIES: copy.get(PROPERTIES),
                        PARENTS: copy.get(PARENTS)
                    }]
                # updating existing release with new values
                existed_crdc_record[SUBMISSION_ID] = self.submission[ID]
                existed_crdc_record[PROPERTIES] = data_record.get(PROPERTIES)
                existed_crdc_record[PARENTS] = self.combine_parents(node_type, existed_crdc_record[PARENTS], data_record.get(PARENTS))
                existed_crdc_record[SUBMISSION_REL_STATUS] = SUBMISSION_REL_STATUS_RELEASED,
                history.append({
                    SUBMISSION_ID: self.submission[ID],
                    SUBMISSION_INTENTION: self.submission.get(SUBMISSION_INTENTION),
                    RELEASE_AT: current_date,
                    PROPERTIES: data_record.get(PROPERTIES),
                    PARENTS: existed_crdc_record[PARENTS]
                })
                existed_crdc_record[SUBMISSION_HISTORY] = history
                existed_crdc_record[ENTITY_TYPE] = data_record.get(ENTITY_TYPE),
                existed_crdc_record[STUDY_ID] = data_record.get(STUDY_ID) or self.submission.get(STUDY_ID)

            result = self.mongo_dao.update_release(existed_crdc_record)
            if not result:
                self.log.error(f"{self.submission[ID]}: Failed to update release for {node_type}/{node_id}/{crdc_id}!")
                return
            # process released children and set release status to "Deleted"
            if self.intention == SUBMISSION_INTENTION_DELETE:
                result, children = self.mongo_dao.get_released_nodes_by_parent_with_status(self.submission[DATA_COMMON_NAME], existed_crdc_record, [SUBMISSION_REL_STATUS_RELEASED, None], self.submission[ID])
                if result and children and len(children) > 0: 
                    self.delete_release_children(children)
    
    def combine_parents(self, node_type, release_parents, node_parents):
        if not release_parents or len(release_parents) == 0:
            return node_parents
        if not node_parents or len(node_parents) == 0:
            return release_parents
        relationships = self.model.get_node_relationships(node_type)
        if node_parents and len(node_parents):
            for parent in node_parents:
                relationship = relationships[parent["parentType"]]
                if not dict_exists_in_list(release_parents, parent, keys=["parentType", "parentIDPropName", "parentIDValue"]):
                    if relationship["type"] == "many_to_many":
                        release_parents.append(parent)
                    else:
                        rel_parent_list = [p for p in release_parents if p["parentType"] == relationship["dest_node"]]
                        if not rel_parent_list or len(rel_parent_list) == 0:
                            release_parents.append(parent)
                        else:
                            rel_parent_list[0]["parentIDValue"] = parent["parentIDValue"]
        return release_parents
    
    def delete_release_children(self, released_children):
        if released_children and len(released_children) > 0:
            for child in released_children:
                child[UPDATED_AT] = current_datetime()
                child[SUBMISSION_REL_STATUS] = SUBMISSION_REL_STATUS_DELETED
                result = self.mongo_dao.update_release(child)
                if not result:
                    self.log.error(f"{self.submission[ID]}: Failed to update release for {child.get(NODE_TYPE)}/{child.get(NODE_ID)}/{child.get(CRDC_ID)}!")
                    return
                # process released children and set release status to "Deleted"
                result, descendent = self.mongo_dao.get_released_nodes_by_parent_with_status(self.submission[DATA_COMMON_NAME], child, [SUBMISSION_REL_STATUS_RELEASED, None], self.submission[ID])
                if result and descendent and len(descendent) > 0: 
                    self.delete_release_children(descendent)
        return
        
    def get_submission_info(self):
        return [self.submission.get(ID), self.submission.get(EXPORT_ROOT_PATH), self.submission.get(BATCH_BUCKET), 
                self.submission.get(DATA_COMMON_NAME), self.submission.get(STUDY_ID)]
    
    def sort_columns(self, cols, node_type):
        columns = list(cols)
        old_index = columns.index(TYPE)
        columns.insert(0, columns.pop(old_index))
        old_index = columns.index(self.model.get_node_id(node_type))
        columns.insert(1, columns.pop(old_index))
        return columns
    
    def transfer_release_metadata(self):
        """
        transfer released data to cds cbiit metadata bucket by aws datasync
        """
        id, root_path, bucket_name, dataCommon, _ = self.get_submission_info()
        dest_bucket_name = self.mongo_dao.get_bucket_name("Metadata Bucket", dataCommon)
        dest_file_folder =  f'{get_date_time("%Y-%m-%dT%H:%M:%S")}-{id}'
        data_file_folder = os.path.join(root_path, "metadata/release")
        tags = [
            {"Key": "Tier", "Value": self.configs[TIER_CONFIG]},
            {"Key": "Type", "Value" : "Metadata"}
            ]
        self.transfer_s3_obj(bucket_name, data_file_folder, dest_bucket_name, dest_file_folder, tags)
    
    def transfer_released_files(self):
        """
        transfer released files includes data files and metadata files to data manage bucket by aws datasync
        """
        _, root_path, bucket_name, _, study_id = self.get_submission_info()
        dest_bucket_name = self.configs.get(DM_BUCKET_CONFIG_NAME)
        dest_file_folder =  study_id
        data_file_folder = os.path.join(root_path, "file")
        tags = [
            {"Key": "Tier", "Value": self.configs[TIER_CONFIG]}, 
            {"Key": "Type", "Value" : "Data File"}
            ]
        self.transfer_s3_obj(bucket_name, data_file_folder, dest_bucket_name, dest_file_folder, tags)

    def transfer_s3_obj(self, bucket_name, data_file_folder, dest_bucket_name, dest_file_folder, tags):
        """
        transfer s3 object with AWS DataSync
        """
        datasync_role = self.configs.get(DATASYNC_ROLE_ARN_CONFIG)
        datasync = boto3.client('datasync')
        try:
            # Create source S3 location
            source_location = datasync.create_location_s3(
                S3BucketArn=f'arn:aws:s3:::{bucket_name}',
                S3Config={'BucketAccessRoleArn': datasync_role},
                Subdirectory= f'{data_file_folder}'
            )

            # Create destination S3 location
            destination_location = datasync.create_location_s3(
                S3BucketArn=f'arn:aws:s3:::{dest_bucket_name}',
                S3Config={'BucketAccessRoleArn': datasync_role},
                Subdirectory=f'{dest_file_folder}'
            )

            # Create DataSync task
            task = datasync.create_task(
                SourceLocationArn=source_location['LocationArn'],
                DestinationLocationArn=destination_location['LocationArn'],
                Name='Data_Hub_SyncTask',
                Options={
                    'VerifyMode': 'ONLY_FILES_TRANSFERRED'
                },
                Tags = tags
            )
            self.log.info(f"DataSync task {task['TaskArn']} created to transfer files from {data_file_folder} to {dest_bucket_name}:{dest_file_folder}.")

            # Start DataSync task
            task_execution = datasync.start_task_execution(
                TaskArn=task['TaskArn']
            )
            task_execution_arn = task_execution['TaskExecutionArn']
            self.log.info(f"Started DataSync task execution: {task_execution_arn}")

            start_monitoring_task(task_execution_arn, task['TaskArn'], datasync, source_location, destination_location, self.log)

        except ClientError as ce:
            self.log.exception(ce)
            self.log.exception(f"Failed to transfer files from {data_file_folder} to {dest_bucket_name}:{dest_file_folder}. {ce.response['Error']['Message']}")
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to transfer files from {data_file_folder} to {dest_bucket_name}:{dest_file_folder}. {get_exception_msg()}")


def start_monitoring_task(task_execution_arn, task_arn, dataSync, source, dest, log):
            monitor_thread = threading.Thread(target=monitor_datasync_task, args=(task_execution_arn, task_arn, dataSync, source, dest, log))
            monitor_thread.start()
    
def monitor_datasync_task(task_execution_arn, task_arn, datasync, source, dest, log, wait_interval=30):
        # Initialize the DataSync client
        try:
            # Poll the task status
            while True:
                response = datasync.describe_task_execution(TaskExecutionArn=task_execution_arn)
                status = response['Status']
                
                if status in ['SUCCESS', 'ERROR']:
                    print(f"Task: {task_arn} completed with status: {status}")
                    # wait 5 min or 300 sec for SNS to send notification before delete the tasks.
                    print(f"Wait 5min before deleting task and locations.")
                    time.sleep(300)
                    datasync.delete_task(TaskArn=task_arn)
                    print(f"Task: {task_arn} deleted.")
                    datasync.delete_location(LocationArn=source['LocationArn'])
                    print(f"Source location: {source['LocationArn']} is deleted.")
                    datasync.delete_location(LocationArn=dest['LocationArn'])
                    print(f"Destination location: {dest['LocationArn']} is deleted.")
                    
                    break
                else:
                    print(f"Current status for task {task_arn}: {status}. Waiting for {wait_interval} seconds before next check...")
                    time.sleep(wait_interval)
        except ClientError as ce:
            log.exception(ce)
            log.exception(f"Failed to monitor DataSync task {task_arn}: {ce.response['Error']['Message']}")
        except Exception as e:
            log.exception(e)
            log.exception(f"Failed to monitor DataSync task {task_arn}: {get_exception_msg()}")
        finally:
            source = None
            dest = None
            datasync.close()
            datasync = None
# Private class
class ValidationDirectory:
    @staticmethod
    def get_archive(root_path):
        return f"{root_path}/{EXPORT_METADATA}/{ARCHIVE_RELEASE}"

    @staticmethod
    def get_release(root_path):
        return f"{root_path}/{EXPORT_METADATA}/{RELEASE}"