import boto3
from botocore.exceptions import ClientError


class S3Service:
    def __init__(self, aws_profile = None):
        self.session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
        self.s3_client = self.session.client('s3')

    def close(self, log):
        try:
            self.s3_client.close()
            self.session = None
        except Exception as e1:
            log.exception(e1)
            log.critical(
                f'An error occurred while attempting to close the s3 client! Check debug log for details.')

    def archive_s3_if_exists(self, bucket_name, prev_directory, new_directory):
        # prev_directory = ValidationDirectory.get_release(root_path)
        # new_directory = ValidationDirectory.get_archive(root_path)
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

    def get_file_info(self, bucket_name, key):
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            return response.get('Metadata', {})
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                return None
            else:
                raise e
            
    def move_file(self, source_bucket, source_key, dest_bucket, dest_key):
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(Bucket=dest_bucket, CopySource=copy_source, Key=dest_key)
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                return None
            else:
                raise e

    def add_tags(self, bucket_name, key, tags):
        try:
            response = self.s3_client.put_object_tagging(
                Bucket=bucket_name,
                Key=key,
                Tagging={
                    'TagSet': tags
                }
            )
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                return None
            else:
                raise e
            
    def list_objects(self, bucket_name, prefix):
        file_key_list = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        file_key_list.append(obj['Key'])
            return file_key_list
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                return None
            else:
                raise e