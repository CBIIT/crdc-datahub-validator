#!/usr/bin/env python3
import os
from bento.common.utils import get_logger, get_md5
from bento.common.s3 import S3Bucket
from boto3.s3.transfer import TransferConfig
from common.constants import STATUS, BATCH_TYPE_METADATA, DATA_COMMON_NAME, ERRORS, DB, \
    SUCCEEDED, ERRORS, S3_DOWNLOAD_DIR, SQS_NAME, FILE_ID, BATCH_STATUS_LOADED, \
    BATCH_STATUS_REJECTED 
from common.utils import cleanup_s3_download_dir, get_exception_msg

TRANSFER_UNIT_MB = 1024 * 1024
MULTI_PART_THRESHOLD = 100 * TRANSFER_UNIT_MB
MULTI_PART_CHUNK_SIZE = MULTI_PART_THRESHOLD
PARTS_LIMIT = 900
SINGLE_PUT_LIMIT = 4_500_000_000
BUCKET_OWNER_ACL = 'bucket-owner-full-control'

def test_s3_list():
    bucket = S3Bucket("crdcdh-test-submission")
    key = "6681e23e-c091-40b0-9dfe-b1e415d97cd7/1f42b5f1-5ea4-4923-a9bb-f496c63362ce/file/Archive.zip"
    fileName="/Users/gup2/workspace/testdata/PRECINCT01-2023-08-21/Archive.zip"
    org_md5 = get_md5(fileName)
    org_size = os.path.getsize(fileName)
    print(f"Orig MD5: {org_md5}, Orig_size: {org_size}")
    _upload_obj(bucket, fileName, key, org_size)
    fileList = bucket.bucket.objects.filter(Prefix="6681e23e-c091-40b0-9dfe-b1e415d97cd7/1f42b5f1-5ea4-4923-a9bb-f496c63362ce/file/Archive.zip")
    for file in fileList:
        file_dict = {"fileName":file.key.split('/')[-1], "size": file.size, "md5": file.e_tag.strip('"')}
        print(file_dict)

def _upload_obj(bucket, org_url, key, org_size):
    parts = int(org_size) // MULTI_PART_CHUNK_SIZE
    chunk_size = MULTI_PART_CHUNK_SIZE if parts < PARTS_LIMIT else int(org_size) // PARTS_LIMIT
    t_config = TransferConfig(multipart_threshold=MULTI_PART_THRESHOLD,multipart_chunksize=chunk_size)
    with open(org_url, 'rb') as stream:
            bucket._upload_file_obj( key, stream, t_config, {'ACL': BUCKET_OWNER_ACL})

# _upload_file_obj(self, key, data, config=None, extra_args={'ACL': BUCKET_OWNER_ACL}):

if __name__ == '__main__':
    test_s3_list()