#define constants, enums, etc.
#config 
MONGO_DB = "connection-str"
SQS_NAME = "sqs"
RETRIES = "retries"
DB = "db"
SERVICE_TYPE = "service-type"
SERVICE_TYPE_ESSENTIAL = "essential"
SERVICE_TYPE_FILE= "file"
SERVICE_TYPE_METADATA= "metadata"
SERVICE_TYPES = [SERVICE_TYPE_ESSENTIAL, SERVICE_TYPE_FILE, SERVICE_TYPE_METADATA]

MODEL_FILE_DIR = "models-loc"
LOADER_QUEUE = "LOADER_QUEUE"
FILE_QUEUE = "FILE_QUEUE"
METADATA_QUEUE = "METADATA_QUEUE"
#file validation 
FILE_INVALID_REASON = "invalid_reason"

#upload
UPLOAD_STATUS ="upload_status"

#Batch
BATCH_BUCKET = "bucketName"
FILE_PREFIX = "filePrefix" #bucket folders
BATCH_COLLECTION = "batch"
BATCH_ID = "batchID"
SUCCEEDED = "succeeded"
ERRORS = "errors"
WARNINGS = "warnings"
BATCH_CREATED = "createdAt"
BATCH_UPDATED = "updatedAt"
BATCH_STATUS= "status"
BATCH_STATUS_REJECTED = "rejected"
BATCH_STATUS_UPLOADED = "uploaded"
BATCH_STATUS_LOADED = "loaded"
SUBMISSION_COLLECTION="submissions"
BATCH_TYPE_METADATA ="metadata"
DATA_COMMON_NAME ="dataCommons"
BATCH_INTENTION = "metadataIntention"

# file 
FILE_ID = "dataRecordID"
S3_FILE_INFO = "s3FileInfo"
ID = "_id"
SIZE ="size"
MD5 = "md5"
DATA_COLlECTION = "dataRecords"
FILE_NAME = "fileName"
FILE_STATUS = "status"
STATUS_ERROR = "Error"
STATUS_WARNING = "Warning"
STATUS_PASSED = "Passed"
STATUS_NEW = "New"
SUBMISSION_ID = "submissionID"
NODE_ID = "nodeID"
NODE_TYPE = "nodeType"
S3_BUCKET_DIR = "s3_bucket_drive"

#data model
DATA_COMMON = "data_commons"
VERSION = "version"
MODEL_SOURCE = "source_files"
NAME_PROP = "name"
DESC_PROP = "description"
ID_PROPERTY = "id_property"
MODEL = "model"
VALUE_PROP = "value"
VALUE_EXCLUSIVE = "exclusive"
ALLOWED_VALUES = "permissible_values"
RELATION_LABEL = "label"
RELATION_PARENT_NODE ="parent_nodes"
NODE_LABEL = "node"
IDS = "id_fields"
NODE_PROPERTIES = "properties"
PROP_REQUIRED="required"
TYPE ="type"
MODELS_DEFINITION_FILE = "content.json"
TIER = "DEV_TIER"
TIER_CONFIG = "tier"
UPDATED_AT = "updatedAt"
FILE_SIZE = "file_size"

#s3 download directory
S3_DOWNLOAD_DIR = "s3_download"
# sqs message
SCOPE = "scope"

# validation status
PASSED = "Passed"
ERROR = "Error"
WARNING = "Warning"

