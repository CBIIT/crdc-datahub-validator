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
BATCH_CREATED = "createdAt"
BATCH_UPDATED = "updatedAt"
BATCH_STATUS= "status"
BATCH_STATUS_REJECTED = "rejected"
BATCH_STATUS_UPLOADED = "uploaded"
BATCH_STATUS_LOADED = "loaded"
SUBMISSION_COLLECTION="submissions"
BATCH_TYPE_METADATA ="metadata"
DATA_COMMON_NAME ="dataCommons"


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
TIER="DEV_TIER"

#s3 download directory
S3_DOWNLOAD_DIR = "s3_download"

DATA_COLlECTION = "DataRecords"



