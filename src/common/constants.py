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
SERVICE_TYPE_EXPORT = "export"
SERVICE_TYPES = [SERVICE_TYPE_ESSENTIAL, SERVICE_TYPE_FILE, SERVICE_TYPE_METADATA, SERVICE_TYPE_EXPORT]

MODEL_FILE_DIR = "models-loc"
LOADER_QUEUE = "LOADER_QUEUE"
FILE_QUEUE = "FILE_QUEUE"
METADATA_QUEUE = "METADATA_QUEUE"
EXPORTER_QUEUE = "EXPORTER_QUEUE"
#file validation 
FILE_INVALID_REASON = "invalid_reason"

#upload
UPLOAD_STATUS ="upload_status"
RELEASE = "release"
ARCHIVE_RELEASE = "archive_release"
#Batch
BATCH_BUCKET = "bucketName"
FILE_PREFIX = "filePrefix"
BATCH_COLLECTION = "batch"
BATCH_ID = "batchID"
SUCCEEDED = "succeeded"
ERRORS = "errors"
WARNINGS = "warnings"
CREATED_AT = "createdAt"
UPDATED_AT = "updatedAt"
STATUS= "status"
BATCH_STATUS_FAILED = "Failed"
BATCH_TYPE_METADATA ="metadata"
BATCH_STATUS_UPLOADED = "Uploaded"
SUBMISSION_COLLECTION="submissions"
DATA_COMMON_NAME ="dataCommons"
BATCH_INTENTION = "metadataIntention"
INTENTION_NEW = "New"
INTENTION_DELETE = "Delete"
INTENTION_UPDATE = "Update"

# export
EXPORT_METADATA = "metadata"
EXPORT_ROOT_PATH = "rootPath"

# file
FILE_ID = "dataRecordID"
S3_FILE_INFO = "s3FileInfo"
ID = "_id"
SIZE ="size"
MD5 = "md5"
DATA_COLlECTION = "dataRecords"
FILE_NAME = "fileName"
STATUS_ERROR = "Error"
STATUS_WARNING = "Warning"
STATUS_PASSED = "Passed"
STATUS_NEW = "New"
SUBMISSION_ID = "submissionID"
NODE_ID = "nodeID"
NODE_TYPE = "nodeType"
S3_BUCKET_DIR = "s3_bucket_drive"
FILE_ERRORS = "fileErrors"
PROPERTIES = "props"
PARENTS = "parents"
PARENT_TYPE = "parentType"
PARENT_ID_VAL = "parentIDValue"
RAW_DATA = "rawData"

FILE_NAME_FIELD = "name-field"
FILE_SIZE_FIELD = "size-field"
FILE_MD5_FIELD = "md5-field"
FILE_MD5_COLLECTION = "fileMD5"
LAST_MODIFIED = "LastModified"

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
NODES_LABEL = "nodes"
IDS = "id_fields"
RELATIONSHIPS = "relationships"
NODE_PROPERTIES = "properties"
PROP_REQUIRED="required"
TYPE ="type"
MODELS_DEFINITION_FILE = "content.json"
TIER = "TIER"
TIER_CONFIG = "tier"
FILE_SIZE = "file_size"
MIN = 'minimum'
MAX = 'maximum'
VALIDATION_RESULT = "result"
VALIDATED_AT = "validatedAt"
FILE_VALIDATION_STATUS = "fileValidationStatus"
METADATA_VALIDATION_STATUS = "metadataValidationStatus"
MODEL_VERSION = "modelVersion"
KEY = "key"
#s3 download directory
S3_DOWNLOAD_DIR = "s3_download"
ROOT_PATH = "rootPath"
# sqs message
SCOPE = "scope"
SQS_TYPE = "type"
TYPE_LOAD = "Load Metadata"
TYPE_METADATA_VALIDATE = "Validate Metadata"
TYPE_FILE_VALIDATE = "Validate Single File"
TYPE_FILE_VALIDATE_ALL = "Validate Submission Files"
TYPE_EXPORT_METADATA = "Export Metadata"
FAILED = "Failed"

#dataRecords
CRDC_ID = "CRDC_ID"
RELEASE_COLLECTION = "release"






