import argparse
import os
import yaml
from common.constants import MONGO_DB, SQS_NAME, DB, MODEL_FILE_DIR, SERVICE_TYPE_PV_PULLER,\
    LOADER_QUEUE, SERVICE_TYPE, SERVICE_TYPE_ESSENTIAL, SERVICE_TYPE_FILE, SERVICE_TYPE_METADATA, \
    SERVICE_TYPES, DB, FILE_QUEUE, METADATA_QUEUE, STS_API_ALL_URL_V2, TIER, TIER_CONFIG, SERVICE_TYPE_EXPORT, EXPORTER_QUEUE,\
    DM_BUCKET_CONFIG_NAME, PROD_BUCKET_CONFIG_NAME, DATASYNC_ROLE_ARN_CONFIG , DATASYNC_ROLE_ARN_ENV, CONFIG_TYPE, \
    CONFIG_KEY, DATASYNC_LOG_ARN_ENV, DATASYNC_LOG_ARN_CONFIG, STS_RESOURCE_CONFIG_TYPE,\
    STS_DATA_RESOURCE_CONFIG, STS_DUMP_CONFIG, STS_API_ALL_URL, STS_API_ONE_URL, STS_API_ONE_URL_V2
from bento.common.utils import get_logger
from common.utils import clean_up_key_value, get_exception_msg, load_message_config
from common.mongo_dao import MongoDao
DM_BUCKET_NAME_ENV = "DM_BUCKET_NAME"

class Config():
    def __init__(self):
        self.log = get_logger('Upload Config')
        self.mongodb_dao = None
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-t', '--service-type', type=str, choices=["essential", "file", "metadata", "export"], help='validation type, required')
        parser.add_argument('-s', '--server', help='Mongo database host, optional, it can be acquired from env.')
        parser.add_argument('-o', '--port', help='Mongo database port, optional, it can be acquired from env.')
        parser.add_argument('-u', '--user', help='Mongo database user id, optional, it can be acquired from env.')
        parser.add_argument('-p', '--pwd', help='Mongo database user password, optional, it can be acquired from env.')
        parser.add_argument('-d', '--db', help='Mongo database with batch collection, optional, it can be acquired from env.')
        parser.add_argument('-m', '--models-loc', help='metadata models local, only required for essential and metadata service types')
        parser.add_argument('-q', '--sqs', help='aws sqs name, optional, it can be acquired from env.')
        parser.add_argument('config', help='configuration file path, contains all above parameters, required')
       
        args = parser.parse_args()
        self.data = {}
        if args.config and os.path.isfile(args.config.strip()):
            with open(args.config.strip()) as c_file:
                self.data = yaml.safe_load(c_file)['Config']
        else: 
            self.log.critical(f'No configuration file is found!  Please check the file path: "{args.config}"')
            return

        self._override(args)

    def _override(self, args):
        for key, value in vars(args).items():
            # Ignore config file argument
            if key == 'config':
                continue
            if isinstance(value, bool):
                if value:
                    self.data[key] = value
            elif value is not None:
                self.data[key] = value

    def validate(self):
        if len(self.data)== 0:
            return False
        self.data = clean_up_key_value(self.data)
        service_type = self.data.get(SERVICE_TYPE)
        if service_type is None or service_type not in SERVICE_TYPES:
            self.log.critical(f'Service type is required and must be "essential", "file" or "metadata" or "export"!')
            return False
        
        db_server = self.data.get("server", os.environ.get("MONGO_DB_HOST"))
        db_port = self.data.get("port", os.environ.get("MONGO_DB_PORT"))
        db_user_id = self.data.get("user", os.environ.get("MONGO_DB_USER"))
        db_user_password = self.data.get("pwd", os.environ.get("MONGO_DB_PASSWORD"))
        db_name= self.data.get("db", os.environ.get("DATABASE_NAME"))
        if db_server is None or db_port is None or db_user_id is None or db_user_password is None \
            or db_name is None:
            self.log.critical(f'Missing Mongo BD setting(s)!')
            return False
        else:
            self.data[DB] = db_name
            self.data[MONGO_DB] = f"mongodb://{db_user_id}:{db_user_password}@{db_server}:{db_port}/?authMechanism=DEFAULT"
            self.mongodb_dao =  MongoDao(self.data[MONGO_DB], db_name)
        
        models_loc= self.data.get(MODEL_FILE_DIR)
        if models_loc is None and self.data[SERVICE_TYPE] not in [SERVICE_TYPE_FILE, SERVICE_TYPE_PV_PULLER]:
            self.log.critical(f'Metadata models location is required!')
            return False
        # get env configuration from DB
        env_vars = [TIER, LOADER_QUEUE, FILE_QUEUE, METADATA_QUEUE, EXPORTER_QUEUE, DM_BUCKET_NAME_ENV, DATASYNC_ROLE_ARN_ENV, DATASYNC_LOG_ARN_ENV, STS_RESOURCE_CONFIG_TYPE]
        try:
            configs_in_db = self.mongodb_dao.get_configuration_by_ev_var(env_vars) 
            if configs_in_db is None or len(configs_in_db) == 0:
                self.log.critical(f'Can not find configured environments variables in database!')
                return False

            #  try to get sqs setting from env configured in database.
            if not self.set_sqs(configs_in_db):
                return False
            
            config_in_db = next(val[CONFIG_KEY] for val in configs_in_db if val[CONFIG_TYPE] == TIER)
            tier =  config_in_db[TIER_CONFIG] if config_in_db and config_in_db.get(TIER_CONFIG) else self.data.get(TIER_CONFIG)
            if not tier and self.data[SERVICE_TYPE] not in [SERVICE_TYPE_FILE, SERVICE_TYPE_PV_PULLER]:
                self.log.critical(f'No tier is configured in both env and args!')
                return False
            else:
                self.data[TIER_CONFIG] = tier

            config_in_db = next(val[CONFIG_KEY] for val in configs_in_db if val[CONFIG_TYPE] == DM_BUCKET_NAME_ENV)
            dm_bucket = config_in_db[DM_BUCKET_CONFIG_NAME] if config_in_db and config_in_db.get(DM_BUCKET_CONFIG_NAME) else self.data.get(DM_BUCKET_CONFIG_NAME)
            if not dm_bucket and self.data[SERVICE_TYPE] in [SERVICE_TYPE_EXPORT]:
                self.log.critical(f'No data management bucket is configured in both env and args!')
                return False
            else:
                self.data[DM_BUCKET_CONFIG_NAME] = self.data[PROD_BUCKET_CONFIG_NAME] = dm_bucket

            config_in_db = next(val[CONFIG_KEY] for val in configs_in_db if val[CONFIG_TYPE] == DATASYNC_ROLE_ARN_ENV)
            datasync_role = config_in_db[DATASYNC_ROLE_ARN_CONFIG] if config_in_db and config_in_db.get(DATASYNC_ROLE_ARN_CONFIG) else self.data.get(DATASYNC_ROLE_ARN_CONFIG)
            config_in_db = next(val[CONFIG_KEY] for val in configs_in_db if val[CONFIG_TYPE] == DATASYNC_LOG_ARN_ENV)
            datasync_log_arn = config_in_db[DATASYNC_LOG_ARN_CONFIG] if config_in_db and config_in_db.get(DATASYNC_LOG_ARN_CONFIG) else self.data.get(DATASYNC_LOG_ARN_CONFIG)
            sts_resource = next(val[CONFIG_KEY] for val in configs_in_db if val[CONFIG_TYPE] == STS_RESOURCE_CONFIG_TYPE)
            if not (datasync_role or datasync_log_arn) and self.data[SERVICE_TYPE] == SERVICE_TYPE_EXPORT:
                self.log.critical(f'No datasync role is configured in both env and args!')
                return False
            else:
                self.data[DATASYNC_ROLE_ARN_CONFIG] = datasync_role
                self.data[DATASYNC_LOG_ARN_CONFIG] = datasync_log_arn

            if not sts_resource and self.data[SERVICE_TYPE] in [SERVICE_TYPE_METADATA, SERVICE_TYPE_PV_PULLER]:
                self.log.critical(f'No sts resource is configured in both env and args!')
                return False
            else:
                self.data[STS_DATA_RESOURCE_CONFIG] = sts_resource.get(STS_DATA_RESOURCE_CONFIG)
                self.data[STS_DUMP_CONFIG] = sts_resource.get(STS_DUMP_CONFIG)
                self.data[STS_API_ALL_URL] = sts_resource.get(STS_API_ALL_URL)
                self.data[STS_API_ONE_URL] = sts_resource.get(STS_API_ONE_URL)
                self.data[STS_API_ALL_URL_V2] = sts_resource.get(STS_API_ALL_URL_V2)
                self.data[STS_API_ONE_URL_V2] = sts_resource.get(STS_API_ONE_URL_V2)
            # load configured customized message to memory
            if self.data[SERVICE_TYPE] in [SERVICE_TYPE_METADATA, SERVICE_TYPE_FILE]:
                message_config = load_message_config()
                if not message_config:
                    self.log.critical(f'Failed to load message config!')
                    return False
            return True
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f'Error occurred set configurations: {get_exception_msg()}')
            return False
    
    def set_sqs(self, configs_in_db):
        if self.data[SERVICE_TYPE] in [SERVICE_TYPE_PV_PULLER]: 
            return True
        sqs_name = None
        if self.data[SERVICE_TYPE] == SERVICE_TYPE_ESSENTIAL:
            sqs_name = LOADER_QUEUE
        elif self.data[SERVICE_TYPE] == SERVICE_TYPE_FILE:
            sqs_name = FILE_QUEUE
        elif self.data[SERVICE_TYPE] == SERVICE_TYPE_METADATA:
            sqs_name = METADATA_QUEUE
        elif self.data[SERVICE_TYPE] == SERVICE_TYPE_EXPORT:
            sqs_name = EXPORTER_QUEUE

        env_config = next(val[CONFIG_KEY] for val in configs_in_db if val[CONFIG_TYPE] == sqs_name)
        # if no env saved in database for sqs, check config/arg
        sqs = env_config.get(SQS_NAME, self.data.get(SQS_NAME)) 

        if not sqs and self.data[SERVICE_TYPE] not in [SERVICE_TYPE_PV_PULLER]:
            self.log.critical(f'AWS sqs name is required!')
            return False
        else:
            self.data[SQS_NAME] = sqs
            return True

