import argparse
import os
import yaml
from common.constants import MONGO_DB, SQS_NAME, DB, MODEL_FILE_DIR, \
    LOADER_QUEUE, SERVICE_TYPE, SERVICE_TYPE_ESSENTIAL, SERVICE_TYPE_FILE, SERVICE_TYPE_METADATA, \
    SERVICE_TYPES, DB, FILE_QUEUE, METADATA_QUEUE, TIER, TIER_CONFIG, SERVICE_TYPE_EXPORT
from bento.common.utils import get_logger
from common.utils import clean_up_key_value

class Config():
    def __init__(self):
        self.log = get_logger('Upload Config')
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-t', '--service-type', type=str, choices=["essential", "file", "metadata"], help='validation type, required')
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
            return None
        
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
            self.log.critical(f'Service type is required and must be "essential", "file" or "metadata"!')
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

        models_loc= self.data.get(MODEL_FILE_DIR)
        if models_loc is None and self.data[SERVICE_TYPE] != SERVICE_TYPE_FILE and self.data[SERVICE_TYPE] != SERVICE_TYPE_EXPORT:
            self.log.critical(f'Metadata models location is required!')
            return False
        
        #  try to get sqs setting from env.
        if self.data[SERVICE_TYPE] == SERVICE_TYPE_ESSENTIAL:
            sqs = os.environ.get(LOADER_QUEUE, self.data.get(SQS_NAME))
        elif self.data[SERVICE_TYPE] == SERVICE_TYPE_FILE:
            sqs = os.environ.get(FILE_QUEUE, self.data.get(SQS_NAME))
        elif self.data[SERVICE_TYPE] in [SERVICE_TYPE_METADATA, SERVICE_TYPE_EXPORT]:
            sqs = os.environ.get(METADATA_QUEUE, self.data.get(SQS_NAME))
        else:
            sqs = None
        
        # if no env set got sqs, check config/arg
        if not sqs:
            self.log.critical(f'AWS sqs name is required!')
            return False
        else:
                self.data[SQS_NAME] = sqs

        tier = os.environ.get(TIER, self.data.get(TIER_CONFIG))
        if not tier and self.data[SERVICE_TYPE] not in [SERVICE_TYPE_FILE, SERVICE_TYPE_EXPORT]:
            self.log.critical(f'No tier is configured in both env and args!')
            return False
        else:
            self.data[TIER_CONFIG] = tier

        return True

