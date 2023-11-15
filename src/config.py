import argparse
import os
import yaml
from common.constants import MONGO_DB, SQS_NAME, RETRIES, DB, MODEL_FILE_DIR, \
    LOADER_QUEUE, SERVICE_TYPE, SERVICE_TYPE_ESSENTIAL, SERVICE_TYPE_FILE, SERVICE_TYPE_METADATA, \
        SERVICE_TYPES, DB, FILE_QUEUE, METADATA_QUEUE, TIER
from bento.common.utils import get_logger
from common.utils import clean_up_key_value

class Config():
    def __init__(self):
        self.log = get_logger('Upload Config')
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-s', '--service-type', type=str, choices=["essential", "file", "metadata"], help='validation type, required')
        parser.add_argument('-c', '--mongo', help='Mongo database connection string, required')
        parser.add_argument('-d', '--db', help='Mongo database with batch collection, required')
        parser.add_argument('-p', '--models-loc', help='metadata models local, required')
        parser.add_argument('-t', '--tier', help='current tier, optional')
        parser.add_argument('-q', '--sqs', help='aws sqs name, required')
        parser.add_argument('-r', '--retries', help='db connection, data loading, default value is 3, optional')
        
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
        
        mongo = self.data.get(MONGO_DB)
        if mongo is None:
            self.log.critical(f'Mongo DB connection string is required!')
            return False
        
        db = self.data.get(DB)
        if db is None:
            self.log.critical(f'Mongo DB for batch is required!')
            return False
        
        models_loc= self.data.get(MODEL_FILE_DIR)
        if models_loc is None:
            self.log.critical(f'Metadata models location is required!')
            return False
        if self.data[SERVICE_TYPE] == SERVICE_TYPE_ESSENTIAL:
            sqs = os.environ.get(LOADER_QUEUE)
        elif self.data[SERVICE_TYPE] == SERVICE_TYPE_FILE:
            sqs = os.environ.get(FILE_QUEUE)
        elif self.data[SERVICE_TYPE] == SERVICE_TYPE_METADATA:
            sqs = os.environ.get(METADATA_QUEUE)
        else:
            sqs = None

        tier = os.environ.get(TIER, self.data.get("tier"))
        if not tier:
            self.log.critical(f'No tier is configured in both env and args!')
            return False
        else:
            self.data["tier"] = tier
            
        if not sqs:
            #env LOADER_QUEUE
            sqs = self.data.get(SQS_NAME)
            if not sqs:
                self.log.critical(f'AWS sqs name is required!')
                return False
            else:
                self.data[SQS_NAME] = sqs
            

        retry = self.data.get(RETRIES, 3) #default value is 3
        if isinstance(retry, str):
            if not retry.isdigit():
                self.log.critical(f'retries is not integer!')
                return False
            else:
                self.data[RETRIES] =int(retry) 
        else:
            self.data[RETRIES] =int(retry) 

  
        return True

