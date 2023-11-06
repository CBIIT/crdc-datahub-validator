import argparse
import os
import yaml
from common.constants import MONGO_DB, SQS_NAME, RETRIES, BATCH_DB, METADATA_DB, MODEL_FILE_DIR, \
    LOADER_QUEUE
from bento.common.utils import get_logger
from common.utils import clean_up_key_value


#requirements of the ticket CRDCDH-13:
VALIDATE_HELP = """
Command line arguments / configuration
-d --mongo, Mongo database connection string, required
-b --batch-db, Mongo database with batch collection, required
-m --matadata-db, Mongo database to load metadata, required
-p --models-dir, metadata model file directory, required
-q --sqs, aws sqs name, required
-r --retries, db connection, data loading, default value is 3
config, configuration file path, contains all above parameters, required

If config_file is given, then everything else is potentially optional (if it’s included in config file)
Some arguments are only needed for type = “file” or type = “metadata”, e.g., —intention, —manifest
"""

class Config():
    def __init__(self):
        self.log = get_logger('Upload Config')
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-d', '--mongo', help='Mongo database connection string, required')
        parser.add_argument('-b', '--batch-db', help='Mongo database with batch collection, required')
        parser.add_argument('-m', '--matadata-db', help='Mongo database to load metadata, required')
        parser.add_argument('-p', '--models-dir', help='metadata model file directory, required')
        parser.add_argument('-q', '--sts', help='aws sqs name, required')
        parser.add_argument('-r', '--retries', help='db connection, data loading, default value is 3, optional')
        parser.add_argument('config', help='configuration file path, contains all above parameters, required')
       
        args = parser.parse_args()
        self.data = {}
        if args.config and os.path.isfile(args.config.strip()):
            with open(args.config.strip()) as c_file:
                self.data = yaml.safe_load(c_file)['Config']

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
        
        mongo = self.data.get(MONGO_DB)
        if mongo is None:
            self.log.critical(f'Mongo DB connection string is required!')
            return False
        
        batch_db = self.data.get(BATCH_DB)
        if batch_db is None:
            self.log.critical(f'Mongo DB for batch is required!')
            return False
        
        matadata_db = self.data.get(METADATA_DB)
        if matadata_db is None:
            self.log.critical(f'Mongo DB connection string is required!')
            return False
        
        models_dir = self.data.get(MODEL_FILE_DIR)
        if models_dir is None:
            self.log.critical(f'Metadata model files dir is required!')
            return False
        
        sqs = os.environ.get(LOADER_QUEUE)
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

