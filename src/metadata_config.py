import argparse
import os
import yaml
from common.constants import UPLOAD_TYPE, INTENTION, INTENTIONS,SUBMISSION_ID, FILE_DIR, UPLOAD_TYPES
from common.utils import clean_up_key_value


#requirements of the ticket CRDCDH-13:
UPLOAD_HELP = """
Command line arguments / configuration
-u --submission, submission ID, required
-t --type, valid value in [“file”, “metadata”], required
-d --data, folder that contains either data files (type = “file”) or metadata (TSV/TXT) files (type = “metadata”), required
-c --config, configuration file path, can potentially contain all above parameters, preferred

Following argument is needed when type = "metadata"

-i --intention, valid value in [“New”, “Update”, “Delete”], conditional required when type = “metadata”, default to “new”
CLI Argument and configuration module will validate and combine parameters from CLI and/or config file
If config_file is given, then everything else is potentially optional (if it’s included in config file)
Some arguments are only needed for type = “file” or type = “metadata”, e.g., —intention, —manifest
"""

class Config():
    def __init__(self):
        # self.log = get_logger('Upload Config')
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-u', '--submission', help='submission ID, required')
        parser.add_argument('-t', '--type', help='valid value in [“file”, “metadata”], required')
        parser.add_argument('-b', '--batch', help='batch ID, required')
        parser.add_argument('-i', '--intention', help='valid value in [“New”, “Update”, “Delete”]')
        parser.add_argument('-m', '--mongo', help='Mongo database connection string, required')
        parser.add_argument('-d', '--data', help='folder that contains either data files (type = “file”) or metadata (TSV/TXT) files (type = “metadata”), required')
       
        parser.add_argument('-c', '--config', help='configuration file, can potentially contain all above parameters, optional')


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

        
        submissionId = self.data.get(SUBMISSION_ID)
        if submissionId is None:
            self.log.critical(f'submission Id is required!')
            return False

        type = self.data.get(UPLOAD_TYPE)
        if type is None:
            self.log.critical(f'upload type is required!')
            return False
        elif type not in UPLOAD_TYPES:
            self.log.critical(f'{type} is not valid uploading type!')
            return False
        else:
            if type == UPLOAD_TYPES[0]: #file
                pass

            elif type == UPLOAD_TYPES[1]: #metadata
                self.data[INTENTION] = INTENTIONS[0] #New
        
                intention = self.data.get(INTENTION)
                if intention is None:
                    self.log.critical(f'intention is required for metadata uploading!')
                    return False
                elif intention not in INTENTIONS:
                    self.log.critical(f'{intention} is not a valid intention!')
                    return False
        
        filepath = self.data.get(FILE_DIR)
        if filepath is None:
            self.log.critical(f'data file path is required!')
            return False
        else:
            filepath = filepath
            self.data[FILE_DIR]  = filepath
            if not os.path.isdir(filepath): 
                self.log.critical(f'data file path is not valid!')
                return False
  
        return True

