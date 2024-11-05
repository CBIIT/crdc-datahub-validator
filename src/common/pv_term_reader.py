import os
from bento.common.utils import get_logger
from common.constants import MODELS_DEFINITION_FILE, CDE_TERM
from common.utils import download_file_to_dict, get_exception_msg


YML_FILE_EXT = ["yml", "yaml"]
DEF_MODEL_FILE = "model-file"
DEF_MODEL_FILES = "model-files"
DEF_MODEL_PROP_FILE = "prop-file"
DEF_VERSION = "versions"
MODE_ID_FIELDS = "id_fields"
PROP_DEFINITIONS = 'PropDefinitions'
PROP_TYPE = 'Type'
PROP_ENUM = 'Enum'
ITEM_TYPE = 'item_type'
VALUE_TYPE = 'value_type'
prop_list_types = [
    "value-list", # value_type: value type: list
            # a string with comma ',' characters as deliminator, e.g, "value1,value2,value3", represents a value list value1,value2,value3
    "list" # value_type: list
            # a string with asterisk '*' characters as deliminator, e.g, "value1*value2+value3", represents a array [value1, value2, value3]
]

class TermReader:
    
    def __init__(self, model_def_loc, tier):
        self.log = get_logger('CDE')
        self.cdes = None
        self.models_props = FileNotFoundError
        msg = None
        # get models definition file, content.json in models dir
        self.model_def_dir = os.path.join(model_def_loc, tier + "/cache")
        models_def_file_path = os.path.join(self.model_def_dir, MODELS_DEFINITION_FILE)
        self.models_def = download_file_to_dict(models_def_file_path)
        # to do check if  self.models_def is a dict
        if not isinstance(self.models_def, dict):
            msg = f'Invalid models definition at "{models_def_file_path}"!'
            self.log.error(msg)
            raise Exception(msg)
        self.cdes = {}
        for k, v in self.models_def.items():
            data_common = k
            version = v["current-version"]
            self.create_cde(data_common, version)

    """
    create a CDE term dict by parsing yaml model property file
    """
    def create_cde(self, data_common, version):
        dc = data_common
        v = self.models_def[dc]
        model_dir = os.path.join(self.model_def_dir, os.path.join(dc, version))
        #process model files for the data common
        model_file_names = [os.path.join(model_dir, file) for file in v[DEF_MODEL_FILES]]
        #process model files for the data common
        try:
            result, properties_term, msg = self.parse_model_props(model_file_names)
            if not result:
                self.log.error(msg)
                return
            self.cdes[dc] = properties_term
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to create data model: {data_common}/{version}!"
            self.log.exception(f"{msg} {get_exception_msg()}")

    """
    parse model property file
    """
    def parse_model_props(self, model_file_names):
        properties = {}
        permissive_value_dic = {}
        values = None
        msg = None
        try:
            for model_file_name in model_file_names:
                self.log.info('Reading properties file: {} ...'.format(model_file_name))
                if model_file_name and '.' in model_file_name and model_file_name.split('.')[-1].lower() in YML_FILE_EXT:
                    prop = download_file_to_dict(model_file_name).get(PROP_DEFINITIONS, None)
                    if prop:
                        properties.update(prop)
                    
            if not properties or len(properties.keys()) == 0:
                msg = f'Invalid model properties file: {model_file_names}!'
                self.log.error(msg)
                return False, None, msg
            
        except Exception as e:
            self.log.exception(e)
            msg = f'Failed to read yaml file to dict: {model_file_names}!'
            self.log.exception(msg)
            raise e
        
        # filter properties with enum and value list
        for prop_name, prop in properties.items():
            values = prop.get(CDE_TERM)
            if values and len(values) > 0:
                terms = [term for term in values if 'caDSR' in term.get('Origin', '')]
                if terms and len(terms) > 0:
                    permissive_value_dic[prop_name] = terms[0]
            else: 
                msg = f'No term for the property: {prop_name}!'
        return True, permissive_value_dic, msg




   
    