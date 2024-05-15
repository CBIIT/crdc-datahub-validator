import os
from bento.common.utils import get_logger
from common.model_reader import YamlModelParser
from common.model import DataModel
from common.constants import MODELS_DEFINITION_FILE, LIST_DELIMITER_PROP
from common.utils import download_file_to_dict, get_exception_msg


YML_FILE_EXT = ".yml"
DEF_MODEL_FILE = "model-file"
DEF_MODEL_PROP_FILE = "prop-file"
DEF_VERSION = "current-version"
MODE_ID_FIELDS = "id_fields"
DEF_SEMANTICS = "semantics"
DEF_FILE_NODES = "file-nodes"

class ModelFactory:
    
    def __init__(self, model_def_loc, tier):
        self.log = get_logger('Models')
        self.models = None
        msg = None
        # get models definition file, content.json in models dir
        self.model_def_dir = os.path.join(model_def_loc, tier)
        models_def_file_path = os.path.join(self.model_def_dir, MODELS_DEFINITION_FILE)
        self.models_def = download_file_to_dict(models_def_file_path)
        # to do check if  self.models_def is a dict
        if not isinstance(self.models_def, dict):
            msg = f'Invalid models definition at "{models_def_file_path}"!'
            self.log.error(msg)
            raise Exception(msg)
        self.models = {}
        for k, v in self.models_def.items():
            data_common = k
            version = v[DEF_VERSION]
            self.create_model(data_common, version)

    """
    create a data model dict by parsing yaml model files
    """
    def create_model(self, data_common, version):
        dc = data_common.upper()
        v = self.models_def[dc]
        model_dir = os.path.join(self.model_def_dir, os.path.join(dc, version))
        #process model files for the data common
        file_name= os.path.join(model_dir, v[DEF_MODEL_FILE])
        props_file_name = os.path.join(model_dir, v[DEF_MODEL_PROP_FILE])
        delimiter = v.get(LIST_DELIMITER_PROP)
        #process model files for the data common
        model_reader = YamlModelParser([file_name, props_file_name], dc, delimiter, version)
        model_reader.model.update({DEF_FILE_NODES: v[DEF_SEMANTICS][DEF_FILE_NODES]})
        self.models.update({model_key(dc, version): model_reader.model})

    """
    get model by data common
    """       
    def get_model_by_data_common(self, data_common):
        dc = data_common.upper()
        v = self.models_def[dc]
        version = v[DEF_VERSION]
        model = self.models.get(model_key(dc, version))
        return DataModel(model)
    
    """
    get model by data common and version
    """       
    def get_model_by_data_common_version(self, data_common, version):
        if version:
            model = self.models.get(model_key(data_common, version))
            if not model:
                try:
                    self.create_model(data_common, version)
                    model = self.models.get(model_key(data_common, version))
                except Exception as e:
                    self.log.exception(e)
                    msg = f"Failed to create data model: {data_common}/{version}!"
                    self.log.exception(f"{msg} {get_exception_msg()}")
            return DataModel(model)   
        else:
            return self.get_model_by_data_common(data_common)
        
def model_key(data_common, version):
    return f"{data_common.upper()}_{version}"
        
       
        

   