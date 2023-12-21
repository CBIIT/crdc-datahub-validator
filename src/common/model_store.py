import os
from bento.common.utils import get_logger
from common.model_reader import YamlModelParser
from common.model import DataModel
from common.constants import DATA_COMMON, IDS, MODELS_DEFINITION_FILE, MODEL
from common.utils import download_file_to_dict


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
        self.models = []
        msg = None
        # get models definition file, content.json in models dir
        model_def_dir = os.path.join(model_def_loc, tier)
        models_def_file_path = os.path.join(model_def_dir, MODELS_DEFINITION_FILE)
        self.models_def = download_file_to_dict(models_def_file_path)
        # to do check if  self.models_def is a dict
        if not isinstance(self.models_def, dict):
            msg = f'Invalid models definition at "{models_def_file_path}"!'
            self.log.error(msg)
            raise Exception(msg)
        
        for k, v in self.models_def.items():
            data_common = k
            version = v[DEF_VERSION]
            model_dir = os.path.join(model_def_dir, os.path.join(k, version))
            #process model files for the data common
            file_name= os.path.join(model_dir, v[DEF_MODEL_FILE])
            props_file_name = os.path.join(model_dir, v[DEF_MODEL_PROP_FILE])

            # call yml parser to generate model in dict
            model_reader = YamlModelParser([file_name, props_file_name], data_common, version)
            model_reader.model.update({DEF_FILE_NODES: v[DEF_SEMANTICS][DEF_FILE_NODES]})
            self.models.append({MODEL: model_reader.model, IDS: model_reader.id_fields})




    """
    get model by data common
    """       
    def get_model_by_data_common(self, data_common):
        model = next((x for x in self.models if x[MODEL][DATA_COMMON] == data_common.upper()), None)
        return DataModel(model)
        

   