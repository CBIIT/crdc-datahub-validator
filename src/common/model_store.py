import os
import json
import glob
from bento.common.utils import get_logger
from common.model_reader import Model
from common.constants import DATA_COMMON, IDS, MODEL_FILE_DIR, MODELS_DEFINITION_FILE, TIER, MODEL
from common.utils import download_file_to_dict


YML_FILE_EXT = ".yml"
DEF_VERSION = "current-version"
DEF_MODEL_FILE = "model-file"
DEF_MODEL_PROP_FILE = "prop-file"
MODE_ID_FIELDS = "id_fields"

class ModelFactory:
    
    def __init__(self, configs):
        self.log = get_logger('Models')
        self.models = []
        msg = None
        # get models definition file, content.json in models dir
        models_def_file_path = os.path.join(configs[MODEL_FILE_DIR], MODELS_DEFINITION_FILE)
        tier = os.environ.get(TIER)
        if tier:
            models_def_file_path = models_def_file_path.replace("/dev", f"/{tier}")
        self.models_def = download_file_to_dict(models_def_file_path)

        # to do check if  self.models_def is a dict
        if not isinstance(self.models_def, dict):
            msg = f'Invalid models definition at "{models_def_file_path}"!'
            self.log.error(msg)
            raise Exception(msg)
        
        for k, v in self.models_def.items():
            data_common = k
            version = v[DEF_VERSION]
            model_dir = os.path.join(configs[MODEL_FILE_DIR], os.path.join(k, version))
            #process model files for the data common
            file_name= os.path.join(model_dir, v[DEF_MODEL_FILE])
            props_file_name = os.path.join(model_dir, v[DEF_MODEL_PROP_FILE])
            model_reader = Model([file_name, props_file_name], data_common)
            self.models.append({MODEL: model_reader.model, IDS: model_reader.id_fields})
            
    def get_model_by_data_common(self, data_common):
        return next((x for x in self.models if x['model'][DATA_COMMON] == data_common.upper()), None)
        
    def get_model_ids_by_data_common(self, data_common):       
        model = self.get_model_by_data_common(data_common)
        return model[IDS] if model else None
    
    def get_node_id(self, data_common, node):  
        ids = self.get_model_ids_by_data_common(data_common)
        result = next((x for x in ids if x['node'] == node), None)
        return result["key"] if result else None