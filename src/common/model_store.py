import os
import glob
from bento.common.utils import get_logger
from common.model_reader import Model
from common.constants import DATA_COMMON, IDS, MODEL_FILE_DIR


YML_FILE_EXT = ".yml"

class ModelFactory:
    
    def __init__(self, configs):
        self.log = get_logger('Models')
        self.models = []
        # to do get props files
        model_files = glob.glob('{}/*model.yml'.format(configs[MODEL_FILE_DIR]))
        for file_name in model_files:
            if file_name and os.path.isfile(file_name):
                data_common = os.path.basename(file_name).split("-")[0].upper()
                # populate model properties
                props_file_name = f'{file_name.replace(YML_FILE_EXT, "")}-props{YML_FILE_EXT}'
                props =[]
                if not os.path.isfile(props_file_name):
                    msg = f'Can NOT open model properties file: "{props_file_name}"'
                    self.log.error(msg)
                    raise Exception(msg)
                model_reader = Model([file_name, props_file_name], data_common)
                self.models.append({"model": model_reader.model, "id_fields": model_reader.id_fields})
            else:
                msg = f'Can NOT open model file: "{file_name}"'
                self.log.error(msg)
                raise Exception(msg)
            
    def get_model_by_data_common(self, data_common):
        return next((x for x in self.models if x['model'][DATA_COMMON] == data_common.upper()), None)
        
    def get_model_ids_by_data_common(self, data_common):       
        model = self.get_model_by_data_common(data_common)
        return model[IDS] if model else None
    
    def get_node_id(self, data_common, node):  
        ids = self.get_model_ids_by_data_common(data_common)
        result = next((x for x in ids if x['node'] == node), None)
        return result["key"] if result else None