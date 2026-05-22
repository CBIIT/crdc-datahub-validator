from common.constants import NODES_LABEL, RELATIONSHIPS, LIST_DELIMITER_PROP, DEF_MAIN_NODES, PROPERTY_NAMES, OMIT_DCF_PREFIX, COMPOSITION_KEY

class DataModel:
    def __init__(self, model):
        self.model = model

    # model connivent functions
    # """
    # get model id fields in the given model
    # """
    # def get_model_ids(self):       
    #         return self.model.get(IDS, None)
    """
    get nodes value
    """   
    def get_nodes(self): 
        return self.model.get(NODES_LABEL, {})
    """
    get id field of a given node in the model
    """   
    def get_node_id(self, node): 
        if self.model[NODES_LABEL].get(node):
            return self.model[NODES_LABEL][node].get("id_property", None)
        return None

    """
    get all node keys in the model
    """
    def get_node_keys(self):
        return self.model[NODES_LABEL].keys()

    """
    get properties of a node in the model
    """
    def get_node_props(self, node):
        if self.model[NODES_LABEL].get(node):
            return self.model[NODES_LABEL][node].get("properties", None)

    """
    get relationships of a node in the model
    """
    def get_node_relationships(self, node):
        if self.model[NODES_LABEL].get(node):
            return self.model[NODES_LABEL][node].get(RELATIONSHIPS, None)
        
    """
    get required properties of a node in the model
    """
    def get_node_req_props(self, node):
        props = self.get_node_props(node)
        if not props:
            return None
        return {k: v for (k, v) in props.items() if v.get("required") == True}
    
    """
    get file nodes in the model
    """
    def get_file_nodes(self):
        return self.model.get("file-nodes", {})
    
    """
    get main nodes in the model
    """
    def get_main_nodes(self):
        return self.model.get(DEF_MAIN_NODES, {})
    
    def get_entity_type(self, node_type):
        return self.model.get(DEF_MAIN_NODES, {}).get(node_type, None)
    
    """
    get entity type of a given node in the model
    """
    def get_entity_type(self, node_type):
        return self.model.get(DEF_MAIN_NODES, {}).get(node_type, None)
    
    """
    get configured property names of a given node's property name in the model
    """
    def get_configured_prop_name(self, prop_name):
        return self.model.get(PROPERTY_NAMES, {}).get(prop_name, None)
    
    """
    get file name property
    """
    def get_file_name(self):
        file_nodes_dict =  self.model.get("file-nodes", {})
        file_nodes_vals = list(file_nodes_dict.values())
        return file_nodes_vals[0]["name-field"] if len(file_nodes_vals) > 0 else None
    
    """
    get list delimiter
    """
    def get_list_delimiter(self):
        return self.model.get(LIST_DELIMITER_PROP)
    
    """
    get dcf prefix
    """
    def get_omit_dcf_prefix(self):
        return self.model.get(OMIT_DCF_PREFIX, False)
    
    """
    get composition key
    """
    def get_composition_key(self, node):
        return self.model[NODES_LABEL][node].get(COMPOSITION_KEY, None)

    def get_model_version(self):
        return self.model.get("version", None)
    
    def get_data_commons(self):
        return self.model.get("data_commons", None)


    

    
    
    