from common.constants import IDS, NODES_LABEL, MODEL, RELATIONSHIPS

class DataModel:
    def __init__(self, model):
        self.model = model

     # model connivent functions
    """
    get model id fields in the given model
    """
    def get_model_ids(self):       
            return self.model.get(IDS, None)

    """
    get id field of a given node in the model
    """   
    def get_node_id(self, node): 
        return self.model[MODEL][NODES_LABEL][node].get("id_property", None)

    """
    get all node keys in the model
    """
    def get_node_keys(self):
        return self.model[MODEL][NODES_LABEL].keys()

    """
    get properties of a node in the model
    """
    def get_node_props(self, node):
        if self.model[MODEL][NODES_LABEL].get(node):
            return self.model[MODEL][NODES_LABEL][node].get("properties", None)

    """
    get relationships of a node in the model
    """
    def get_node_relationships(self, node):
        if self.model[MODEL][NODES_LABEL].get(node):
            return self.model[MODEL][NODES_LABEL][node].get(RELATIONSHIPS, None)
        
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
        return self.model[MODEL].get("file-nodes", {})