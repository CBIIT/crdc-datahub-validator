import os
import re
from bento.common.utils import get_logger, MULTIPLIER, DEFAULT_MULTIPLIER
from common.constants import DATA_COMMON, VERSION, MODEL_SOURCE, NAME_PROP, DESC_PROP, ID_PROPERTY, VALUE_PROP, \
    VALUE_EXCLUSIVE, ALLOWED_VALUES, RELATION_LABEL, TYPE, NODE_LABEL, NODE_PROPERTIES, PROP_REQUIRED
from common.utils import download_file_to_dict, case_insensitive_get

NODES = 'Nodes'
RELATIONSHIPS = 'Relationships'
PROPERTIES = 'Props'
PROP_DEFINITIONS = 'PropDefinitions'
DEFAULT_TYPE = 'string'
PROP_TYPE = 'Type'
PROP_ENUM = 'Enum'
END_POINTS = 'Ends'
SRC = 'Src'
DEST = 'Dst'
VALUE_TYPE = 'value_type'
ITEM_TYPE = 'item_type'
LIST_DELIMITER = '*'
LABEL_NEXT = 'next'
NEXT_RELATIONSHIP = 'next'
UNITS = 'units'
REQUIRED = 'Req'
KEY ='key'
DESCRIPTION = "desc"
PRIVATE = 'Private'
NODE_TYPE = 'type'
ENUM = 'enum'
DEFAULT_VALUE = 'default_value'
HAS_UNIT = 'has_unit'
MIN = 'minimum'
MAX = 'maximum'
EX_MIN = 'exclusiveMinimum'
EX_MAX = 'exclusiveMaximum'
RELATION_DELIMITER = 'S'
DEFAULT_VERSION = "1.0.0"
DEFAULT_DESC = ""


valid_prop_types = [
    "string", # default type
    "integer",
    "number", # float or double
    "datetime",
    "date",
    "boolean", # true/false or yes/no
    "array" # value_type: list
            # a string with asterisk '*' characters as deliminator, e.g, "value1 * value2 * value3", represents a array [value1, value2, value3]
]

valid_relationship_types = ["many_to_one", "one_to_one", "many_to_many"]


def get_list_values(list_str):
    return [item.strip() for item in list_str.split(LIST_DELIMITER) if item.strip()]


def is_parent_pointer(field_name):
    return re.fullmatch(r'\w+\.\w+', field_name) is not None


class Model:
    def __init__(self, yaml_files, data_common, version=DEFAULT_VERSION):
        self.log = get_logger('Model Reader')
        # initialize the data model
        self.model = {DATA_COMMON: data_common, VERSION: version}
        model_file_src = []
        self.id_fields = []
        self.schema = {}
        for aFile in yaml_files:
            try:
                self.log.info('Reading model file: {} ...'.format(aFile))
                if aFile and '.' in aFile and aFile.split('.')[-1].lower() == "yml":
                    model_file_src.append(os.path.basename(aFile))
                    schema = download_file_to_dict(aFile)
                    if schema:
                        self.schema.update(schema)
            except Exception as e:
                self.log.debug(e)
                msg = f'Failed to read yaml file to dict: {aFile}!'
                self.log.exception(msg)
                raise

        self.model.update({MODEL_SOURCE: model_file_src})
        self.nodes = {}
        self.relationships = {}
        self.relationship_props = {}

        # check if model file contents are valid
        if NODES not in self.schema:
            msg = f'Can\'t load any nodes: "{yaml_files}"!'
            self.log.error(msg)
            raise Exception(msg)
        elif PROP_DEFINITIONS not in self.schema:
            msg = f'Can\'t load any properties: "{yaml_files}"!'
            self.log.error(msg)
            raise Exception(msg)
        
        self.log.debug("-------------processing nodes-----------------")
        for key, value in self.schema[NODES].items():
            # Assume all keys start with '_' are not regular nodes
            if not key.startswith('_'):
                self.process_node(key, value)
        # insert nodes to model   
        self.model.update({NODES: self.nodes})

        self.log.debug("-------------processing nodes relationship-----------------")
        if RELATIONSHIPS in self.schema:
            for key, value in self.schema[RELATIONSHIPS].items():
                # Assume all keys start with '_' are not regular nodes
                if not key.startswith('_'):
                    self.process_relationship(key, value)

    def process_node(self, name, desc):
        """
        Process input node only 
        :param name: node
        :param desc:
        :return:
        """
        properties = self._process_properties(name, desc)

        # All nodes that has properties will be save to self.nodes
        if properties[NODE_PROPERTIES]:
            self.nodes[name] = properties

    def _process_properties(self, name, desc):
        """
        Gather properties from description

        :param desc: description of properties
        :return: a dict with properties, required property list and private property list
        """
        props = {}
        keys = []
        if PROPERTIES in desc and desc[PROPERTIES] is not None:
            for prop in desc[PROPERTIES]:
                props[prop]  = self.get_prop_detail(prop)
                value_unit_props = self.process_value_unit_type(prop, props[prop])
                if value_unit_props:
                    props.update(value_unit_props)
                if self.is_required_prop(prop):
                    if self.is_key_prop(prop): 
                        keys.append(prop)
        key_str = None if len(keys) == 0 else LIST_DELIMITER.join(map(str, keys)).strip(LIST_DELIMITER)
        self.id_fields.append({NODE_LABEL: name, KEY: key_str})
        return { NAME_PROP: name, DESC_PROP: DEFAULT_DESC, ID_PROPERTY: key_str, NODE_PROPERTIES: props, RELATIONSHIPS.lower(): {}}

    def process_relationship(self, name, desc):
        if MULTIPLIER in desc:
            multiplier = desc[MULTIPLIER]
            if( multiplier.lower() not in valid_relationship_types):
                 self.log.error(f'multiplier "{multiplier}" is not valid!')
                 multiplier = DEFAULT_MULTIPLIER
        else:
            multiplier = DEFAULT_MULTIPLIER

        if END_POINTS in desc:
            for end_points in desc[END_POINTS]:
                src = end_points[SRC]
                dest = end_points[DEST]
                if MULTIPLIER in end_points:
                    actual_multiplier = end_points[MULTIPLIER]
                    self.log.debug(
                        'End point multiplier: "{}" overriding relationship multiplier: "{}"'.format(actual_multiplier,
                                                                                                     multiplier))
                else:
                    actual_multiplier = multiplier
                if src in self.nodes:
                    self.add_relationship_to_node(src, actual_multiplier, name, dest)
                    # nodes[src][self.plural(dest)] = '[{}] @relation(name:"{}")'.format(dest, name)
                else:
                    self.log.error('Source node "{}" not found!'.format(src))

                if not dest in self.nodes:
                    self.log.error('Destination node "{}" not found!'.format(dest))


    # Process singular/plural array/single value based on relationship multipliers like  many-to-many, many-to-one etc.
    # Return a relationship property to add into a node
    def add_relationship_to_node(self, name, multiplier, relationship, dest):
        if multiplier not in valid_relationship_types:
            self.log.warning('Unsupported relationship multiplier: "{}"'.format(multiplier))
            return
        
        node = self.nodes[name]
        node[RELATIONSHIPS.lower()]={dest: {"dest_node": dest, TYPE: multiplier, RELATION_LABEL: relationship}}

    def is_required_prop(self, name):
        if name in self.schema[PROP_DEFINITIONS]:
            prop = self.schema[PROP_DEFINITIONS][name]
            result = prop.get(REQUIRED, False)
            result = str(result).lower()
            if result == "true" or result == "yes":
                return True
        return False
    
    def is_key_prop(self, name):
        if name in self.schema[PROP_DEFINITIONS]:
            prop = self.schema[PROP_DEFINITIONS][name]
            result = case_insensitive_get(prop, KEY, False)
            result = str(result).lower()
            if result == "true" or result == "yes":
                return True
        return False

    def get_prop_type(self, node_type, prop):
        if node_type in self.nodes:
            node = self.nodes[node_type]
            if prop in node[PROPERTIES]:
                return node[PROPERTIES][prop][PROP_TYPE]
        return DEFAULT_TYPE

    def get_prop_detail(self, name):
        result = {NAME_PROP: name, DESC_PROP: None, TYPE: DEFAULT_TYPE, PROP_REQUIRED: False}
        if name in self.schema[PROP_DEFINITIONS]:
            prop = self.schema[PROP_DEFINITIONS][name]
            result[DESC_PROP] = case_insensitive_get(prop, DESCRIPTION, "").replace("'", "\'")
            required = str(case_insensitive_get(prop, REQUIRED, "") )
            result[PROP_REQUIRED] = (required.lower()  == "true" or required.lower() == "yes")
            key = None
            if PROP_TYPE in prop:
                key = PROP_TYPE
            elif PROP_ENUM in prop:
                key = PROP_ENUM
            if key:
                prop_desc = prop[key]
                if isinstance(prop_desc, str):
                    if prop_desc not in valid_prop_types:
                        prop_desc = DEFAULT_TYPE
                    result[TYPE] = prop_desc.lower()
                elif isinstance(prop_desc, dict):
                    if VALUE_TYPE in prop_desc:
                        result[TYPE] = prop_desc[VALUE_TYPE]
                        if ITEM_TYPE in prop_desc:
                            item_type = self._get_item_type(prop_desc[ITEM_TYPE])
                            result[ITEM_TYPE] = item_type
                        elif PROP_ENUM in prop_desc:
                            item_type = self._get_item_type(prop_desc[PROP_ENUM])
                            result[ITEM_TYPE] = item_type
                        if UNITS in prop_desc:
                            result[HAS_UNIT] = True
                elif isinstance(prop_desc, list):
                    enum = []
                    for t in prop_desc:
                        if not re.search(r'://', t):
                            enum.append(t)
                    if len(enum) > 0:
                        result[ALLOWED_VALUES] = enum
                else:
                    self.log.debug(
                        'Property type: "{}" not supported, use default type: "{}"'.format(prop_desc, DEFAULT_TYPE))

                # Add value boundary support
                if MIN in prop:
                    result[MIN] = {VALUE_PROP: float(prop[MIN]), VALUE_EXCLUSIVE: False}
                if MAX in prop:
                    result[MAX] = {VALUE_PROP: float(prop[MAX]), VALUE_EXCLUSIVE: False}
                if EX_MIN in prop:
                    result[MIN] = {VALUE_PROP: float(prop[EX_MIN]), VALUE_EXCLUSIVE: True}
                if EX_MAX in prop:
                    result[MAX] = {VALUE_PROP: float(prop[EX_MAX]), VALUE_EXCLUSIVE: True}

        return result

    def _get_item_type(self, item_type):
        if isinstance(item_type, str):
            return item_type
        elif isinstance(item_type, list):
            enum = set()
            for t in item_type:
                if not re.search(r'://', t):
                    enum.add(t)
            if len(enum) > 0:
                return {TYPE: DEFAULT_TYPE, ALLOWED_VALUES: enum}
            else:
                return None
        else:
            self.log.error(f"{item_type} is not a scala or Enum!")
            return None

    def get_prop(self, node_name, name):
        if node_name in self.nodes:
            node = self.nodes[node_name]
            if name in node[PROPERTIES]:
                return node[PROPERTIES][name]
        return None

    def get_default_value(self, node_name, name):
        prop = self.get_prop(node_name, name)
        if prop:
            return prop.get(DEFAULT_VALUE, None)

    def get_default_unit(self, node_name, name):
        unit_prop_name = self.get_unit_property_name(name)
        return self.get_default_value(node_name, unit_prop_name)

    def get_valid_values(self, node_name, name):
        prop = self.get_prop(node_name, name)
        if prop:
            return prop.get(ENUM, None)

    def get_valid_units(self, node_name, name):
        unit_prop_name = self.get_unit_property_name(name)
        return self.get_valid_values(node_name, unit_prop_name)

    def get_extra_props(self, node_name, name, value):
        results = {}
        prop = self.get_prop(node_name, name)
        if prop and HAS_UNIT in prop and prop[HAS_UNIT]:
            # For MVP use default unit for all values
            results[self.get_unit_property_name(name)] = self.get_default_unit(node_name, name)
            org_prop_name = self.get_original_value_property_name(name)
            # For MVP use value is same as original value
            results[org_prop_name] = value
            results[self.get_unit_property_name(org_prop_name)] = self.get_default_unit(node_name, name)
        return results

    def process_value_unit_type(self, name, prop_type):
        results = {}
        if name in self.schema[PROP_DEFINITIONS]:
            prop = self.schema[PROP_DEFINITIONS][name]
            if PROP_TYPE in prop:
                prop_desc = prop[PROP_TYPE]
                if isinstance(prop_desc, dict):
                    if UNITS in prop_desc:
                        units = prop_desc[UNITS]
                        if units:
                            enum = set(units)
                            unit_prop_name = self.get_unit_property_name(name)
                            results[unit_prop_name] = {TYPE: DEFAULT_TYPE, ALLOWED_VALUES: enum, DEFAULT_VALUE: units[0]}
                            org_prop_name = self.get_original_value_property_name(name)
                            org_unit_prop_name = self.get_unit_property_name(org_prop_name)
                            results[org_prop_name] = prop_type
                            results[org_unit_prop_name] = {TYPE: DEFAULT_TYPE, ALLOWED_VALUES: enum, DEFAULT_VALUE: units[0]}
        return results

    @staticmethod
    def get_unit_property_name(name):
        return name + '_unit'

    @staticmethod
    def get_original_value_property_name(name):
        return name + '_original'


    @staticmethod
    def _validate_value_range(model_type, value):
        """
        Validate an int of float value, return whether value is in range

        :param model_type: dict specify value type and boundary/range
        :param value: value to be validated
        :return: boolean
        """

        if MIN in model_type:
            if value < model_type[MIN]:
                return False
        if MAX in model_type:
            if value > model_type[MAX]:
                return False
        if EX_MIN in model_type:
            if value <= model_type[EX_MIN]:
                return False
        if EX_MAX in model_type:
            if value >= model_type[EX_MAX]:
                return False
        return True
  
    def node_count(self):
        return len(self.nodes)


    # Get all id fields in the model.
    def get_id_fields(self):
       
        return self.id_fields

