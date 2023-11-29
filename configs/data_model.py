valide_prop_types = [
    "string", # default type
    "integer",
    "number", # float or double
    "datetime",
    "date",
    "boolean", # true/false or yes/no
    "array" # value_type: list
            # a string with asterisk '*' characters as deliminators, e.g, "value1 * value2 * value3", represents a array [value1, value2, value3]
]

valid_relationship_types = ["many_to_one", "one_to_one", "many_to_many"]

example_model = {
    "data_commons": "CDS",
    "version": "1.0.0",
    "source_files": [
        "cds-model.yml",
        "cds-model-props.yml"
    ],
    "nodes": {
        "program": {
            "name": "program",
            "description": "example node description",
            "id_property": "program_acronym",
            "properties": {
                "program_sort_order": {
                    "name": "program_sort_order",
                    "description": "example property description",
                    "type": "integer",
                    "required": False
                },
                "program_acronym": {
                    "name": "program_acronym",
                    "description": "example property description",
                    "type": "string",
                    "required": True
                }
            },
            "relationships" :{}
        },
        "case": {
            "name": "case",
            "description": "example node description",
            "id_property": "case_id",
            "properties": { 
                "case_id": {
                    "name": "case_id",
                    "description": "example property description",
                    "type": "string",
                    "required": True
                },
                "sex":{
                   "name": "sex",
                   "description": "example property description",
                   "type": "string",
                   "permissible_values": [
                       "Male",
                       "Female",
                       "Unknown"
                       ]
               },
               "age": {
                   "name": "age",
                   "description": "example property description",
                   "type": "integer",
                   "minimum": {
                       "value": 0,
                       "exclusive": True
                       },
                   "maximum": {
                       "value": 200,
                       "exclusive": False
                       }
               },
               "image_type_included": {
                   "name": "image_type_included",
                   "description": "example property description",
                   "type": "array",
                   "item_type": "string", # only valide for array type
                   "permissible_values": [ # applies to items in the array
                                          "CT",
                                          "Histopathology",
                                          "MRI",
                                          "PET",
                                          "X-ray",
                                          "Optical",
                                          "Ultrasound"
                                          ],
                   "required": True
               }
            },
            "relationships" :{
                "study_arm": {
                    "dest_node": "study_arm",
                    "type": "many_to_one",
                    "label": "of_study_arm"
                },
                "cohort": {
                    "dest_node": "study_arm",
                    "type": "many_to_one",
                    "label": "member_of"
                },
                "study": {
                    "dest_node": "study_arm",
                    "type": "many_to_one",
                    "label": "member_of"
                }
            }
        }

    },
    "file-nodes": {
        "file": {
            "name-field": "file_name",
            "size-field": "file_size",
            "md5-field": "md5sum"
        }
    }

}
