import pytest
from unittest.mock import MagicMock
import sys
import os

current_directory = os.getcwd()
sys.path.insert(0, current_directory + '/src')
from metadata_validator import MetaDataValidator
from common.constants import STATUS_WARNING, ERRORS, WARNINGS, STATUS_PASSED, STATUS_ERROR, DB, MONGO_DB
from common.error_messages import FAILED_VALIDATE_RECORDS
from common.model import DataModel
# from src.common.mongo_dao import MongoDao

# needs to modify for dev database test
# @pytest.fixture
# def mock_mongo_dao():
#     configs = {
#         DB: MONGO_DB,
#         "connection-str": "mongodb://XXXX:XXXX@localhost:27017/?authMechanism=DEFAULT"
#     }
#     return MongoDao(configs)

@pytest.fixture
def mock_mongo_dao():
    mock_dao = MagicMock()
    mock_dao.search_nodes_by_type_and_value.return_value = []
    mock_dao.search_nodes_by_index.return_value = []
    return mock_dao

@pytest.fixture
def mock_model_store():
    return MagicMock()


@pytest.fixture
def validator(mock_mongo_dao, mock_model_store):
    validator = MetaDataValidator(mock_mongo_dao, mock_model_store, None)
    validator.submission = {"_id": "test_submission"}
    return validator

@pytest.mark.parametrize(
    "data_record, node_definition, return_value, expected_errors, expected_warnings, expected_result", [
        # Test case 1: empty parent property
        ({"nodeType": "program", "parents": []
          },
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "key1",
                     "properties": {
                         "key1": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "program",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 }
             }
         }},
         # mock for searching_nodes_by_type_and_value
         [],
         # Errors
         [{"code": "M013", "severity": "Error", "title": "Relationship not specified", "offendingProperty": "program", "offendingValue": None, "description": "test_prefix All related node IDs are missing. Please ensure at least one related node ID is included."}],
         # Warnings
         [], STATUS_WARNING),
        # Test case 2: parent property not exist
        ({"nodeType": "program"},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "key1",
                     "properties": {
                         "key1": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "program",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 }
             }
         }},
         # mock for searching_nodes_by_type_and_value
         [],
         # Errors
         [{"code": "M013", "severity": "Error", "title": "Relationship not specified", "offendingProperty": "program", "offendingValue": None, "description": "test_prefix All related node IDs are missing. Please ensure at least one related node ID is included."}],
         # Warnings
         [], STATUS_WARNING),
        # Test case 3: invalid parent type
        ({"nodeType": "program", "parents": [
            {
                "parentType": "invalid-type",
                "parentIDPropName": "study_id",
                "parentIDValue": "CDS-study-007"
            }
        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "key1",
                     "properties": {
                         "key1": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "program",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 }
             }
         }},
         # mock for searching_nodes_by_type_and_value
         [],
         # Errors
         [{"code": "M023", "severity": "Error", "title": "Invalid relationship", "offendingProperty": "program", "offendingValue": None, "description": "test_prefix Relationship to a \"invalid-type\" node is not defined."}],
         # Warnings
         [], STATUS_ERROR),
        # Test case 4: parent node not found in database
        ({"nodeType": "program", "parents": [
            {
                "parentType": "study",
                "parentIDPropName": "study_id",
                "parentIDValue": "CDS-study-007"
            }
        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "program",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 },
                 "study": {
                     "id_property": "test",
                     "properties": {
                         "study_id": {"required": True}
                     }
                 }
             }
         }},
         # mock for database - empty result means parent not found
         [],
         # Errors
         [],
         # Warnings
         [], STATUS_PASSED),
        # Test case 5: parent node does not exist in the model
        ({"nodeType": "program", "parents": [
            {
                "parentType": "fake-study",
                "parentIDPropName": "study_id",
                "parentIDValue": "CDS-study-007"
            }
        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True}
                     }
                 },
                 "study": {
                     "id_property": "test",
                     "properties": {
                         "study_id": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "study",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 }
             }
         }},
         # mock for database
         [{"nodeType": "study", "props": {"study_id": "CDS-study-007"}}],
         # Errors
         [],
         # Warnings
         [], STATUS_PASSED),
        # Test case 6: current node and parent node does not exist in the model
        ({"nodeType": "fake-program", "parents": [
            {
                "parentType": "fake-study",
                "parentIDPropName": "study_id",
                "parentIDValue": "CDS-study-007"
            }
        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "study",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 },
                 "study": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True}
                     }
                 }
             }
         }},
         # mock for database
         [{"nodeType": "study", "props": {"study_id": "CDS-study-007"}}],
         # Errors
         [],
         # Warnings
         [], STATUS_PASSED),


        # Test case 10: multiple parents nodes with valid data in the database
        ({"nodeType": "program", "parents": [
            {
                "parentType": "study",
                "parentIDPropName": "study_id",
                "parentIDValue": "study_test"
            },
            {
                "parentType": "program",
                "parentIDPropName": "program_id",
                "parentIDValue": "program_test"
            },

        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True},
                         "program_id": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "study",
                             "type": "many_to_one",
                             "label": "member_of"
                         },
                         "program": {
                             "dest_node": "program",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 },
                 "study": {
                     "id_property": "test",
                     "properties": {
                         "study_id": {"required": True}
                     }
                 }
             }
         }},
         # mock for database
         [{"nodeType": "study", "props": {"study_id": "study_test"}},
          {"nodeType": "program", "props": {"program_id": "program_test"}}],
         # Errors
         [],
         # Warnings
         [], STATUS_PASSED),
        # Test case 11: multiple parents nodes with boolean value
        ({"nodeType": "program", "parents": [
            {
                "parentType": "study",
                "parentIDPropName": "study_id",
                "parentIDValue": False
            },
            {
                "parentType": "program",
                "parentIDPropName": "program_id",
                "parentIDValue": True
            },

        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True},
                         "program_id": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "study",
                             "type": "many_to_one",
                             "label": "member_of"
                         },
                         "program": {
                             "dest_node": "program",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 },
                 "study": {
                     "id_property": "test",
                     "properties": {
                         "study_id": {"required": True}
                     }
                 }
             }
         }},
         # mock for database
         [{"nodeType": "study", "props": {"study_id": False}},
          {"nodeType": "program", "props": {"program_id": True}}],
         # Errors
         [],
         # Warnings
         [], STATUS_PASSED),
        # Test case 12: a parent node with boolean value
        ({"nodeType": "program", "parents": [
            {
                "parentType": "study",
                "parentIDPropName": "study_id",
                "parentIDValue": True
            }
        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True},
                         "program_id": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "study",
                             "type": "many_to_one",
                             "label": "member_of"
                         },
                         "program": {
                             "dest_node": "program",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 },
                 "study": {
                     "id_property": "test",
                     "properties": {
                         "study_id": {"required": True}
                     }
                 }
             }
         }},
         # mock for database
         [{"nodeType": "study", "props": {"study_id": True}},
          {"nodeType": "program", "props": {"program_id": True}}],
         # Errors
         [],
         # Warnings
         [], STATUS_PASSED),
        # Test case 13: a parent node does not exist in the relationship
        ({"nodeType": "program", "parents": [
            {
                "parentType": "file",
                "parentIDPropName": "file_id",
                "parentIDValue": True
            }
        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True},
                         "program_id": {"required": True}
                     },
                     "relationships": {
                         "study": {
                             "dest_node": "study",
                             "type": "many_to_one",
                             "label": "member_of"
                         }
                     }
                 },
                 "file": {
                     "id_property": "test",
                     "properties": {
                         "file_id": {"required": True}
                     }
                 }
             }
         }},
         # mock for database
         [{"nodeType": "study", "props": {"study_id": True}},
          {"nodeType": "program", "props": {"program_id": True}}],
         # Errors
         [{"code": "M023", "severity": "Error", "title": "Invalid relationship", "offendingProperty": "program", "offendingValue": None, "description": "test_prefix Relationship to a \"file\" node is not defined."}],
         # Warnings
         [], STATUS_ERROR)
    ])
def test_validate_required_props(validator, data_record, node_definition, return_value, expected_errors,
                                 expected_warnings,
                                 expected_result):
    # uncomment this if test in dev database
    validator.mongo_dao.search_nodes_by_type_and_value.return_value = return_value
    validator.mongo_dao.search_nodes_by_index.return_value = return_value
    # create mock data model and set it on the validator - extract actual model from test data
    actual_model = node_definition["model"]
    mock_model = DataModel(actual_model)
    validator.model = mock_model
    result = validator.validate_relationship(data_record, "test_prefix")
    assert result['result'] == expected_result
    assert result[ERRORS] == expected_errors
    assert result[WARNINGS] == expected_warnings