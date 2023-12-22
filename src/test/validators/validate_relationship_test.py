import pytest
from unittest.mock import MagicMock
import sys
import os

current_directory = os.getcwd()
sys.path.insert(0, current_directory + '/src')
from src.metadata_validator import MetaDataValidator
from src.common.constants import STATUS_WARNING, ERRORS, WARNINGS, STATUS_PASSED, STATUS_ERROR, DB, MONGO_DB
from src.common.error_messages import FAILED_VALIDATE_RECORDS
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
def mock_mongo_dao(mocker):
    mock_dao = mocker.MagicMock()
    mock_dao.search_nodes_by_type_and_value.return_value = []
    return mock_dao

@pytest.fixture
def mock_model_store(mocker):
    return mocker.MagicMock()


@pytest.fixture
def validator(mock_mongo_dao, mock_model_store):
    return MetaDataValidator(mock_mongo_dao, mock_model_store)

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
                     }
                 }
             }
         }},
         # mock for searching_nodes_by_type_and_value
         [],
         # Errors
         [],
         # Warnings
         [{"title": FAILED_VALIDATE_RECORDS, 'description': "Parent property does not exist or empty"}], STATUS_WARNING),
        # Test case 2: parent property not exist
        ({"nodeType": "program"},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "key1",
                     "properties": {
                         "key1": {"required": True}
                     }
                 }
             }
         }},
         # mock for searching_nodes_by_type_and_value
         [],
         # Errors
         [],
         # Warnings
         [{"title": FAILED_VALIDATE_RECORDS, 'description': "Parent property does not exist or empty"}], STATUS_WARNING),
        # Test case 3: parent property none
        ({"nodeType": "program", "parents": None},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "key1",
                     "properties": {
                         "key1": {"required": True}
                     }
                 }
             }
         }},
         # mock for searching_nodes_by_type_and_value
         [],
         # Errors
         [],
         # Warnings
         [{"title": FAILED_VALIDATE_RECORDS, 'description': "Parent property does not exist or empty"}], STATUS_WARNING),
        # Test case 4: current node and parent node in the model
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
                     }
                 },
                 "study": {
                     "id_property": "test",
                     "properties": {
                         "study_id": {"required": True}
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
         # mock for database
         [{"nodeType": "study", "props": {"study_id": "CDS-study-007"}}],
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
         [{"title": FAILED_VALIDATE_RECORDS, 'description': "Parent property 'fake-study' does not exist."}],
         # Warnings
         [], STATUS_ERROR),
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
                     }
                 },
                 "study": {
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
                 }
             }
         }},
         # mock for database
         [{"nodeType": "study", "props": {"study_id": "CDS-study-007"}}],
         # Errors
         [{"title": FAILED_VALIDATE_RECORDS, 'description': "Current node property 'fake-program' does not exist."},
          {"title": FAILED_VALIDATE_RECORDS, 'description': "Parent property 'fake-study' does not exist."}],
         # Warnings
         [], STATUS_ERROR),

        # Test case 7: ID/property is missing
        ({"nodeType": "program", "parents": [
            {
                "parentType": "study",
                "parentIDPropName": "fake_study_id",
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
                         "key1": {"required": True}
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
         [{"title": FAILED_VALIDATE_RECORDS, 'description': "ID property in parent node 'fake_study_id' does not exist."}],
         # Warnings
         [], STATUS_ERROR),

        # Test case 8: invalid parents value
        ({"nodeType": "program", "parents": [
            {
                "parentType": "study",
                "parentIDPropName": "study_id",
                "parentIDValue": None
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
                         "key1": {"required": True},
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
         [{"title": FAILED_VALIDATE_RECORDS, 'description': "'study_id's parent value is missing or empty."}],
         # Warnings
         [], STATUS_ERROR),
        # Test case 9: at least one parent node has non-empty parentIDValue property
        ({"nodeType": "program", "parents": [
            {
                "parentType": "study",
                "parentIDPropName": "study_id",
                "parentIDValue": ""
            },
            {
                "parentType": "program",
                "parentIDPropName": "program_id",
                "parentIDValue": ""
            },

        ]},
         {"model": {
             "nodes": {
                 "program": {
                     "id_property": "test",
                     "properties": {
                         "key1": {"required": True},
                         "program_id": {"required": True}
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
         [{"nodeType": "study", "props": {"study_id": "CDS-study-007"}},
          {"nodeType": "program", "props": {"program_id": "program_test"}}],
         # Errors
         [{"title": FAILED_VALIDATE_RECORDS,
           'description': "'study_id's parent value is missing or empty."},
          {"title": FAILED_VALIDATE_RECORDS,
           'description': "'program_id's parent value is missing or empty."}
          ],
         # Warnings
         [], STATUS_ERROR),
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
    ])

def test_validate_required_props(validator, data_record, node_definition, return_value, expected_errors,
                                 expected_warnings,
                                 expected_result):
    # uncomment this if test in dev database
    validator.mongo_dao.search_nodes_by_type_and_value.return_value = return_value
    result = validator.validate_relationship(data_record, node_definition)
    assert result['result'] == expected_result
    assert result[ERRORS] == expected_errors
    assert result[WARNINGS] == expected_warnings