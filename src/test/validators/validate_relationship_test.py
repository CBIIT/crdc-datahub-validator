import pytest
import sys
import os

current_directory = os.getcwd()
sys.path.insert(0, current_directory + '/src')
from src.metadata_validator import MetaDataValidator
from src.common.constants import STATUS_WARNING, ERRORS, WARNINGS, STATUS_PASSED, STATUS_ERROR
from src.common.error_messages import FAILED_VALIDATE_RECORDS


@pytest.fixture
def validator():
    # Dummy parameters
    return MetaDataValidator(None, None, None)


@pytest.mark.parametrize("data_record, node_definition, expected_errors, expected_warnings, expected_result", [
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
                    "key1": {"required": True}
                }
            }
         }
     }},
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
                     "key1": {"required": True}
                 }
             }
         }
     }},
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
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, 'description': "Current node property 'fake-program' does not exist."},
      {"title": FAILED_VALIDATE_RECORDS, 'description': "Parent property 'fake-study' does not exist."}],
     # Warnings
     [], STATUS_ERROR)
])
def test_validate_required_props(validator, data_record, node_definition, expected_errors, expected_warnings,
                                 expected_result):
    result = validator.validate_relationship(data_record, node_definition)
    assert result['result'] == expected_result
    assert result[ERRORS] == expected_errors
    assert result[WARNINGS] == expected_warnings
