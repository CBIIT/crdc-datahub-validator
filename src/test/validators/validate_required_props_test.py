import pytest
import sys
import os

current_directory = os.getcwd()
sys.path.insert(0, current_directory + '/src')
from src.metadata_validator import MetaDataValidator
from src.common.constants import STATUS_ERROR, STATUS_PASSED, STATUS_WARNING, ERRORS, WARNINGS
from src.common.error_messages import FAILED_VALIDATE_RECORDS


@pytest.fixture
def validator():
    # Dummy parameters
    return MetaDataValidator(None, None, None)


@pytest.mark.parametrize("data_record, node_definition, expected_errors, expected_warnings, expected_result", [
    # Test case 1: Valid data record
    ({"nodeType": "program", "props": {"key1": "value1"}},
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
     [], STATUS_PASSED),

    # Test case 2: Missing required property
    ({"nodeType": "program", "props": {}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "study_id",
                 "properties": {
                     "study_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 3: Empty value for required property
    ({"nodeType": "program", "props": {"test_id": "test", "study_id": "valid"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "study_id",
                 "properties": {
                     "study_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [],
     # Warnings
     [{'title': 'Failed to validate dataRecords',
       'description': "data record key 'test_id' does not exist in the node-definition."}], STATUS_WARNING),

    # Test case 4: no valid nodes
    ({"nodeType": "fake-program", "props": {"study_id": "test"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "study_id",
                 "properties": {
                     "study_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'fake-program' does not exist."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 5: invalid format
    ({"nodeType": "program", "props": {"study_id": "test"}},
     {"model": {}},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'program' does not exist."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 6: invalid nodes
    ({"nodeType": "program", "props": {"study_id": "test"}},
     {"model": {}},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'program' does not exist."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 7: invalid format
    ({"fake-nodeType": "program", "props": {"study_id": "test"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "study_id",
                 "properties": {
                     "study_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 8: invalid format
    ({"nodeType": "program", "fake-props": {"study_id": "test"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "study_id",
                 "properties": {
                     "study_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 9: Valid data record
    ({"nodeType": "program", "props": {"key1": "value1"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "study_id",
                 "properties-fake": {
                     "key1": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 10: id propery does not exists
    ({"nodeType": "program", "props": {"test_id": "value1", "study_id": "valid"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "study_id",
                 "properties": {
                     "test_id": {"required": True},
                     "program_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [],
     # Warnings
     [{'title': 'Failed to validate dataRecords',
       'description': "data record key 'study_id' does not exist in the node-definition."}], STATUS_WARNING),

    # Test case 11: required field valid
    ({"nodeType": "program", "props": {"program_id": "value1"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "program_id",
                 "properties": {
                     "test_id": {"required": False},
                     "program_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [],
     # Warnings
     [], STATUS_PASSED),

    # Test case 12: one field is empty
    ({"nodeType": "program", "props": {"test_id": "", "program_id": "test"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "program_id",
                 "properties": {
                     "test_id": {"required": True},
                     "program_id": {"required": True}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'test_id' is missing or empty."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 13: all field value empty
    ({"nodeType": "program", "props": {"test_id": "", "program_id": ""}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "program_id",
                 "properties": {
                     "test_id": {"required": True},
                     "program_id": {"required": False}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, 'description': "ID/Key property is missing or empty in the data-record."},
      {"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'test_id' is missing or empty."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 14: id property is empty string not allowed
    ({"nodeType": "program", "props": {"program_id": ""}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "program_id",
                 "properties": {
                     "program_id": {"required": False}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, 'description': "ID/Key property is missing or empty in the data-record."}],
     # Warnings
     [], STATUS_ERROR),
    # 
    # # Test case 15: integer value is not allowed
    ({"nodeType": "program", "props": {"test_id": 123456789, "program_id": "test"}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "program_id",
                 "properties": {
                     "test_id": {"required": True},
                     "program_id": {"required": True}
                     # "program_id": {"required": False}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, 'description': "Required property 'test_id' is missing or empty."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 19: id property key none not allowed
    ({"nodeType": "program", "props": {"test_id": None, "program_id": None}},
     {"model": {
         "nodes": {
             "program": {
                 "id_property": "program_id",
                 "properties": {
                     "test_id": {"required": True},
                     "program_id": {"required": True}
                     # "program_id": {"required": False}
                 }
             }
         }
     }},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, 'description': "ID/Key property is missing or empty in the data-record."},
      {"title": FAILED_VALIDATE_RECORDS, 'description': "Required property 'test_id' is missing or empty."},
      {"title": FAILED_VALIDATE_RECORDS, 'description': "Required property 'program_id' is missing or empty."}],
     # Warnings
     [], STATUS_ERROR),
])
def test_validate_required_props(validator, data_record, node_definition, expected_errors, expected_warnings,
                                 expected_result):
    result = validator.validate_required_props(data_record, node_definition)
    assert result['result'] == expected_result
    assert result[ERRORS] == expected_errors
    assert result[WARNINGS] == expected_warnings
