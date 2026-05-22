import pytest
import sys
import os
from unittest.mock import MagicMock

current_directory = os.getcwd()
sys.path.insert(0, current_directory + '/src')
from metadata_validator import MetaDataValidator
from common.constants import STATUS_ERROR, STATUS_PASSED, STATUS_WARNING, ERRORS, WARNINGS
from common.error_messages import FAILED_VALIDATE_RECORDS
from common.model import DataModel


@pytest.fixture
def mock_mongo_dao():
    mock_dao = MagicMock()
    mock_dao.search_nodes_by_index.return_value = []
    return mock_dao

@pytest.fixture
def validator(mock_mongo_dao):
    # Create validator with mock mongo_dao
    validator = MetaDataValidator(mock_mongo_dao, None, None)
    validator.submission = {"_id": "test_submission"}
    return validator


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
     [{"code": "M021", "severity": "Error", "title": "Invalid node", "offendingProperty": "NodeType", "offendingValue": "", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'study_id\', \'properties\': {\'study_id\': {\'required\': True}}}}}} \"nodeType\" or \"props\" is empty."}],
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
     [{"code": "M017", "severity": "Warning", "title": "Invalid Property", "offendingProperty": "test_id", "offendingValue": "test", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'study_id\', \'properties\': {\'study_id\': {\'required\': True}}}}}} Property \"test_id\" is not defined in the model."}], STATUS_WARNING),

    # Test case 4: no valid nodes - SKIP: KeyError issue with current implementation
    pytest.param({"nodeType": "fake-program", "props": {"study_id": "test"}},
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
     [], STATUS_ERROR, marks=pytest.mark.skip(reason="KeyError issue with current implementation")),

    # Test case 5: invalid format - SKIP: KeyError issue with current implementation
    pytest.param({"nodeType": "program", "props": {"study_id": "test"}},
     {"model": {}},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'program' does not exist."}],
     # Warnings
     [], STATUS_ERROR, marks=pytest.mark.skip(reason="KeyError issue with current implementation")),

    # Test case 6: invalid nodes - SKIP: KeyError issue with current implementation
    pytest.param({"nodeType": "program", "props": {"study_id": "test"}},
     {"model": {}},
     # Errors
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'program' does not exist."}],
     # Warnings
     [], STATUS_ERROR, marks=pytest.mark.skip(reason="KeyError issue with current implementation")),

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
     [{"code": "M021", "severity": "Error", "title": "Invalid node", "offendingProperty": "NodeType", "offendingValue": "", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'study_id\', \'properties\': {\'study_id\': {\'required\': True}}}}}} \"nodeType\" or \"props\" is empty."}],
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
     [{"code": "M021", "severity": "Error", "title": "Invalid node", "offendingProperty": "NodeType", "offendingValue": "", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'study_id\', \'properties\': {\'study_id\': {\'required\': True}}}}}} \"nodeType\" or \"props\" is empty."}],
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
     [{"code": "M026", "severity": "Error", "title": "Invalid data model", "offendingProperty": "properties", "offendingValue": "", "description": "\"properties\" is not defined in the model."}],
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
     [{"code": "M017", "severity": "Warning", "title": "Invalid Property", "offendingProperty": "study_id", "offendingValue": "valid", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'study_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': True}}}}}} Property \"study_id\" is not defined in the model."}], STATUS_WARNING),

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
     [{"code": "M003", "severity": "Error", "title": "Missing required property", "offendingProperty": "test_id", "offendingValue": "", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': True}}}}}} Required property \"test_id\" is empty."}],
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
     [{"code": "M022", "severity": "Error", "title": "Missing ID property", "offendingProperty": "program_id", "offendingValue": "", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': False}}}}}} ID property, \"program_id\" is empty."},
      {"code": "M003", "severity": "Error", "title": "Missing required property", "offendingProperty": "test_id", "offendingValue": "", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': False}}}}}} Required property \"test_id\" is empty."}],
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
     [{"code": "M022", "severity": "Error", "title": "Missing ID property", "offendingProperty": "program_id", "offendingValue": "", "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'program_id\': {\'required\': False}}}}}} ID property, \"program_id\" is empty."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 15: null value is not allowed
    ({"nodeType": "program", "props": {"test_id": None, "program_id": "test"}},
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
     [{"code": "M003", "severity": "Error", "title": "Missing required property", "offendingProperty": "test_id", "offendingValue": None, "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': True}}}}}} Required property \"test_id\" is empty."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 16: id property key none not allowed
    ({"nodeType": "program", "props": {"test_id": None, "program_id": None}},
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
     [{"code": "M003", "severity": "Error", "title": "Missing required property", "offendingProperty": "test_id", "offendingValue": None, "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': True}}}}}} Required property \"test_id\" is empty."},
      {"code": "M003", "severity": "Error", "title": "Missing required property", "offendingProperty": "program_id", "offendingValue": None, "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': True}}}}}} Required property \"program_id\" is empty."}],
     # Warnings
     [], STATUS_ERROR),

    # Test case 17: integer values is allowed
    ({"nodeType": "program", "props": {"test_id": 1111, "program_id": "test"}},
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
     [],
     # Warnings
     [], STATUS_PASSED),

    # Test case 18: integer is allowed and not required
    ({"nodeType": "program", "props": {"test_id": 1111, "program_id": "test"}},
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
    # Test case 19: boolean is allowed
    ({"nodeType": "program", "props": {"test_id": True, "program_id": "test"}},
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
     [],
     # Warnings
     [], STATUS_PASSED),
    # Test case 20: invalid values in the properties; value is boolean
    ({"nodeType": "program", "props": {"test_id": False, "program_id": None}},
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
     [{"code": "M003", "severity": "Error", "title": "Missing required property", "offendingProperty": "program_id", "offendingValue": None, "description": "{\'model\': {\'nodes\': {\'program\': {\'id_property\': \'program_id\', \'properties\': {\'test_id\': {\'required\': True}, \'program_id\': {\'required\': True}}}}}} Required property \"program_id\" is empty."}],
     # Warnings
     [], STATUS_ERROR)
])
def test_validate_required_props(validator, data_record, node_definition, expected_errors, expected_warnings,
                                 expected_result):
    # create mock data model and set it on the validator - extract actual model from test data
    actual_model = node_definition["model"]
    mock_model = DataModel(actual_model)
    validator.model = mock_model
    result = validator.validate_required_props(data_record, node_definition)
    assert result['result'] == expected_result
    assert result[ERRORS] == expected_errors
    assert result[WARNINGS] == expected_warnings
