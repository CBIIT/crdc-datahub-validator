import pytest
import sys
import os

current_directory = os.getcwd()
sys.path.insert(0, current_directory + '/src')
from src.metadata_validator import MetaDataValidator
from src.common.constants import STATUS_ERROR, STATUS_PASSED
from src.common.error_messages import FAILED_VALIDATE_RECORDS


@pytest.fixture
def validator():
    # Dummy parameters
    return MetaDataValidator(None, None, None)

@pytest.mark.parametrize("data_record, node_definition, expected_errors, expected_result", [
    # Test case 1: Valid data record
    ({"nodeType": "program", "props": {"key1": "value1"}}, {"model": {"nodes": {"program": {"properties": {"key1": {"required": True}}}}}},
     [], STATUS_PASSED),

    # Test case 2: Missing required property
    ({"nodeType": "program", "props": {}}, {"model": {"nodes": {"program": {"properties": {"study_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

    # Test case 3: Empty value for required property
    ({"nodeType": "program", "props": {"study_id": ""}}, {"model": {"nodes": {"program": {"properties": {"study_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'study_id' is missing or empty."}], STATUS_ERROR),

    # Test case 4: no valid nodes
    ({"nodeType": "fake-program", "props": {"study_id": "test"}}, {"model": {"nodes": {"program": {"properties": {"study_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'fake-program' does not exist."}], STATUS_ERROR),

    # Test case 5: invalid format
    ({"nodeType": "program", "props": {"study_id": "test"}},
     {"model": {}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'program' does not exist."}], STATUS_ERROR),

    # Test case 6: invalid nodes
    ({"nodeType": "program", "props": {"study_id": "test"}},
     {"model": {"nodes": {}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'program' does not exist."}], STATUS_ERROR),

    # Test case 7: invalid format
    ({"fake-nodeType": "program", "props": {"study_id": "test"}}, {"model": {"nodes": {"program": {"properties": {"study_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

    # Test case 8: invalid format
    ({"nodeType": "program", "fake-props": {"study_id": "test"}},
     {"model": {"nodes": {"program": {"properties": {"study_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

    # Test case 9: Valid data record
    ({"nodeType": "program", "props": {"key1": "value1"}},
     {"model": {"nodes": {"program": {"properties-fake": {"key1": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

    # Test case 10: all fields not required
    ({"nodeType": "program", "props": {"test_id": "test"}},
     {"model": {"nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": True}}}}}},
     [], STATUS_PASSED),

    # Test case 11: all fields not required
    ({"nodeType": "program", "props": {"test_id": "test"}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": False}, "program_id": {"required": True}}}}}},
     [], STATUS_PASSED),

    # Test case 12: all fields not required
    ({"nodeType": "program", "props": {"fake_id": "test"}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": True}}}}}},
     [], STATUS_PASSED),

    # Test case 13: all fields not required
    ({"nodeType": "program", "props": {"test_id": "", "node_id": ""}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'test_id' is missing or empty."}],
     STATUS_ERROR),

    # Test case 14: all fields not required
    ({"nodeType": "program", "props": {"test_id": "", "program_id": ""}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": False}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'test_id' is missing or empty."}],
     STATUS_ERROR),

    # Test case 15: all fields not required
    ({"nodeType": "program", "props": {"test_id": "", "program_id": ""}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": False}, "program_id": {"required": False}}}}}},
     [],
     STATUS_PASSED),

    # Test case 16: all fields not required
    ({"nodeType": "program", "props": {"test_id": "", "program_id": ""}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'test_id' is missing or empty."},
      {"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'program_id' is missing or empty."}],
     STATUS_ERROR),

    # Test case 17: multiple values can be empty and throw an error
    ({"nodeType": "program", "props": {"test_id": "test", "program_id": ""}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'program_id' is missing or empty."}],
     STATUS_ERROR),

    # Test case 18: integer value is not allowed
    ({"nodeType": "program", "props": {"test_id": 123456789, "program_id": "test"}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'test_id' is missing or empty."}],
     STATUS_ERROR),

    # Test case 19: None type is invalid
    ({"nodeType": "program", "props": {"test_id": None, "program_id": None}},
     {"model": {
         "nodes": {"program": {"properties": {"test_id": {"required": True}, "program_id": {"required": True}}}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'test_id' is missing or empty."},
      {"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'program_id' is missing or empty."}],
     STATUS_ERROR),

])
def test_validate_required_props(validator, data_record, node_definition, expected_errors, expected_result):
    result = validator.validate_required_props(data_record, node_definition)
    assert result['result'] == expected_result
    assert result["errors"] == expected_errors
