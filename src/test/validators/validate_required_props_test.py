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
    ({"nodeType": "program", "rawData": {"key1": "value1"}}, {"model": {"nodes": {"program": {"properties": ["key1"]}}}},
     [], STATUS_PASSED),

    # Test case 2: Missing required property
    ({"nodeType": "program", "rawData": {}}, {"model": {"nodes": {"program": {"properties": ["study_id"]}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

    # Test case 3: Empty value for required property
    ({"nodeType": "program", "rawData": {"study_id": ""}}, {"model": {"nodes": {"program": {"properties": ["study_id"]}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required property 'study_id' is missing or empty."}], STATUS_ERROR),

    # Test case 4: no valid nodes
    ({"nodeType": "fake-program", "rawData": {"study_id": "test"}}, {"model": {"nodes": {"program": {"properties": ["test_id"]}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'fake-program' does not exist."}], STATUS_ERROR),

    # Test case 5: invalid format
    ({"nodeType": "program", "rawData": {"study_id": "test"}},
     {"fake-model": {}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "node definition is not correctly formatted."}], STATUS_ERROR),

    # Test case 6: invalid nodes
    ({"nodeType": "program", "rawData": {"study_id": "test"}},
     {"model": {"nodes": {}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "Required node 'program' does not exist."}], STATUS_ERROR),

    # Test case 7: invalid format
    ({"fake-nodeType": "program", "rawData": {"study_id": "test"}}, {"model": {"nodes": {"program": {"properties": ["study_id"]}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

    # Test case 8: invalid format
    ({"nodeType": "program", "fake-rawData": {"study_id": "test"}},
     {"model": {"nodes": {"program": {"properties": ["study_id"]}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

    # Test case 9: Valid data record
    ({"nodeType": "program", "rawData": {"key1": "value1"}},
     {"model": {"nodes": {"program": {"properties-fake": ["key1"]}}}},
     [{"title": FAILED_VALIDATE_RECORDS, "description": "data record is not correctly formatted."}], STATUS_ERROR),

])
def test_validate_required_props(validator, data_record, node_definition, expected_errors, expected_result):
    result = validator.validate_required_props(data_record, node_definition)
    assert result['result'] == expected_result
    assert result["errors"] == expected_errors
