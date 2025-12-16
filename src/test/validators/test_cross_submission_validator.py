import pytest
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime

# Add src to path for imports
current_directory = os.getcwd()
sys.path.insert(0, current_directory + '/src')

from x_submission_validator import CrossSubmissionValidator
from common.constants import (
    STATUS_ERROR, STATUS_PASSED, FAILED, DATA_COMMON_NAME, STATUS, 
    SUBMISSION_STATUS_SUBMITTED, SUBMISSION_REL_STATUS_RELEASED, STUDY_ID,
    NODE_TYPE, NODE_ID, ORIN_FILE_NAME, ADDITION_ERRORS, UPDATED_AT, VALIDATED_AT
)


class TestCrossSubmissionValidator:
    """Test cases for CrossSubmissionValidator class."""

    @pytest.fixture
    def mock_mongo_dao(self):
        """Create a mock mongo_dao for testing."""
        mock_dao = MagicMock()
        return mock_dao

    @pytest.fixture
    def validator(self, mock_mongo_dao):
        """Create a CrossSubmissionValidator instance with mocked dependencies."""
        return CrossSubmissionValidator(mock_mongo_dao)

    @pytest.fixture
    def valid_submission(self):
        """Create a valid submission object for testing."""
        return {
            '_id': 'test-submission-123',
            DATA_COMMON_NAME: 'test-data-commons',
            STATUS: SUBMISSION_STATUS_SUBMITTED,
            STUDY_ID: 'test-study-456'
        }

    @pytest.fixture
    def sample_data_records(self):
        """Create sample data records for testing."""
        return [
            {
                NODE_TYPE: 'program',
                NODE_ID: 'prog-001',
                ORIN_FILE_NAME: 'test_file.txt',
                'lineNumber': 1
            },
            {
                NODE_TYPE: 'study',
                NODE_ID: 'study-001',
                ORIN_FILE_NAME: 'test_file.txt',
                'lineNumber': 2
            }
        ]

    def test_init(self, mock_mongo_dao):
        """Test CrossSubmissionValidator initialization."""
        validator = CrossSubmissionValidator(mock_mongo_dao)
        assert validator.mongo_dao == mock_mongo_dao
        assert validator.model is None
        assert validator.submission is None
        assert validator.isError is None

    def test_validate_submission_not_found(self, validator, mock_mongo_dao):
        """Test validate when submission is not found."""
        mock_mongo_dao.get_submission.return_value = None
        
        result = validator.validate('non-existent-submission')
        
        assert result == FAILED
        mock_mongo_dao.get_submission.assert_called_once_with('non-existent-submission')

    def test_validate_submission_no_data_commons(self, validator, mock_mongo_dao):
        """Test validate when submission has no dataCommons field."""
        submission = {
            '_id': 'test-submission',
            STATUS: SUBMISSION_STATUS_SUBMITTED,
            STUDY_ID: 'test-study'
        }
        mock_mongo_dao.get_submission.return_value = submission
        
        result = validator.validate('test-submission')
        
        assert result == FAILED

    def test_validate_submission_invalid_status(self, validator, mock_mongo_dao):
        """Test validate when submission has invalid status."""
        submission = {
            '_id': 'test-submission',
            DATA_COMMON_NAME: 'test-data-commons',
            STATUS: 'InvalidStatus',
            STUDY_ID: 'test-study'
        }
        mock_mongo_dao.get_submission.return_value = submission
        
        result = validator.validate('test-submission')
        
        assert result == FAILED

    def test_validate_submission_no_metadata(self, validator, mock_mongo_dao, valid_submission):
        """Test validate when submission has no metadata records."""
        mock_mongo_dao.get_submission.return_value = valid_submission
        mock_mongo_dao.get_dataRecords_chunk.return_value = []
        
        result = validator.validate('test-submission')
        
        assert result == FAILED
        mock_mongo_dao.get_dataRecords_chunk.assert_called_once()

    def test_validate_success_no_conflicts(self, validator, mock_mongo_dao, valid_submission, sample_data_records):
        """Test successful validation with no conflicts."""
        mock_mongo_dao.get_submission.return_value = valid_submission
        mock_mongo_dao.get_dataRecords_chunk.return_value = sample_data_records
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (False, [])
        mock_mongo_dao.update_data_records_addition_error.return_value = True
        
        result = validator.validate('test-submission')
        
        assert result == STATUS_PASSED
        assert validator.submission == valid_submission
        assert validator.data_commons == 'test-data-commons'
        assert validator.isError is None

    def test_validate_success_with_conflicts(self, validator, mock_mongo_dao, valid_submission, sample_data_records):
        """Test validation with conflicts found."""
        mock_mongo_dao.get_submission.return_value = valid_submission
        mock_mongo_dao.get_dataRecords_chunk.return_value = sample_data_records
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (True, [{'id': 'conflicting-sub-123'}])
        mock_mongo_dao.update_data_records_addition_error.return_value = True
        
        result = validator.validate('test-submission')
        
        assert result == STATUS_ERROR
        assert validator.isError is True

    def test_validate_batch_processing(self, validator, mock_mongo_dao, valid_submission):
        """Test validation with multiple batches of data records."""
        # Create enough records to trigger batch processing
        large_batch = [{'nodeType': 'program', 'nodeID': f'prog-{i}'} for i in range(1001)]
        
        mock_mongo_dao.get_submission.return_value = valid_submission
        mock_mongo_dao.get_dataRecords_chunk.side_effect = [
            large_batch[:1000],  # First batch
            large_batch[1000:]   # Second batch (smaller)
        ]
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (False, [])
        mock_mongo_dao.update_data_records_addition_error.return_value = True
        
        result = validator.validate('test-submission')
        
        assert result == STATUS_PASSED
        assert mock_mongo_dao.get_dataRecords_chunk.call_count == 2

    def test_validate_nodes_exception_handling(self, validator, mock_mongo_dao, valid_submission):
        """Test validate_nodes exception handling."""
        mock_mongo_dao.get_submission.return_value = valid_submission
        mock_mongo_dao.get_dataRecords_chunk.return_value = [{'invalid': 'record'}]
        mock_mongo_dao.update_data_records_addition_error.return_value = True
        
        # Mock validate_node to raise an exception
        with patch.object(validator, 'validate_node', side_effect=Exception("Test exception")):
            result = validator.validate('test-submission')
        
        assert result == STATUS_ERROR
        assert validator.isError is True

    def test_validate_nodes_update_failure(self, validator, mock_mongo_dao, valid_submission, sample_data_records):
        """Test validate_nodes when database update fails."""
        mock_mongo_dao.get_submission.return_value = valid_submission
        mock_mongo_dao.get_dataRecords_chunk.return_value = sample_data_records
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (False, [])
        mock_mongo_dao.update_data_records_addition_error.return_value = False
        
        result = validator.validate('test-submission')
        
        assert result == STATUS_ERROR
        assert validator.isError is True

    def test_validate_node_no_conflicts(self, validator, mock_mongo_dao, valid_submission):
        """Test validate_node with no conflicts."""
        validator.submission = valid_submission
        validator.data_commons = 'test-data-commons'
        
        data_record = {
            NODE_TYPE: 'program',
            NODE_ID: 'prog-001',
            ORIN_FILE_NAME: 'test_file.txt',
            'lineNumber': 1
        }
        
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (False, [])
        
        status, errors = validator.validate_node(data_record, 'test-submission')
        
        assert status == STATUS_PASSED
        assert errors == []
        mock_mongo_dao.find_node_in_other_submissions_in_status.assert_called_once_with(
            'test-submission', 'test-study-456', 'test-data-commons', 
            'program', 'prog-001', [SUBMISSION_STATUS_SUBMITTED, SUBMISSION_REL_STATUS_RELEASED]
        )

    def test_validate_node_with_conflicts(self, validator, mock_mongo_dao, valid_submission):
        """Test validate_node with conflicts found."""
        validator.submission = valid_submission
        validator.data_commons = 'test-data-commons'
        
        data_record = {
            NODE_TYPE: 'program',
            NODE_ID: 'prog-001',
            ORIN_FILE_NAME: 'test_file.txt',
            'lineNumber': 1
        }
        
        conflicting_submissions = [
            {'_id': 'conflicting-sub-123'},
            {'_id': 'conflicting-sub-456'}
        ]
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (True, conflicting_submissions)
        
        status, errors = validator.validate_node(data_record, 'test-submission')
        
        assert status == STATUS_ERROR
        assert len(errors) == 1
        assert 'conflictingSubmissions' in errors[0]
        assert errors[0]['conflictingSubmissions'] == ['conflicting-sub-123', 'conflicting-sub-456']

    def test_validate_node_exception(self, validator, mock_mongo_dao, valid_submission):
        """Test validate_node exception handling."""
        validator.submission = valid_submission
        validator.data_commons = 'test-data-commons'
        
        data_record = {
            NODE_TYPE: 'program',
            NODE_ID: 'prog-001',
            ORIN_FILE_NAME: 'test_file.txt',
            'lineNumber': 1
        }
        
        mock_mongo_dao.find_node_in_other_submissions_in_status.side_effect = Exception("Database error")
        
        status, errors = validator.validate_node(data_record, 'test-submission')
        
        assert status == STATUS_ERROR
        assert len(errors) == 1
        assert errors[0]['title'] == 'Internal error'

    def test_validate_released_submission(self, validator, mock_mongo_dao, sample_data_records):
        """Test validation with released submission status."""
        released_submission = {
            '_id': 'test-submission',
            DATA_COMMON_NAME: 'test-data-commons',
            STATUS: SUBMISSION_REL_STATUS_RELEASED,
            STUDY_ID: 'test-study'
        }
        
        mock_mongo_dao.get_submission.return_value = released_submission
        mock_mongo_dao.get_dataRecords_chunk.return_value = sample_data_records
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (False, [])
        mock_mongo_dao.update_data_records_addition_error.return_value = True
        
        result = validator.validate('test-submission')
        
        assert result == STATUS_PASSED

    @patch('x_submission_validator.current_datetime')
    def test_validate_nodes_timestamps(self, mock_datetime, validator, mock_mongo_dao, valid_submission, sample_data_records):
        """Test that validate_nodes sets proper timestamps."""
        mock_datetime.return_value = '2024-01-01T12:00:00Z'
        
        mock_mongo_dao.get_submission.return_value = valid_submission
        mock_mongo_dao.get_dataRecords_chunk.return_value = sample_data_records
        mock_mongo_dao.find_node_in_other_submissions_in_status.return_value = (False, [])
        mock_mongo_dao.update_data_records_addition_error.return_value = True
        
        validator.validate('test-submission')
        
        # Verify that timestamps were set on the records
        updated_records = mock_mongo_dao.update_data_records_addition_error.call_args[0][0]
        for record in updated_records:
            assert record[UPDATED_AT] == '2024-01-01T12:00:00Z'
            assert record[VALIDATED_AT] == '2024-01-01T12:00:00Z'

    def test_validate_empty_data_commons(self, validator, mock_mongo_dao):
        """Test validate when dataCommons field is empty string."""
        submission = {
            '_id': 'test-submission',
            DATA_COMMON_NAME: '',
            STATUS: SUBMISSION_STATUS_SUBMITTED,
            STUDY_ID: 'test-study'
        }
        mock_mongo_dao.get_submission.return_value = submission
        
        result = validator.validate('test-submission')
        
        assert result == FAILED

    def test_validate_none_data_commons(self, validator, mock_mongo_dao):
        """Test validate when dataCommons field is None."""
        submission = {
            '_id': 'test-submission',
            DATA_COMMON_NAME: None,
            STATUS: SUBMISSION_STATUS_SUBMITTED,
            STUDY_ID: 'test-study'
        }
        mock_mongo_dao.get_submission.return_value = submission
        
        result = validator.validate('test-submission')
        
        assert result == FAILED

    def test_validate_missing_data_commons_key(self, validator, mock_mongo_dao):
        """Test validate when dataCommons key is missing from submission."""
        submission = {
            '_id': 'test-submission',
            STATUS: SUBMISSION_STATUS_SUBMITTED,
            STUDY_ID: 'test-study'
        }
        mock_mongo_dao.get_submission.return_value = submission
        
        result = validator.validate('test-submission')
        
        assert result == FAILED
