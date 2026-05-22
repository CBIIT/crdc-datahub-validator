import pytest
import json
import sys
import os
from unittest.mock import MagicMock, patch

# Resolve project root from this file (src/test/validators/...) and add src to path.
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(_this_dir)))
sys.path.insert(0, os.path.join(_project_root, 'src'))

from metadata_validator import metadataValidate, MetaDataValidator
from common import constants
from pymongo import errors


# ---------------------------------------------------------------------------
# increment_completed_batches (mongo_dao)
# ---------------------------------------------------------------------------

class TestIncrementCompletedBatches:

    def _setup_mock_db(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_validation_col = MagicMock()

        def db_getitem(key):
            if key == constants.VALIDATION_COLLECTION:
                return mock_validation_col
            return MagicMock()

        mock_db.__getitem__.side_effect = db_getitem
        return mock_validation_col

    @patch("common.mongo_dao.MongoClient")
    def test_increments_and_returns_count(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {constants.COMPLETED_BATCHES: 2}

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches("val-1", 5)

        assert count == 2
        assert is_last is False
        assert failed == 0
        assert worst == constants.STATUS_PASSED
        assert details == []

    @patch("common.mongo_dao.MongoClient")
    def test_is_last_batch_when_count_equals_total(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {constants.COMPLETED_BATCHES: 5}

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches("val-1", 5)

        assert count == 5
        assert is_last is True
        assert failed == 0

    @patch("common.mongo_dao.MongoClient")
    def test_is_last_batch_when_count_exceeds_total(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {constants.COMPLETED_BATCHES: 6}

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches("val-1", 5)

        assert count == 6
        assert is_last is True
        assert failed == 0

    @patch("common.mongo_dao.MongoClient")
    def test_batch_failed_increments_both_counters(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {constants.COMPLETED_BATCHES: 3, constants.FAILED_BATCHES: 1}

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches("val-1", 5, batch_failed=True)

        assert count == 3
        assert is_last is False
        assert failed == 1
        update_arg = col.find_one_and_update.call_args[0][1]
        assert update_arg['$inc'] == {constants.COMPLETED_BATCHES: 1, constants.FAILED_BATCHES: 1}

    @patch("common.mongo_dao.MongoClient")
    def test_batch_success_does_not_increment_failed(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {constants.COMPLETED_BATCHES: 3}

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches("val-1", 5, batch_failed=False)

        assert failed == 0
        update_arg = col.find_one_and_update.call_args[0][1]
        assert update_arg['$inc'] == {constants.COMPLETED_BATCHES: 1}

    @patch("common.mongo_dao.MongoClient")
    def test_batch_status_uses_max_operator(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {
            constants.COMPLETED_BATCHES: 2, constants.WORST_BATCH_STATUS: 2,
        }

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches(
            "val-1", 5, batch_status=constants.STATUS_ERROR
        )

        update_arg = col.find_one_and_update.call_args[0][1]
        assert update_arg['$max'] == {constants.WORST_BATCH_STATUS: constants.STATUS_PRECEDENCE[constants.STATUS_ERROR]}
        assert worst == constants.STATUS_ERROR

    @patch("common.mongo_dao.MongoClient")
    def test_worst_status_maps_back_from_numeric(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {
            constants.COMPLETED_BATCHES: 3, constants.WORST_BATCH_STATUS: 2,
        }

        dao = MongoDao("mongodb://localhost", "test_db")
        _, _, _, worst, _ = dao.increment_completed_batches("val-1", 5, batch_status=constants.STATUS_ERROR)

        assert worst == constants.STATUS_ERROR

    @patch("common.mongo_dao.MongoClient")
    def test_status_detail_uses_push_operator(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {
            constants.COMPLETED_BATCHES: 1,
            constants.BATCH_STATUS_DETAILS: ['Submission not found: sub-1'],
        }

        dao = MongoDao("mongodb://localhost", "test_db")
        _, _, _, _, details = dao.increment_completed_batches(
            "val-1", 5, status_detail='Submission not found: sub-1'
        )

        update_arg = col.find_one_and_update.call_args[0][1]
        assert update_arg['$push'] == {constants.BATCH_STATUS_DETAILS: 'Submission not found: sub-1'}
        assert details == ['Submission not found: sub-1']

    @patch("common.mongo_dao.MongoClient")
    def test_no_push_when_status_detail_is_none(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {constants.COMPLETED_BATCHES: 1}

        dao = MongoDao("mongodb://localhost", "test_db")
        dao.increment_completed_batches("val-1", 5, status_detail=None)

        update_arg = col.find_one_and_update.call_args[0][1]
        assert '$push' not in update_arg

    @patch("common.mongo_dao.MongoClient")
    def test_no_max_when_batch_status_is_none(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = {constants.COMPLETED_BATCHES: 1}

        dao = MongoDao("mongodb://localhost", "test_db")
        dao.increment_completed_batches("val-1", 5, batch_status=None)

        update_arg = col.find_one_and_update.call_args[0][1]
        assert '$max' not in update_arg

    @patch("common.mongo_dao.MongoClient")
    def test_validation_not_found_returns_none(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.return_value = None

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches("val-missing", 5)

        assert count is None
        assert is_last is False
        assert failed == 0
        assert worst is None
        assert details == []

    @patch("common.mongo_dao.MongoClient")
    def test_pymongo_error_returns_none(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find_one_and_update.side_effect = errors.PyMongoError("db error")

        dao = MongoDao("mongodb://localhost", "test_db")
        count, is_last, failed, worst, details = dao.increment_completed_batches("val-1", 5)

        assert count is None
        assert is_last is False
        assert failed == 0
        assert worst is None
        assert details == []


# ---------------------------------------------------------------------------
# get_dataRecords_by_ids (mongo_dao)
# ---------------------------------------------------------------------------

class TestGetDataRecordsByIds:

    def _setup_mock_db(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_data_col = MagicMock()

        def db_getitem(key):
            if key == constants.DATA_COLLECTION:
                return mock_data_col
            return MagicMock()

        mock_db.__getitem__.side_effect = db_getitem
        return mock_data_col

    @patch("common.mongo_dao.MongoClient")
    def test_full_match(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        records = [{constants.ID: 'r1'}, {constants.ID: 'r2'}]
        col.find.return_value = records

        dao = MongoDao("mongodb://localhost", "test_db")
        result = dao.get_dataRecords_by_ids(['r1', 'r2'])

        assert len(result) == 2

    @patch("common.mongo_dao.MongoClient")
    def test_partial_match_logs_warning(self, mock_client_class, caplog):
        import logging
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find.return_value = [{constants.ID: 'r1'}]

        dao = MongoDao("mongodb://localhost", "test_db")
        with caplog.at_level(logging.WARNING):
            result = dao.get_dataRecords_by_ids(['r1', 'r2', 'r3'])

        assert len(result) == 1
        warning_msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any('Partial match' in m for m in warning_msgs)

    @patch("common.mongo_dao.MongoClient")
    def test_empty_result(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find.return_value = []

        dao = MongoDao("mongodb://localhost", "test_db")
        result = dao.get_dataRecords_by_ids(['r1'])

        assert result == []

    @patch("common.mongo_dao.MongoClient")
    def test_pymongo_error_returns_none(self, mock_client_class):
        from common.mongo_dao import MongoDao
        col = self._setup_mock_db(mock_client_class)
        col.find.side_effect = errors.PyMongoError("db error")

        dao = MongoDao("mongodb://localhost", "test_db")
        result = dao.get_dataRecords_by_ids(['r1'])

        assert result is None


# ---------------------------------------------------------------------------
# Batch handler (metadataValidate) — integration tests
# ---------------------------------------------------------------------------

class TestBatchHandler:
    """Tests for the constants.TYPE_METADATA_VALIDATE_BATCH branch in metadataValidate."""

    @pytest.fixture
    def mock_configs(self):
        from common.constants import MODEL_FILE_DIR, TIER_CONFIG, SQS_NAME
        return {
            MODEL_FILE_DIR: '/fake/models',
            TIER_CONFIG: '/fake/tier.json',
            SQS_NAME: 'test-queue',
        }

    @pytest.fixture
    def mock_model_store(self):
        store = MagicMock()
        model = MagicMock()
        model.model = True
        model.get_nodes.return_value = ['node1']
        store.get_model_by_data_common_version.return_value = model
        return store

    @pytest.fixture
    def mock_mongo_dao(self):
        dao = MagicMock()
        dao.get_dataRecords_by_ids.return_value = [{constants.ID: 'r1'}, {constants.ID: 'r2'}]
        dao.get_submission.return_value = {
            '_id': 'sub-1',
            constants.DATA_COMMON_NAME: 'CDS',
            constants.MODEL_VERSION: '1.0',
            constants.STUDY_ID: 'study-1',
        }
        dao.find_study_by_id.return_value = {'studyName': 'Test Study'}
        dao.find_organization_name_by_study_id.return_value = ['Test Org']
        dao.increment_completed_batches.return_value = (1, False, 0, constants.STATUS_PASSED, [])
        dao.update_validation_status.return_value = True
        dao.set_submission_validation_status.return_value = True
        return dao

    def _make_batch_msg(self, overrides=None):
        payload = {
            constants.SQS_TYPE: constants.TYPE_METADATA_VALIDATE_BATCH,
            constants.SUBMISSION_ID: 'sub-1',
            constants.VALIDATION_ID: 'val-1',
            constants.DATA_RECORD_IDS: ['r1', 'r2'],
            constants.TOTAL_BATCHES: 3,
            constants.BATCH_INDEX: 0,
            constants.SCOPE: 'New',
        }
        if overrides:
            payload.update(overrides)
        msg = MagicMock()
        msg.body = json.dumps(payload)
        return msg

    def _run_one_message(self, mock_configs, mock_model_store, mock_mongo_dao, msg):
        """Run the service loop for exactly one message then break."""
        job_queue = MagicMock()
        job_queue.receiveMsgs.side_effect = [[msg], KeyboardInterrupt]

        with patch('metadata_validator.ModelFactory', return_value=mock_model_store):
            with patch('metadata_validator.set_scale_in_protection'):
                metadataValidate(mock_configs, job_queue, mock_mongo_dao)

    # -- happy path (non-last batch) --

    def test_happy_path_non_last_batch(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg()
        mock_mongo_dao.increment_completed_batches.return_value = (1, False, 0, constants.STATUS_PASSED, [])

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.get_dataRecords_by_ids.assert_called_once_with(['r1', 'r2'])
        mock_mongo_dao.increment_completed_batches.assert_called_once()
        call_kwargs = mock_mongo_dao.increment_completed_batches.call_args[1]
        assert call_kwargs['batch_failed'] is False
        assert call_kwargs['status_detail'] is None
        mock_mongo_dao.update_validation_status.assert_not_called()
        msg.delete.assert_called_once()

    # -- happy path (last batch) --

    def test_happy_path_last_batch_sets_final_status(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg({constants.BATCH_INDEX: 2})
        mock_mongo_dao.increment_completed_batches.return_value = (3, True, 0, constants.STATUS_PASSED, [])

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.update_validation_status.assert_called_once()
        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[0] == 'val-1'
        assert args[1] == constants.STATUS_PASSED
        assert kwargs['status_detail'] is None

        mock_mongo_dao.set_submission_validation_status.assert_called_once()
        sub_kwargs = mock_mongo_dao.set_submission_validation_status.call_args[1]
        assert sub_kwargs['status_detail'] is None
        msg.delete.assert_called_once()

    # -- validate_nodes throws, counter still increments --

    def test_validate_nodes_exception_still_increments(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg()

        with patch.object(MetaDataValidator, 'validate_nodes', side_effect=RuntimeError('boom')):
            self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        call_kwargs = mock_mongo_dao.increment_completed_batches.call_args[1]
        assert call_kwargs['batch_failed'] is True
        assert call_kwargs['batch_status'] == constants.FAILED

    def test_validate_nodes_exception_last_batch_uses_worst_status(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg({constants.BATCH_INDEX: 2})
        mock_mongo_dao.increment_completed_batches.return_value = (3, True, 1, constants.STATUS_ERROR, [])

        with patch.object(MetaDataValidator, 'validate_nodes', side_effect=RuntimeError('boom')):
            self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        assert args[1] == constants.STATUS_ERROR

    # -- model not available --

    def test_model_unavailable_still_increments(self, mock_configs, mock_model_store, mock_mongo_dao):
        bad_model = MagicMock()
        bad_model.model = None
        bad_model.get_nodes.return_value = []
        mock_model_store.get_model_by_data_common_version.return_value = bad_model

        msg = self._make_batch_msg()
        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.increment_completed_batches.assert_called_once()

    def test_model_unavailable_last_batch_preserves_status_error(self, mock_configs, mock_model_store, mock_mongo_dao):
        bad_model = MagicMock()
        bad_model.model = None
        bad_model.get_nodes.return_value = []
        mock_model_store.get_model_by_data_common_version.return_value = bad_model
        mock_mongo_dao.increment_completed_batches.return_value = (
            1, True, 1, constants.FAILED, ['CDS model version "1.0" is not available.']
        )

        msg = self._make_batch_msg({constants.TOTAL_BATCHES: 1})
        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert any('not available' in d for d in kwargs['status_detail'])
        # Caller passes FAILED; DAO does not write it to submission
        sub_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        assert sub_args[2] == constants.FAILED

    # -- missing study --

    def test_missing_study_id_still_increments(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_submission.return_value = {
            '_id': 'sub-1',
            constants.DATA_COMMON_NAME: 'CDS',
            constants.MODEL_VERSION: '1.0',
        }
        msg = self._make_batch_msg()
        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.increment_completed_batches.assert_called_once()

    def test_study_not_found_still_increments(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.find_study_by_id.return_value = None
        msg = self._make_batch_msg()
        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.increment_completed_batches.assert_called_once()

    def test_missing_study_last_batch_sets_failed(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.find_study_by_id.return_value = None
        mock_mongo_dao.increment_completed_batches.return_value = (
            1, True, 1, constants.FAILED, ['Invalid submission, no study found, sub-1!']
        )
        msg = self._make_batch_msg({constants.TOTAL_BATCHES: 1})

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert any('no study found' in d for d in kwargs['status_detail'])
        sub_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        assert sub_args[2] == constants.FAILED

    # -- submission not found --

    def test_submission_not_found_still_increments(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_submission.return_value = None
        msg = self._make_batch_msg()
        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.increment_completed_batches.assert_called_once()

    def test_submission_not_found_last_batch_sets_failed(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_submission.return_value = None
        mock_mongo_dao.increment_completed_batches.return_value = (
            1, True, 1, constants.FAILED, ['Submission not found: sub-1']
        )
        msg = self._make_batch_msg({constants.TOTAL_BATCHES: 1})

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert any('Submission not found' in d for d in kwargs['status_detail'])
        mock_mongo_dao.set_submission_validation_status.assert_not_called()

    # -- no data records found --

    def test_no_data_records_still_increments(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_dataRecords_by_ids.return_value = []
        msg = self._make_batch_msg()
        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.increment_completed_batches.assert_called_once()

    def test_no_data_records_last_batch_updates_submission_status(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_dataRecords_by_ids.return_value = []
        mock_mongo_dao.increment_completed_batches.return_value = (
            1, True, 1, constants.FAILED, ['No data records found for provided IDs in batch 0']
        )
        msg = self._make_batch_msg({constants.TOTAL_BATCHES: 1})

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.update_validation_status.assert_called_once()
        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert any('No data records found' in d for d in kwargs['status_detail'])

        mock_mongo_dao.set_submission_validation_status.assert_called_once()
        sub_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        sub_kwargs = mock_mongo_dao.set_submission_validation_status.call_args[1]
        assert sub_args[2] == constants.FAILED
        assert any('No data records found' in d for d in sub_kwargs['status_detail'])

    # -- missing scope treated as invalid message --

    def test_missing_scope_treated_as_invalid(self, mock_configs, mock_model_store, mock_mongo_dao):
        payload = {
            constants.SQS_TYPE: constants.TYPE_METADATA_VALIDATE_BATCH,
            constants.SUBMISSION_ID: 'sub-1',
            constants.VALIDATION_ID: 'val-1',
            constants.DATA_RECORD_IDS: ['r1', 'r2'],
            constants.TOTAL_BATCHES: 3,
            constants.BATCH_INDEX: 0,
        }
        msg = MagicMock()
        msg.body = json.dumps(payload)

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.get_submission.assert_called_once()
        mock_mongo_dao.get_dataRecords_by_ids.assert_not_called()
        call_kwargs = mock_mongo_dao.increment_completed_batches.call_args[1]
        assert call_kwargs['batch_failed'] is True
        assert call_kwargs['batch_status'] == constants.FAILED
        msg.delete.assert_called_once()

    def test_missing_scope_last_batch_updates_submission_status(self, mock_configs, mock_model_store, mock_mongo_dao):
        payload = {
            constants.SQS_TYPE: constants.TYPE_METADATA_VALIDATE_BATCH,
            constants.SUBMISSION_ID: 'sub-1',
            constants.VALIDATION_ID: 'val-1',
            constants.DATA_RECORD_IDS: ['r1', 'r2'],
            constants.TOTAL_BATCHES: 1,
            constants.BATCH_INDEX: 0,
        }
        msg = MagicMock()
        msg.body = json.dumps(payload)
        mock_mongo_dao.increment_completed_batches.return_value = (
            1, True, 1, constants.FAILED, ['Missing required field: scope']
        )

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.update_validation_status.assert_called_once()
        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert 'Missing required field: scope' in kwargs['status_detail']

        mock_mongo_dao.set_submission_validation_status.assert_called_once()
        sub_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        sub_kwargs = mock_mongo_dao.set_submission_validation_status.call_args[1]
        assert sub_args[2] == constants.FAILED
        assert 'Missing required field: scope' in sub_kwargs['status_detail']

    # -- zero total_batches treated as invalid message --

    def test_zero_total_batches_treated_as_invalid(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg({constants.TOTAL_BATCHES: 0})

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.increment_completed_batches.assert_not_called()
        mock_mongo_dao.get_dataRecords_by_ids.assert_not_called()
        msg.delete.assert_called_once()

    # -- worst-status precedence --

    def test_worst_status_precedence_used_as_final(self, mock_configs, mock_model_store, mock_mongo_dao):
        """Final status comes from worst_status in the 5-tuple, not from local batch state."""
        msg = self._make_batch_msg({constants.BATCH_INDEX: 2})
        mock_mongo_dao.increment_completed_batches.return_value = (3, True, 0, constants.STATUS_WARNING, [])

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        assert args[1] == constants.STATUS_WARNING

    def test_last_batch_succeeds_but_prior_batch_failed_uses_worst_status(self, mock_configs, mock_model_store, mock_mongo_dao):
        """When the finalizing batch succeeds locally but worst_status is Failed,
        the final status must reflect the worst across all batches."""
        msg = self._make_batch_msg({constants.BATCH_INDEX: 2})
        mock_mongo_dao.increment_completed_batches.return_value = (
            3, True, 1, constants.FAILED, ['Batch 1: Submission not found']
        )

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert kwargs['status_detail'] == ['Batch 1: Submission not found']

    def test_multiple_prior_failures_accumulated_in_details(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg({constants.BATCH_INDEX: 2})
        accumulated = ['Batch 0: No data records found', 'Batch 1: Missing scope']
        mock_mongo_dao.increment_completed_batches.return_value = (3, True, 2, constants.STATUS_ERROR, accumulated)

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.STATUS_ERROR
        assert kwargs['status_detail'] == accumulated

    # -- accumulated details written as array --

    def test_accumulated_details_written_as_array(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg({constants.BATCH_INDEX: 2})
        details_array = ['Batch 0: error A', 'Batch 1: error B']
        mock_mongo_dao.increment_completed_batches.return_value = (3, True, 2, constants.STATUS_ERROR, details_array)

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert isinstance(kwargs['status_detail'], list)
        assert kwargs['status_detail'] == details_array

        sub_kwargs = mock_mongo_dao.set_submission_validation_status.call_args[1]
        assert isinstance(sub_kwargs['status_detail'], list)
        assert sub_kwargs['status_detail'] == details_array

    # -- empty batch_details on success yields None --

    def test_success_with_empty_details_sets_none(self, mock_configs, mock_model_store, mock_mongo_dao):
        msg = self._make_batch_msg({constants.BATCH_INDEX: 2})
        mock_mongo_dao.increment_completed_batches.return_value = (3, True, 0, constants.STATUS_PASSED, [])

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert kwargs['status_detail'] is None

    # -- increment_completed_batches exception skips finalization --

    def test_increment_exception_skips_finalization(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.increment_completed_batches.side_effect = RuntimeError("db down")
        msg = self._make_batch_msg()

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.update_validation_status.assert_not_called()
        mock_mongo_dao.set_submission_validation_status.assert_not_called()
        msg.delete.assert_called_once()

    # -- increment_completed_batches returns None (DB error, no exception) --

    def test_increment_returns_none_logs_error(self, mock_configs, mock_model_store, mock_mongo_dao, caplog):
        import logging
        mock_mongo_dao.increment_completed_batches.return_value = (None, False, 0, None, [])
        msg = self._make_batch_msg()

        with caplog.at_level(logging.ERROR):
            self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.update_validation_status.assert_not_called()
        mock_mongo_dao.set_submission_validation_status.assert_not_called()
        assert any('validation may be stuck' in r.getMessage() for r in caplog.records)

    # -- missing validation_id --

    def test_missing_validation_id_skips_processing(self, mock_configs, mock_model_store, mock_mongo_dao):
        """When validation_id is absent, the function returns before try/finally,
        so no increment and no data record fetch should occur."""
        msg = self._make_batch_msg({constants.VALIDATION_ID: None})

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        mock_mongo_dao.increment_completed_batches.assert_not_called()
        mock_mongo_dao.get_dataRecords_by_ids.assert_not_called()
        msg.delete.assert_called_once()

    # -- missing constants.DATA_COMMON_NAME on last batch --

    def test_missing_datacommon_last_batch_sets_failed(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_submission.return_value = {
            '_id': 'sub-1',
            constants.MODEL_VERSION: '1.0',
            constants.STUDY_ID: 'study-1',
        }
        mock_mongo_dao.increment_completed_batches.return_value = (
            1, True, 1, constants.FAILED, ['Invalid submission, no datacommon found, sub-1!']
        )
        msg = self._make_batch_msg({constants.TOTAL_BATCHES: 1})

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert any('no datacommon found' in d for d in kwargs['status_detail'])
        sub_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        assert sub_args[2] == constants.FAILED

    # -- get_dataRecords_by_ids returns None (DB error) --

    def test_data_records_db_error_returns_none_still_increments(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_dataRecords_by_ids.return_value = None
        msg = self._make_batch_msg()

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        call_kwargs = mock_mongo_dao.increment_completed_batches.call_args[1]
        assert call_kwargs['batch_failed'] is True
        assert call_kwargs['batch_status'] == constants.FAILED

    def test_data_records_db_error_last_batch_sets_failed(self, mock_configs, mock_model_store, mock_mongo_dao):
        mock_mongo_dao.get_dataRecords_by_ids.return_value = None
        mock_mongo_dao.increment_completed_batches.return_value = (
            1, True, 1, constants.FAILED, ['No data records found for provided IDs in batch 0']
        )
        msg = self._make_batch_msg({constants.TOTAL_BATCHES: 1})

        self._run_one_message(mock_configs, mock_model_store, mock_mongo_dao, msg)

        args = mock_mongo_dao.update_validation_status.call_args[0]
        kwargs = mock_mongo_dao.update_validation_status.call_args[1]
        assert args[1] == constants.FAILED
        assert any('No data records found' in d for d in kwargs['status_detail'])
