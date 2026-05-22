"""Unit tests for essential_validator TYPE_DELETE (Delete Metadata) flow: deleteOrphanedDataFiles and F008 orphan errors."""
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(_this_dir)))
sys.path.insert(0, os.path.join(_project_root, "src"))

from common import constants
from essential_validator import essentialValidate


def _make_delete_message(overrides=None):
    payload = {
        constants.SQS_TYPE: constants.TYPE_DELETE,
        constants.SUBMISSION_ID: "sub-1",
        constants.NODE_TYPE: "Subject",
        constants.NODE_IDS: ["n1", "n2"],
        constants.DELETE_ALL: False,
        constants.EXCLUSIVE_IDS: [],
    }
    if overrides is not None:
        payload.update(overrides)
    msg = MagicMock()
    msg.body = json.dumps(payload)
    return msg


def _run_one_delete_message(configs, job_queue, mongo_dao, msg):
    """Run essentialValidate until one Delete Metadata message is processed, then break."""
    job_queue.receiveMsgs.side_effect = [[msg], KeyboardInterrupt]
    with patch("essential_validator.ModelFactory"):
        with patch("essential_validator.set_scale_in_protection"):
            try:
                essentialValidate(configs, job_queue, mongo_dao)
            except KeyboardInterrupt:
                pass


@pytest.fixture
def mock_configs():
    return {
        constants.SQS_NAME: "test-queue",
        constants.MODEL_FILE_DIR: "/tmp/models",
        constants.TIER_CONFIG: None,
    }


@pytest.fixture
def mock_mongo_dao():
    dao = MagicMock()
    dao.search_nodes_by_type_and_submission.return_value = ["n1", "n2"]
    return dao


class TestDeleteMessageDeleteOrphanedDataFiles:
    """deleteOrphanedDataFiles default and passing to MetadataRemover."""

    def test_message_without_delete_orphaned_data_files_calls_remove_metadata_with_false(
        self, mock_configs, mock_mongo_dao
    ):
        """When message omits deleteOrphanedDataFiles, remove_metadata is called with delete_orphaned_data_files=False."""
        msg = _make_delete_message()
        job_queue = MagicMock()
        job_queue.receiveMsgs.side_effect = [[msg], KeyboardInterrupt]

        with patch("essential_validator.ModelFactory"):
            with patch("essential_validator.set_scale_in_protection"):
                with patch("essential_validator.MetadataRemover") as mock_remover_class:
                    mock_remover = MagicMock()
                    mock_remover.submission = {constants.ID: "sub-1", constants.METADATA_VALIDATION_STATUS: constants.STATUS_PASSED}
                    mock_remover.remove_metadata.return_value = (True, [])
                    mock_remover_class.return_value = mock_remover
                    try:
                        essentialValidate(mock_configs, job_queue, mock_mongo_dao)
                    except KeyboardInterrupt:
                        pass

        mock_remover.remove_metadata.assert_called_once()
        # remove_metadata(submission_id, node_type, node_ids, delete_orphaned_data_files)
        call_args = mock_remover.remove_metadata.call_args[0]
        assert call_args[3] is False

    def test_message_with_delete_orphaned_data_files_true_calls_remove_metadata_with_true(
        self, mock_configs, mock_mongo_dao
    ):
        """When message has deleteOrphanedDataFiles true, remove_metadata is called with True."""
        msg = _make_delete_message({constants.DELETE_ORPHANED_DATA_FILES: True})
        job_queue = MagicMock()
        job_queue.receiveMsgs.side_effect = [[msg], KeyboardInterrupt]

        with patch("essential_validator.ModelFactory"):
            with patch("essential_validator.set_scale_in_protection"):
                with patch("essential_validator.MetadataRemover") as mock_remover_class:
                    mock_remover = MagicMock()
                    mock_remover.submission = {constants.ID: "sub-1", constants.METADATA_VALIDATION_STATUS: constants.STATUS_PASSED}
                    mock_remover.remove_metadata.return_value = (True, [])
                    mock_remover_class.return_value = mock_remover
                    try:
                        essentialValidate(mock_configs, job_queue, mock_mongo_dao)
                    except KeyboardInterrupt:
                        pass

        call_args = mock_remover.remove_metadata.call_args[0]
        assert call_args[3] is True


class TestDeleteSuccessAppendsOrphanErrors:
    """set_submission_validation_status receives combined fileErrors (existing + orphan F008)."""

    def test_set_submission_validation_status_called_with_combined_file_errors(
        self, mock_configs, mock_mongo_dao
    ):
        """On successful delete with orphan_errors, set_submission_validation_status is called with fileErrors = existing + orphan_errors."""
        msg = _make_delete_message()
        job_queue = MagicMock()
        job_queue.receiveMsgs.side_effect = [[msg], KeyboardInterrupt]

        existing_error = {"submittedID": "old.csv", "errors": []}
        orphan_error = {"submittedID": "orphan.csv", "errors": [{"code": "F008"}]}
        # Re-fetch returns fresh submission with existing fileErrors; combined = existing + orphan_errors
        mock_mongo_dao.get_submission.return_value = {
            constants.ID: "sub-1",
            constants.METADATA_VALIDATION_STATUS: constants.STATUS_PASSED,
            constants.FILE_ERRORS: [existing_error],
        }

        with patch("essential_validator.ModelFactory"):
            with patch("essential_validator.set_scale_in_protection"):
                with patch("essential_validator.MetadataRemover") as mock_remover_class:
                    mock_remover = MagicMock()
                    mock_remover.submission = {
                        constants.ID: "sub-1",
                        constants.METADATA_VALIDATION_STATUS: constants.STATUS_PASSED,
                        constants.FILE_ERRORS: [existing_error],
                    }
                    mock_remover.remove_metadata.return_value = (True, [orphan_error])
                    mock_remover_class.return_value = mock_remover
                    try:
                        essentialValidate(mock_configs, job_queue, mock_mongo_dao)
                    except KeyboardInterrupt:
                        pass

        mock_mongo_dao.set_submission_validation_status.assert_called_once()
        call_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        # set_submission_validation_status(submission, file_status, metadata_status, cross_submission_status, fileErrors, is_delete, ...)
        file_errors = call_args[4]
        assert file_errors == [existing_error, orphan_error]
        assert call_args[5] is True

    def test_set_submission_validation_status_with_no_existing_file_errors(
        self, mock_configs, mock_mongo_dao
    ):
        """When submission has no fileErrors, combined is just orphan_errors."""
        msg = _make_delete_message()
        job_queue = MagicMock()
        job_queue.receiveMsgs.side_effect = [[msg], KeyboardInterrupt]

        orphan_error = {"submittedID": "orphan.csv", "errors": [{"code": "F008"}]}
        # Re-fetch returns None so we use validator.submission (no FILE_ERRORS); existing = []
        mock_mongo_dao.get_submission.return_value = None

        with patch("essential_validator.ModelFactory"):
            with patch("essential_validator.set_scale_in_protection"):
                with patch("essential_validator.MetadataRemover") as mock_remover_class:
                    mock_remover = MagicMock()
                    mock_remover.submission = {
                        constants.ID: "sub-1",
                        constants.METADATA_VALIDATION_STATUS: constants.STATUS_PASSED,
                    }
                    mock_remover.remove_metadata.return_value = (True, [orphan_error])
                    mock_remover_class.return_value = mock_remover
                    try:
                        essentialValidate(mock_configs, job_queue, mock_mongo_dao)
                    except KeyboardInterrupt:
                        pass

        call_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        file_errors = call_args[4]
        assert file_errors == [orphan_error]

    def test_set_submission_validation_status_no_orphan_errors_keeps_existing_only(
        self, mock_configs, mock_mongo_dao
    ):
        """When remove_metadata returns no orphan_errors, fileErrors is only existing."""
        msg = _make_delete_message()
        job_queue = MagicMock()
        job_queue.receiveMsgs.side_effect = [[msg], KeyboardInterrupt]

        existing_error = {"submittedID": "old.csv", "errors": []}
        mock_mongo_dao.get_submission.return_value = {
            constants.ID: "sub-1",
            constants.METADATA_VALIDATION_STATUS: constants.STATUS_PASSED,
            constants.FILE_ERRORS: [existing_error],
        }

        with patch("essential_validator.ModelFactory"):
            with patch("essential_validator.set_scale_in_protection"):
                with patch("essential_validator.MetadataRemover") as mock_remover_class:
                    mock_remover = MagicMock()
                    mock_remover.submission = {
                        constants.ID: "sub-1",
                        constants.METADATA_VALIDATION_STATUS: constants.STATUS_PASSED,
                        constants.FILE_ERRORS: [existing_error],
                    }
                    mock_remover.remove_metadata.return_value = (True, [])
                    mock_remover_class.return_value = mock_remover
                    try:
                        essentialValidate(mock_configs, job_queue, mock_mongo_dao)
                    except KeyboardInterrupt:
                        pass

        call_args = mock_mongo_dao.set_submission_validation_status.call_args[0]
        file_errors = call_args[4]
        assert file_errors == [existing_error]
