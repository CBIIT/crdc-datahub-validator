"""Unit tests for MongoDao.set_submission_validation_status, especially scope='new' only-update-if-worse behavior."""
import pytest
from unittest.mock import MagicMock, patch

from common.mongo_dao import MongoDao
from common.constants import (
    SUBMISSION_COLLECTION,
    DATA_COLLECTION,
    ID,
    SUBMISSION_ID,
    METADATA_VALIDATION_STATUS,
    VALIDATION_ENDED,
    FILE_ERRORS,
    STATUS_ERROR,
    STATUS_PASSED,
    STATUS_WARNING,
)


def _setup_mock_db(mock_client_class):
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_submission_collection = MagicMock()
    mock_submission_collection.update_one.return_value = MagicMock(matched_count=1)

    def db_getitem(key):
        if key == SUBMISSION_COLLECTION:
            return mock_submission_collection
        return MagicMock()

    mock_db.__getitem__.side_effect = db_getitem
    return mock_submission_collection


@patch("common.mongo_dao.MongoClient")
def test_scope_new_current_worse_than_new_do_not_overwrite(mock_client_class):
    """scope='new', current status Error, new Passed: do not overwrite (keep Error)."""
    mock_submission_collection = _setup_mock_db(mock_client_class)
    dao = MongoDao("mongodb://localhost:27017", "test_db")
    with patch.object(dao, "count_docs", return_value=0):
        submission = {ID: "sub_1", METADATA_VALIDATION_STATUS: STATUS_ERROR}
        dao.set_submission_validation_status(
            submission, None, STATUS_PASSED, None, None, status_detail=None, scope="new"
        )
    update_one_call = mock_submission_collection.update_one.call_args
    set_payload = update_one_call[0][1]["$set"]
    assert set_payload[METADATA_VALIDATION_STATUS] == STATUS_ERROR


@patch("common.mongo_dao.MongoClient")
def test_scope_new_new_worse_than_current_update(mock_client_class):
    """scope='new', current Passed, new Error: update to Error."""
    mock_submission_collection = _setup_mock_db(mock_client_class)
    dao = MongoDao("mongodb://localhost:27017", "test_db")
    with patch.object(dao, "count_docs", return_value=0):
        submission = {ID: "sub_1", METADATA_VALIDATION_STATUS: STATUS_PASSED}
        dao.set_submission_validation_status(
            submission, None, STATUS_ERROR, None, None, status_detail=None, scope="new"
        )
    update_one_call = mock_submission_collection.update_one.call_args
    set_payload = update_one_call[0][1]["$set"]
    assert set_payload[METADATA_VALIDATION_STATUS] == STATUS_ERROR


@patch("common.mongo_dao.MongoClient")
def test_scope_new_current_none_treat_as_passed_passed(mock_client_class):
    """scope='new', no current status (treated as Passed): write Passed."""
    mock_submission_collection = _setup_mock_db(mock_client_class)
    dao = MongoDao("mongodb://localhost:27017", "test_db")
    with patch.object(dao, "count_docs", return_value=0):
        submission = {ID: "sub_1"}
        dao.set_submission_validation_status(
            submission, None, STATUS_PASSED, None, None, status_detail=None, scope="new"
        )
    update_one_call = mock_submission_collection.update_one.call_args
    set_payload = update_one_call[0][1]["$set"]
    assert set_payload[METADATA_VALIDATION_STATUS] == STATUS_PASSED


@patch("common.mongo_dao.MongoClient")
def test_scope_new_current_none_treat_as_passed_warning(mock_client_class):
    """scope='new', no current status (treated as Passed): write Warning when new is Warning."""
    mock_submission_collection = _setup_mock_db(mock_client_class)
    dao = MongoDao("mongodb://localhost:27017", "test_db")
    with patch.object(dao, "count_docs", return_value=0):
        submission = {ID: "sub_1"}
        dao.set_submission_validation_status(
            submission, None, STATUS_WARNING, None, None, status_detail=None, scope="new"
        )
    update_one_call = mock_submission_collection.update_one.call_args
    set_payload = update_one_call[0][1]["$set"]
    assert set_payload[METADATA_VALIDATION_STATUS] == STATUS_WARNING


@patch("common.mongo_dao.MongoClient")
def test_file_errors_persisted_without_file_status(mock_client_class):
    """fileErrors updates FILE_ERRORS when file_status is None (e.g. delete-metadata orphan F008)."""
    mock_submission_collection = _setup_mock_db(mock_client_class)
    dao = MongoDao("mongodb://localhost:27017", "test_db")
    file_err = [{"submittedID": "orphan.csv", "errors": [{"code": "F008"}]}]
    with patch.object(dao, "count_docs", return_value=0):
        with patch.object(dao.s3_service, "submissionHasDataFile", return_value=False):
            submission = {ID: "sub_1", METADATA_VALIDATION_STATUS: STATUS_PASSED}
            dao.set_submission_validation_status(
                submission, None, STATUS_PASSED, None, file_err, is_delete=True
            )
    update_one_call = mock_submission_collection.update_one.call_args
    set_payload = update_one_call[0][1]["$set"]
    assert set_payload[FILE_ERRORS] == file_err


@patch("common.mongo_dao.MongoClient")
def test_scope_all_overwrites_regardless(mock_client_class):
    """scope='all': overwrite current status (no only-if-worse rule)."""
    mock_submission_collection = _setup_mock_db(mock_client_class)
    dao = MongoDao("mongodb://localhost:27017", "test_db")
    with patch.object(dao, "count_docs", return_value=0):
        submission = {ID: "sub_1", METADATA_VALIDATION_STATUS: STATUS_ERROR}
        dao.set_submission_validation_status(
            submission, None, STATUS_PASSED, None, None, status_detail=None, scope="all"
        )
    update_one_call = mock_submission_collection.update_one.call_args
    set_payload = update_one_call[0][1]["$set"]
    assert set_payload[METADATA_VALIDATION_STATUS] == STATUS_PASSED
