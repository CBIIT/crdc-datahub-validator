import pytest
from unittest.mock import MagicMock, patch
from common.mongo_dao import MongoDao
from common.constants import STUDY_COLLECTION, ORGANIZATION_COLLECTION
from pymongo import errors


def _setup_mock_db(mock_client_class):
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_study_collection = MagicMock()
    mock_org_collection = MagicMock()

    def db_getitem(key):
        if key == STUDY_COLLECTION:
            return mock_study_collection
        if key == ORGANIZATION_COLLECTION:
            return mock_org_collection
        return MagicMock()

    mock_db.__getitem__.side_effect = db_getitem
    return mock_study_collection, mock_org_collection


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_success(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)

    study_id = "study_123"
    program_id = "program_456"
    org_name = "Test Organization"

    study_collection.find_one.return_value = {"_id": study_id, "programID": program_id}
    org_collection.find_one.return_value = {"_id": program_id, "name": org_name}

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    result = dao.find_organization_name_by_study_id(study_id)

    assert result == [org_name]


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_with_special_chars(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)

    special_name = "University of Science & Technology (UST) - Division"
    study_collection.find_one.return_value = {"_id": "study_123", "programID": "prog_1"}
    org_collection.find_one.return_value = {"_id": "prog_1", "name": special_name}

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_123") == [special_name]


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_study_not_found(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.return_value = None

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("nonexistent_study") is None


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_program_not_found(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.return_value = {"_id": "study_1", "programID": "p1"}
    org_collection.find_one.return_value = None

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_1") is None


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_pymongo_error_on_study(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.side_effect = errors.PyMongoError("Connection error")

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_123") is None


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_pymongo_error_on_organization(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.return_value = {"_id": "study_123", "programID": "p1"}
    org_collection.find_one.side_effect = errors.PyMongoError("Connection error")

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_123") is None


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_generic_exception_on_study(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.side_effect = Exception("Unexpected error")

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_123") is None


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_generic_exception_on_organization(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.return_value = {"_id": "study_123", "programID": "p1"}
    org_collection.find_one.side_effect = Exception("Unexpected error")

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_123") is None


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_empty_organization_name(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.return_value = {"_id": "study_123", "programID": "p1"}
    org_collection.find_one.return_value = {"_id": "p1", "name": ""}

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_123") == [""]


@patch("common.mongo_dao.MongoClient")
def test_find_organization_name_multiple_studies_same_program(mock_client_class):
    study_collection, org_collection = _setup_mock_db(mock_client_class)
    study_collection.find_one.return_value = {"_id": "study_1", "programID": "program_shared"}
    org_collection.find_one.return_value = {"_id": "program_shared", "name": "Shared Organization"}

    dao = MongoDao("mongodb://localhost:27017", "test_db")
    assert dao.find_organization_name_by_study_id("study_1") == ["Shared Organization"]