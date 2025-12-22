"""
Unit tests for MongoDao.find_organization_name_by_study_id method
Tests cover success cases, error handling, and edge cases
"""

import unittest
from unittest.mock import MagicMock, patch
from common.mongo_dao import MongoDao
from common.constants import STUDY_COLLECTION, ORGANIZATION_COLLECTION
from pymongo import errors


class TestFindOrganizationNameByStudyId(unittest.TestCase):
    """Test cases for find_organization_name_by_study_id method"""

    def setUp(self):
        """Set up test fixtures"""
        self.study_id = "study_123"
        self.program_id = "program_456"
        self.organization_name = "Test Organization"

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_success(self, mock_client_class):
        """Test successful retrieval of organization name"""
        # Setup mocks
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        # Mock study query result
        mock_study_result = {
            '_id': self.study_id,
            'name': 'Test Study',
            'programID': self.program_id
        }
        mock_study_collection.find_one.return_value = mock_study_result
        
        # Mock organization query result
        mock_org_result = {
            '_id': self.program_id,
            'name': self.organization_name
        }
        mock_org_collection.find_one.return_value = mock_org_result
        
        # Create instance
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        # Execute
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        
        # Verify
        self.assertIsNotNone(result)
        self.assertEqual(result, [self.organization_name])

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_with_special_chars(self, mock_client_class):
        """Test organization name retrieval with special characters"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        special_org_name = "University of Science & Technology (UST) - Division"
        
        mock_study_collection.find_one.return_value = {
            '_id': self.study_id,
            'programID': self.program_id
        }
        mock_org_collection.find_one.return_value = {
            '_id': self.program_id,
            'name': special_org_name
        }
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        
        self.assertEqual(result, [special_org_name])

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_study_not_found(self, mock_client_class):
        """Test when study is not found"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        mock_study_collection.find_one.return_value = None
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        # When study is not found, method catches the error and returns None
        result = mongo_dao.find_organization_name_by_study_id("nonexistent_study")
        self.assertIsNone(result)

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_program_not_found(self, mock_client_class):
        """Test when program/organization is not found"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mock_study_collection.find_one.return_value = {
            '_id': self.study_id,
            'programID': self.program_id
        }
        # Organization not found
        mock_org_collection.find_one.return_value = None
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        # When organization is not found, method catches the error and returns None
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        self.assertIsNone(result)

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_pymongo_error_on_study(self, mock_client_class):
        """Test PyMongoError when querying study collection"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        mock_study_collection.find_one.side_effect = errors.PyMongoError("Connection error")
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        
        # Should return None on error
        self.assertIsNone(result)

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_pymongo_error_on_organization(self, mock_client_class):
        """Test PyMongoError when querying organization collection"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mock_study_collection.find_one.return_value = {
            '_id': self.study_id,
            'programID': self.program_id
        }
        mock_org_collection.find_one.side_effect = errors.PyMongoError("Connection error")
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        
        # Should return None on error
        self.assertIsNone(result)

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_generic_exception_on_study(self, mock_client_class):
        """Test generic Exception when querying study collection"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        mock_study_collection.find_one.side_effect = Exception("Unexpected error")
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        
        # Should return None on error
        self.assertIsNone(result)

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_generic_exception_on_organization(self, mock_client_class):
        """Test generic Exception when querying organization collection"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mock_study_collection.find_one.return_value = {
            '_id': self.study_id,
            'programID': self.program_id
        }
        mock_org_collection.find_one.side_effect = Exception("Unexpected error")
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        
        # Should return None on error
        self.assertIsNone(result)

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_empty_organization_name(self, mock_client_class):
        """Test when organization name field is empty"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mock_study_collection.find_one.return_value = {
            '_id': self.study_id,
            'programID': self.program_id
        }
        mock_org_collection.find_one.return_value = {
            '_id': self.program_id,
            'name': ""
        }
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        result = mongo_dao.find_organization_name_by_study_id(self.study_id)
        
        self.assertEqual(result, [""])

    @patch('common.mongo_dao.MongoClient')
    def test_find_organization_name_multiple_studies_same_program(self, mock_client_class):
        """Test correct organization retrieval for multiple studies with same program"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_study_collection = MagicMock()
        mock_org_collection = MagicMock()
        
        def db_getitem(key):
            if key == STUDY_COLLECTION:
                return mock_study_collection
            elif key == ORGANIZATION_COLLECTION:
                return mock_org_collection
            return MagicMock()
        
        mock_db.__getitem__.side_effect = db_getitem
        
        mock_study_collection.find_one.return_value = {
            '_id': "study_1",
            'programID': "program_shared"
        }
        mock_org_collection.find_one.return_value = {
            '_id': "program_shared",
            'name': "Shared Organization"
        }
        
        mongo_dao = MongoDao("mongodb://localhost:27017", "test_db")
        
        result = mongo_dao.find_organization_name_by_study_id("study_1")
        
        self.assertEqual(result, ["Shared Organization"])

if __name__ == '__main__':
    unittest.main()
