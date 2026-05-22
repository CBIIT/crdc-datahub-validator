"""Unit tests for MetadataRemover: delete metadata SQS flow, deleteOrphanedDataFiles, and F008 orphan errors."""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(_this_dir)))
sys.path.insert(0, os.path.join(_project_root, "src"))

from common import constants
from metadata_remover import MetadataRemover


# ---------------------------------------------------------------------------
# __init__ and remove_metadata return contract
# ---------------------------------------------------------------------------

def test_errors_initialized():
    """MetadataRemover initializes self.errors to empty list."""
    mock_dao = MagicMock()
    mock_store = MagicMock()
    remover = MetadataRemover(mock_dao, mock_store)
    assert remover.errors == []
    assert isinstance(remover.errors, list)


def test_remove_metadata_returns_tuple_on_invalid_submission():
    """When get_submission returns None, remove_metadata returns (False, [])."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = None
    mock_store = MagicMock()

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, mock_store)
        result, orphan_errors = remover.remove_metadata("sub-1", "Subject", ["n1"])

    assert result is False
    assert orphan_errors == []
    mock_dao.get_submission.assert_called_once_with("sub-1")


def test_remove_metadata_logs_no_record_when_submission_missing(caplog):
    """Missing submission document logs no-record-found (not missing dataCommons)."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = None
    mock_store = MagicMock()
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, mock_store)
        with caplog.at_level("ERROR", logger="Essential Validator"):
            remover.remove_metadata("sub-1", "Subject", ["n1"])
    assert "no record found" in caplog.text
    assert f"missing {constants.DATA_COMMON_NAME}" not in caplog.text


def test_remove_metadata_logs_missing_datacommons(caplog):
    """Submission without dataCommons field logs missing field (not no-record-found)."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = {"_id": "sub-1"}
    mock_store = MagicMock()
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, mock_store)
        with caplog.at_level("ERROR", logger="Essential Validator"):
            remover.remove_metadata("sub-1", "Subject", ["n1"])
    assert f"missing {constants.DATA_COMMON_NAME}" in caplog.text
    assert "no record found" not in caplog.text


def test_remove_metadata_returns_tuple_on_no_datacommon():
    """When submission has no dataCommons, remove_metadata returns (False, [])."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = {"_id": "sub-1"}  # no dataCommons
    mock_store = MagicMock()

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, mock_store)
        result, orphan_errors = remover.remove_metadata("sub-1", "Subject", ["n1"])

    assert result is False
    assert orphan_errors == []


def test_remove_metadata_returns_tuple_on_model_unavailable():
    """When model is not available, remove_metadata returns (False, [])."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = {
        "_id": "sub-1",
        constants.DATA_COMMON_NAME: "dc1",
        constants.ROOT_PATH: "r",
        constants.MODEL_VERSION: "v1",
        constants.BATCH_BUCKET: "b1",
    }
    mock_store = MagicMock()
    bad_model = MagicMock()
    bad_model.model = None
    bad_model.get_nodes.return_value = []
    mock_store.get_model_by_data_common_version.return_value = bad_model

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, mock_store)
        result, orphan_errors = remover.remove_metadata("sub-1", "Subject", ["n1"])

    assert result is False
    assert orphan_errors == []


def test_remove_metadata_returns_tuple_on_no_existed_nodes():
    """When validate_data returns None/empty, remove_metadata returns (False, [])."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = {
        "_id": "sub-1",
        constants.DATA_COMMON_NAME: "dc1",
        constants.ROOT_PATH: "r",
        constants.MODEL_VERSION: "v1",
        constants.BATCH_BUCKET: "b1",
    }
    mock_store = MagicMock()
    mock_model = MagicMock()
    mock_model.model = {"nodes": []}
    mock_model.get_nodes.return_value = []
    mock_model.get_file_nodes.return_value = {}
    mock_store.get_model_by_data_common_version.return_value = mock_model
    mock_dao.check_metadata_ids.return_value = []  # no nodes to delete

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, mock_store)
        result, orphan_errors = remover.remove_metadata("sub-1", "Subject", ["n1"])

    assert result is False
    assert orphan_errors == []


def test_remove_metadata_success_returns_true_and_orphan_errors():
    """On successful delete, remove_metadata returns (True, orphan_errors) from _find_orphaned_files_and_build_errors."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = {
        "_id": "sub-1",
        constants.DATA_COMMON_NAME: "dc1",
        constants.ROOT_PATH: "r",
        constants.MODEL_VERSION: "v1",
        constants.BATCH_BUCKET: "b1",
    }
    mock_store = MagicMock()
    mock_model = MagicMock()
    mock_model.model = {"nodes": [{}]}
    mock_model.get_nodes.return_value = [{}]  # truthy so "model available" check passes
    mock_model.get_file_nodes.return_value = {}
    mock_store.get_model_by_data_common_version.return_value = mock_model
    mock_dao.check_metadata_ids.return_value = [{constants.NODE_ID: "n1", constants.NODE_TYPE: "Subject"}]
    mock_dao.delete_data_records.return_value = True

    mock_bucket = MagicMock()
    with patch("metadata_remover.S3Bucket", return_value=mock_bucket):
        remover = MetadataRemover(mock_dao, mock_store)
        with patch.object(remover, "process_children", return_value=True):
            with patch.object(
                remover,
                "_find_orphaned_files_and_build_errors",
                return_value=[{"submittedID": "orphan.csv", "errors": [{"code": "F008"}]}],
            ):
                result, orphan_errors = remover.remove_metadata(
                    "sub-1", "Subject", ["n1"], delete_orphaned_data_files=False
                )

    assert result is True
    assert len(orphan_errors) == 1
    assert orphan_errors[0]["submittedID"] == "orphan.csv"
    assert orphan_errors[0]["errors"][0]["code"] == "F008"


def test_delete_nodes_skips_s3_when_delete_orphaned_false():
    """When delete_orphaned_data_files is False, delete_nodes does not call delete_files_in_s3."""
    mock_dao = MagicMock()
    mock_dao.delete_data_records.return_value = True
    s3_info = {constants.FILE_NAME: "data.tsv"}
    nodes = [
        {
            constants.NODE_ID: "n1",
            constants.NODE_TYPE: "File",
            constants.S3_FILE_INFO: s3_info,
        }
    ]
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.submission_id = "sub-1"
        remover.def_file_nodes = {}
        with patch.object(remover, "delete_files_in_s3") as del_s3:
            with patch.object(remover, "process_children", return_value=True):
                assert remover.delete_nodes(nodes, delete_orphaned_data_files=False) is True
        del_s3.assert_not_called()
        mock_dao.delete_data_records.assert_called_once_with(nodes)


def test_process_children_skips_s3_when_delete_orphaned_false():
    """Cascaded child file node: Mongo delete runs but S3 is skipped when flag is False."""
    mock_dao = MagicMock()
    deleted_parent = {constants.NODE_TYPE: "Study", constants.NODE_ID: "p1"}
    child = {
        constants.NODE_TYPE: "CDSFile",
        constants.NODE_ID: "f1",
        constants.PARENTS: [{constants.PARENT_TYPE: "Study", constants.PARENT_ID_VAL: "p1"}],
        constants.S3_FILE_INFO: {constants.FILE_NAME: "child.tsv"},
    }
    mock_dao.get_nodes_by_parents.side_effect = [(True, [child]), (True, [])]
    mock_dao.delete_data_records.return_value = True
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.submission_id = "sub-1"
        remover.def_file_nodes = {"CDSFile": {}}
        with patch.object(remover, "delete_files_in_s3") as del_s3:
            assert remover.process_children([deleted_parent], delete_orphaned_data_files=False) is True
        del_s3.assert_not_called()
    mock_dao.delete_data_records.assert_called_once_with([child])


def test_process_children_removes_only_matching_parent_when_same_parent_type_twice():
    """Child may have multiple parents of the same type; deleting one removes only that edge."""
    mock_dao = MagicMock()
    deleted_parent = {constants.NODE_TYPE: "Study", constants.NODE_ID: "study_a"}
    child = {
        constants.NODE_TYPE: "Sample",
        constants.NODE_ID: "s1",
        constants.PARENTS: [
            {constants.PARENT_TYPE: "Study", constants.PARENT_ID_VAL: "study_a"},
            {constants.PARENT_TYPE: "Study", constants.PARENT_ID_VAL: "study_b"},
        ],
    }
    mock_dao.get_nodes_by_parents.return_value = (True, [child])
    mock_dao.delete_data_records.return_value = True
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.submission_id = "sub-1"
        remover.def_file_nodes = {}
        assert remover.process_children([deleted_parent], delete_orphaned_data_files=False) is True

    mock_dao.update_data_records.assert_called_once()
    updated = mock_dao.update_data_records.call_args[0][0]
    assert len(updated) == 1
    assert updated[0][constants.PARENTS] == [
        {constants.PARENT_TYPE: "Study", constants.PARENT_ID_VAL: "study_b"},
    ]
    mock_dao.delete_data_records.assert_not_called()


def test_process_children_calls_s3_when_delete_orphaned_true():
    """Cascaded child file node: delete_files_in_s3 runs when flag is True."""
    mock_dao = MagicMock()
    deleted_parent = {constants.NODE_TYPE: "Study", constants.NODE_ID: "p1"}
    s3_info = {constants.FILE_NAME: "child.tsv"}
    child = {
        constants.NODE_TYPE: "CDSFile",
        constants.NODE_ID: "f1",
        constants.PARENTS: [{constants.PARENT_TYPE: "Study", constants.PARENT_ID_VAL: "p1"}],
        constants.S3_FILE_INFO: s3_info,
    }
    mock_dao.get_nodes_by_parents.side_effect = [(True, [child]), (True, [])]
    mock_dao.delete_data_records.return_value = True
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.submission_id = "sub-1"
        remover.def_file_nodes = {"CDSFile": {}}
        with patch.object(remover, "delete_files_in_s3", return_value=True) as del_s3:
            assert remover.process_children([deleted_parent], delete_orphaned_data_files=True) is True
        del_s3.assert_called_once_with([s3_info])


def test_delete_nodes_calls_s3_when_delete_orphaned_true():
    """When delete_orphaned_data_files is True, delete_nodes calls delete_files_in_s3 for file nodes."""
    mock_dao = MagicMock()
    mock_dao.delete_data_records.return_value = True
    s3_info = {constants.FILE_NAME: "data.tsv"}
    nodes = [
        {
            constants.NODE_ID: "n1",
            constants.NODE_TYPE: "File",
            constants.S3_FILE_INFO: s3_info,
        }
    ]
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.submission_id = "sub-1"
        remover.def_file_nodes = {}
        with patch.object(remover, "delete_files_in_s3", return_value=True) as del_s3:
            with patch.object(remover, "process_children", return_value=True):
                assert remover.delete_nodes(nodes, delete_orphaned_data_files=True) is True
        del_s3.assert_called_once_with([s3_info])


def test_remove_metadata_passes_delete_orphaned_data_files_to_find_orphans():
    """remove_metadata passes delete_orphaned_data_files flag to _find_orphaned_files_and_build_errors."""
    mock_dao = MagicMock()
    mock_dao.get_submission.return_value = {
        "_id": "sub-1",
        constants.DATA_COMMON_NAME: "dc1",
        constants.ROOT_PATH: "r",
        constants.MODEL_VERSION: "v1",
        constants.BATCH_BUCKET: "b1",
    }
    mock_store = MagicMock()
    mock_model = MagicMock()
    mock_model.model = {"nodes": [{}]}
    mock_model.get_nodes.return_value = [{}]
    mock_model.get_file_nodes.return_value = {}
    mock_store.get_model_by_data_common_version.return_value = mock_model
    mock_dao.check_metadata_ids.return_value = [{constants.NODE_ID: "n1", constants.NODE_TYPE: "Subject"}]
    mock_dao.delete_data_records.return_value = True

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, mock_store)
        with patch.object(remover, "process_children", return_value=True):
            find_orphans = MagicMock(return_value=[])
            with patch.object(remover, "_find_orphaned_files_and_build_errors", find_orphans):
                remover.remove_metadata("sub-1", "Subject", ["n1"], delete_orphaned_data_files=True)

        find_orphans.assert_called_once_with("sub-1", True)


# ---------------------------------------------------------------------------
# _find_orphaned_files_and_build_errors
# ---------------------------------------------------------------------------

def test_find_orphaned_files_returns_empty_when_bucket_or_root_path_missing():
    """_find_orphaned_files_and_build_errors returns [] when bucket or root_path is not set."""
    mock_dao = MagicMock()
    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.bucket = None
        remover.root_path = "root"
        assert remover._find_orphaned_files_and_build_errors("sub-1", False) == []
        remover.bucket = MagicMock()
        remover.root_path = None
        assert remover._find_orphaned_files_and_build_errors("sub-1", False) == []


def test_find_orphaned_files_builds_f008_shape():
    """_find_orphaned_files_and_build_errors returns error dicts with F008 and file_validator shape."""
    mock_dao = MagicMock()
    mock_dao.get_files_by_submission.return_value = []  # no manifest files
    mock_dao.find_batch_by_file_name.return_value = None

    mock_bucket = MagicMock()
    mock_bucket.bucket_name = "test-bucket"
    mock_bucket.client.list_objects_v2.return_value = {
        "Contents": [{"Key": "root/file/orphan.csv", "LastModified": "2024-01-01T00:00:00Z"}],
        "NextContinuationToken": None,
    }

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.bucket = mock_bucket
        remover.root_path = "root"

        errors = remover._find_orphaned_files_and_build_errors("sub-1", delete_orphaned_data_files=False)

    assert len(errors) == 1
    err = errors[0]
    assert err[constants.TYPE] == constants.DATA_FILE_TYPE
    assert err[constants.QC_VALIDATION_TYPE] == constants.DATA_FILE_TYPE
    assert err[constants.SUBMITTED_ID] == "orphan.csv"
    assert err[constants.BATCH_ID] == "-"
    assert err[constants.QC_SEVERITY] == constants.STATUS_ERROR
    assert err[constants.ERRORS][0]["code"] == "F008"


def test_find_orphaned_files_skips_log_keys():
    """S3 keys containing /log are not considered orphaned files."""
    mock_dao = MagicMock()
    mock_dao.get_files_by_submission.return_value = []
    mock_dao.find_batch_by_file_name.return_value = None

    mock_bucket = MagicMock()
    mock_bucket.bucket_name = "test-bucket"
    mock_bucket.client.list_objects_v2.return_value = {
        "Contents": [{"Key": "root/file/log/foo.txt", "LastModified": None}],
        "NextContinuationToken": None,
    }

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.bucket = mock_bucket
        remover.root_path = "root"

        errors = remover._find_orphaned_files_and_build_errors("sub-1", delete_orphaned_data_files=False)

    assert len(errors) == 0


def test_find_orphaned_files_excludes_manifest_files():
    """Files still in get_files_by_submission are not reported as orphaned."""
    mock_dao = MagicMock()
    mock_dao.get_files_by_submission.return_value = [
        {constants.S3_FILE_INFO: {constants.FILE_NAME: "in_manifest.csv"}}
    ]
    mock_dao.find_batch_by_file_name.return_value = None

    mock_bucket = MagicMock()
    mock_bucket.bucket_name = "test-bucket"
    mock_bucket.client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "root/file/in_manifest.csv", "LastModified": None},
            {"Key": "root/file/orphan.csv", "LastModified": None},
        ],
        "NextContinuationToken": None,
    }

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.bucket = mock_bucket
        remover.root_path = "root"

        errors = remover._find_orphaned_files_and_build_errors("sub-1", delete_orphaned_data_files=False)

    assert len(errors) == 1
    assert errors[0][constants.SUBMITTED_ID] == "orphan.csv"


def test_find_orphaned_files_calls_delete_files_in_s3_when_flag_true():
    """When delete_orphaned_data_files is True and orphans exist, delete_files_in_s3 is called."""
    mock_dao = MagicMock()
    mock_dao.get_files_by_submission.return_value = []
    mock_dao.find_batch_by_file_name.return_value = None

    mock_bucket = MagicMock()
    mock_bucket.bucket_name = "test-bucket"
    mock_bucket.client.list_objects_v2.return_value = {
        "Contents": [{"Key": "root/file/orphan.csv", "LastModified": None}],
        "NextContinuationToken": None,
    }

    with patch("metadata_remover.S3Bucket"):
        remover = MetadataRemover(mock_dao, MagicMock())
        remover.bucket = mock_bucket
        remover.root_path = "root"
        delete_files = MagicMock()
        remover.delete_files_in_s3 = delete_files

        remover._find_orphaned_files_and_build_errors("sub-1", delete_orphaned_data_files=True)

    delete_files.assert_called_once()
    call_arg = delete_files.call_args[0][0]
    assert len(call_arg) == 1
    assert call_arg[0][constants.FILE_NAME] == "orphan.csv"
