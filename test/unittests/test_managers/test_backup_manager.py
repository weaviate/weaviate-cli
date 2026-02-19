import json
import pytest
from unittest.mock import MagicMock

from weaviate_cli.managers.backup_manager import BackupManager


@pytest.fixture
def mock_client_with_backup(mock_client: MagicMock) -> MagicMock:
    """
    Configures mock_client with sensible defaults for BackupManager tests.
    - get_meta returns a version > 1.25.0 so BackupConfigCreate is used.
    - collections.exists returns True by default.
    - backup.create returns a SUCCESS result by default.

    Collections and backup are assigned as plain MagicMocks (not spec-constrained)
    so that attribute access like .exists and .create is freely allowed.
    """
    mock_client.get_meta.return_value = {"version": "1.26.0"}

    mock_collections = MagicMock()
    mock_collections.exists.return_value = True
    mock_client.collections = mock_collections

    mock_backup = MagicMock()
    mock_backup.create.return_value = MagicMock(status=MagicMock(value="SUCCESS"))
    mock_backup.restore.return_value = MagicMock(status=MagicMock(value="SUCCESS"))
    mock_client.backup = mock_backup

    return mock_client


@pytest.fixture
def backup_manager(mock_client_with_backup: MagicMock) -> BackupManager:
    """
    Returns a BackupManager instance using a fully configured mock client.
    """
    return BackupManager(mock_client_with_backup)


# ---------------------------------------------------------------------------
# create_backup — collection existence checks
# ---------------------------------------------------------------------------


def test_create_backup_include_collection_not_found(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    create_backup raises when a collection in `include` does not exist.
    """
    mock_client_with_backup.collections.exists.return_value = False

    with pytest.raises(Exception) as exc_info:
        backup_manager.create_backup(
            backup_id="my-backup",
            backend="s3",
            include="NonExistentCollection",
        )

    assert "NonExistentCollection" in str(exc_info.value)
    assert "does not exist" in str(exc_info.value)
    assert "include" in str(exc_info.value)


def test_create_backup_include_one_collection_missing_among_multiple(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    create_backup raises when only one collection among multiple in `include` does not exist.
    """
    mock_client_with_backup.collections.exists.side_effect = [True, False]

    with pytest.raises(Exception) as exc_info:
        backup_manager.create_backup(
            backup_id="my-backup",
            backend="s3",
            include="ExistingCollection,MissingCollection",
        )

    assert "MissingCollection" in str(exc_info.value)
    assert "does not exist" in str(exc_info.value)


def test_create_backup_exclude_collection_not_found(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    create_backup raises when a collection in `exclude` does not exist.
    """
    mock_client_with_backup.collections.exists.return_value = False

    with pytest.raises(Exception) as exc_info:
        backup_manager.create_backup(
            backup_id="my-backup",
            backend="s3",
            exclude="NonExistentCollection",
        )

    assert "NonExistentCollection" in str(exc_info.value)
    assert "does not exist" in str(exc_info.value)
    assert "exclude" in str(exc_info.value)


# ---------------------------------------------------------------------------
# create_backup — success without wait
# ---------------------------------------------------------------------------


def test_create_backup_no_wait_text_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    create_backup without wait emits text success message and does not check result status.
    """
    backup_manager.create_backup(
        backup_id="my-backup",
        backend="s3",
        wait=False,
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "my-backup" in out
    assert "created successfully" in out


def test_create_backup_no_wait_json_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    create_backup without wait and json_output=True emits JSON with status=success.
    """
    backup_manager.create_backup(
        backup_id="my-backup",
        backend="s3",
        wait=False,
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "my-backup" in data["message"]


# ---------------------------------------------------------------------------
# create_backup — with wait
# ---------------------------------------------------------------------------


def test_create_backup_with_wait_success(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    create_backup with wait=True and result.status.value == "SUCCESS" emits success message.
    """
    mock_client_with_backup.backup.create.return_value = MagicMock(
        status=MagicMock(value="SUCCESS")
    )

    backup_manager.create_backup(
        backup_id="my-backup",
        backend="s3",
        wait=True,
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "my-backup" in out
    assert "created successfully" in out


def test_create_backup_with_wait_non_success_raises(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    create_backup with wait=True and result.status.value != "SUCCESS" raises an exception.
    """
    mock_client_with_backup.backup.create.return_value = MagicMock(
        status=MagicMock(value="FAILED")
    )

    with pytest.raises(Exception) as exc_info:
        backup_manager.create_backup(
            backup_id="my-backup",
            backend="s3",
            wait=True,
        )

    assert "my-backup" in str(exc_info.value)
    assert "FAILED" in str(exc_info.value)


# ---------------------------------------------------------------------------
# create_backup — argument passing
# ---------------------------------------------------------------------------


def test_create_backup_passes_correct_args_with_include(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    create_backup passes include_collections as a split list to client.backup.create.
    """
    backup_manager.create_backup(
        backup_id="my-backup",
        backend="gcs",
        include="CollectionA,CollectionB",
        wait=False,
    )

    mock_client_with_backup.backup.create.assert_called_once()
    call_kwargs = mock_client_with_backup.backup.create.call_args.kwargs
    assert call_kwargs["backup_id"] == "my-backup"
    assert call_kwargs["backend"] == "gcs"
    assert call_kwargs["include_collections"] == ["CollectionA", "CollectionB"]
    assert call_kwargs["exclude_collections"] is None
    assert call_kwargs["wait_for_completion"] is False


def test_create_backup_passes_correct_args_with_exclude(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    create_backup passes exclude_collections as a split list and include_collections=None.
    """
    backup_manager.create_backup(
        backup_id="my-backup",
        backend="s3",
        exclude="CollectionX,CollectionY",
        wait=False,
    )

    call_kwargs = mock_client_with_backup.backup.create.call_args.kwargs
    assert call_kwargs["include_collections"] is None
    assert call_kwargs["exclude_collections"] == ["CollectionX", "CollectionY"]


def test_create_backup_passes_none_collections_when_not_specified(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    create_backup passes None for both collection args when neither is specified.
    """
    backup_manager.create_backup(backup_id="my-backup", backend="s3")

    call_kwargs = mock_client_with_backup.backup.create.call_args.kwargs
    assert call_kwargs["include_collections"] is None
    assert call_kwargs["exclude_collections"] is None


# ---------------------------------------------------------------------------
# restore_backup — success without wait
# ---------------------------------------------------------------------------


def test_restore_backup_no_wait_text_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    restore_backup without wait emits text success message.
    """
    backup_manager.restore_backup(
        backup_id="my-backup",
        backend="s3",
        wait=False,
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "my-backup" in out
    assert "restored successfully" in out


def test_restore_backup_no_wait_json_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    restore_backup without wait and json_output=True emits JSON with status=success.
    """
    backup_manager.restore_backup(
        backup_id="my-backup",
        backend="s3",
        wait=False,
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "my-backup" in data["message"]


# ---------------------------------------------------------------------------
# restore_backup — with wait
# ---------------------------------------------------------------------------


def test_restore_backup_with_wait_success(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    restore_backup with wait=True and SUCCESS status emits success message.
    """
    mock_client_with_backup.backup.restore.return_value = MagicMock(
        status=MagicMock(value="SUCCESS")
    )

    backup_manager.restore_backup(
        backup_id="my-backup",
        backend="s3",
        wait=True,
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "my-backup" in out
    assert "restored successfully" in out


def test_restore_backup_with_wait_non_success_raises(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    restore_backup with wait=True and non-SUCCESS status raises an exception.
    """
    mock_client_with_backup.backup.restore.return_value = MagicMock(
        status=MagicMock(value="FAILED")
    )

    with pytest.raises(Exception) as exc_info:
        backup_manager.restore_backup(
            backup_id="my-backup",
            backend="s3",
            wait=True,
        )

    assert "my-backup" in str(exc_info.value)
    assert "FAILED" in str(exc_info.value)


# ---------------------------------------------------------------------------
# get_backup — restore=True (get_restore_status)
# ---------------------------------------------------------------------------


def _make_backup_status_mock(
    backup_id: str = "my-backup",
    path: str = "/backups/my-backup",
    status: str = "SUCCESS",
) -> MagicMock:
    """
    Creates a mock backup status object without dict-like 'collections' key support.

    The production code checks `if "collections" in backup`, which calls __contains__.
    We use a plain MagicMock (no spec) and configure __contains__ to return False so
    that the `collections` branch is skipped, keeping the output predictable.
    """
    mock_status = MagicMock()
    mock_status.backup_id = backup_id
    mock_status.path = path
    mock_status.status = status
    mock_status.__contains__ = MagicMock(return_value=False)
    return mock_status


def test_get_backup_restore_true_text_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    get_backup with restore=True calls get_restore_status and prints text output.
    """
    mock_status = _make_backup_status_mock()
    mock_client_with_backup.backup.get_restore_status.return_value = mock_status

    backup_manager.get_backup(
        backend="s3",
        backup_id="my-backup",
        restore=True,
        json_output=False,
    )

    mock_client_with_backup.backup.get_restore_status.assert_called_once_with(
        backup_id="my-backup",
        backend="s3",
    )
    out = capsys.readouterr().out
    assert "my-backup" in out
    assert "/backups/my-backup" in out
    assert "SUCCESS" in out


def test_get_backup_restore_true_json_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    get_backup with restore=True and json_output=True emits JSON.
    """
    mock_status = _make_backup_status_mock()
    mock_client_with_backup.backup.get_restore_status.return_value = mock_status

    backup_manager.get_backup(
        backend="s3",
        backup_id="my-backup",
        restore=True,
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["backup_id"] == "my-backup"
    assert data["path"] == "/backups/my-backup"
    assert "SUCCESS" in data["status"]


def test_get_backup_restore_true_does_not_call_get_create_status(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    get_backup with restore=True never calls get_create_status.
    """
    mock_status = _make_backup_status_mock()
    mock_client_with_backup.backup.get_restore_status.return_value = mock_status

    backup_manager.get_backup(
        backend="s3",
        backup_id="my-backup",
        restore=True,
    )

    mock_client_with_backup.backup.get_create_status.assert_not_called()


# ---------------------------------------------------------------------------
# get_backup — restore=False with backup_id (get_create_status)
# ---------------------------------------------------------------------------


def test_get_backup_restore_false_with_id_text_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    get_backup with restore=False and a backup_id calls get_create_status and prints text.
    """
    mock_status = _make_backup_status_mock()
    mock_client_with_backup.backup.get_create_status.return_value = mock_status

    backup_manager.get_backup(
        backend="s3",
        backup_id="my-backup",
        restore=False,
        json_output=False,
    )

    mock_client_with_backup.backup.get_create_status.assert_called_once_with(
        backup_id="my-backup",
        backend="s3",
    )
    out = capsys.readouterr().out
    assert "my-backup" in out
    assert "/backups/my-backup" in out
    assert "SUCCESS" in out


def test_get_backup_restore_false_with_id_json_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    get_backup with restore=False and a backup_id and json_output=True emits JSON.
    """
    mock_status = _make_backup_status_mock()
    mock_client_with_backup.backup.get_create_status.return_value = mock_status

    backup_manager.get_backup(
        backend="s3",
        backup_id="my-backup",
        restore=False,
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["backup_id"] == "my-backup"
    assert data["path"] == "/backups/my-backup"
    assert "SUCCESS" in data["status"]


def test_get_backup_restore_false_with_id_does_not_call_get_restore_status(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    get_backup with restore=False never calls get_restore_status.
    """
    mock_status = _make_backup_status_mock()
    mock_client_with_backup.backup.get_create_status.return_value = mock_status

    backup_manager.get_backup(
        backend="s3",
        backup_id="my-backup",
        restore=False,
    )

    mock_client_with_backup.backup.get_restore_status.assert_not_called()


def test_get_backup_restore_false_no_backup_id_raises(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    get_backup with restore=False and backup_id=None raises an unsupported exception.
    """
    with pytest.raises(Exception) as exc_info:
        backup_manager.get_backup(
            backend="s3",
            backup_id=None,
            restore=False,
        )

    assert "not supported" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# cancel_backup — success
# ---------------------------------------------------------------------------


def test_cancel_backup_returns_true_text_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    cancel_backup when client.backup.cancel() returns True emits text success message.
    """
    mock_client_with_backup.backup.cancel.return_value = True

    backup_manager.cancel_backup(
        backend="s3",
        backup_id="my-backup",
        json_output=False,
    )

    mock_client_with_backup.backup.cancel.assert_called_once_with(
        backend="s3",
        backup_id="my-backup",
    )
    out = capsys.readouterr().out
    assert "my-backup" in out
    assert "cancelled successfully" in out


def test_cancel_backup_returns_true_json_output(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock, capsys
) -> None:
    """
    cancel_backup when client.backup.cancel() returns True and json_output=True emits JSON.
    """
    mock_client_with_backup.backup.cancel.return_value = True

    backup_manager.cancel_backup(
        backend="s3",
        backup_id="my-backup",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "my-backup" in data["message"]


# ---------------------------------------------------------------------------
# cancel_backup — failure
# ---------------------------------------------------------------------------


def test_cancel_backup_returns_false_raises(
    backup_manager: BackupManager, mock_client_with_backup: MagicMock
) -> None:
    """
    cancel_backup when client.backup.cancel() returns False raises an exception with current status.
    """
    mock_client_with_backup.backup.cancel.return_value = False
    mock_status = MagicMock()
    mock_status.status = "STARTED"
    mock_client_with_backup.backup.get_create_status.return_value = mock_status

    with pytest.raises(Exception) as exc_info:
        backup_manager.cancel_backup(
            backend="s3",
            backup_id="my-backup",
        )

    assert "my-backup" in str(exc_info.value)
    assert "could not be cancelled" in str(exc_info.value)
