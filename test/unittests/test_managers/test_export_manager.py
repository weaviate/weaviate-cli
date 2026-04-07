import json
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from weaviate_cli.managers.export_manager import ExportManager


@pytest.fixture
def mock_client_with_export(mock_client: MagicMock) -> MagicMock:
    """Configure mock_client with sensible defaults for ExportManager tests."""
    mock_export = MagicMock()

    # Default create return
    mock_create_return = MagicMock()
    mock_create_return.export_id = "test-export"
    mock_create_return.backend = "filesystem"
    mock_create_return.path = "/exports/test-export"
    mock_create_return.status = MagicMock(value="STARTED")
    mock_create_return.started_at = None
    mock_create_return.collections = ["Movies", "Books"]
    mock_export.create.return_value = mock_create_return

    # Default get_status return
    mock_status_return = MagicMock()
    mock_status_return.export_id = "test-export"
    mock_status_return.backend = "filesystem"
    mock_status_return.path = "/exports/test-export"
    mock_status_return.status = MagicMock(value="SUCCESS")
    mock_status_return.started_at = None
    mock_status_return.collections = ["Movies"]
    mock_status_return.error = None
    mock_status_return.took_in_ms = 1234
    mock_status_return.shard_status = None
    mock_export.get_status.return_value = mock_status_return

    # Default cancel return
    mock_export.cancel.return_value = True

    mock_client.export = mock_export
    return mock_client


@pytest.fixture
def export_manager(mock_client_with_export: MagicMock) -> ExportManager:
    return ExportManager(mock_client_with_export)


# ---------------------------------------------------------------------------
# create_export — validation
# ---------------------------------------------------------------------------


def test_create_export_include_and_exclude_raises(
    export_manager: ExportManager,
) -> None:
    """create_export raises when both include and exclude are specified."""
    with pytest.raises(Exception) as exc_info:
        export_manager.create_export(
            export_id="test",
            backend="filesystem",
            file_format="parquet",
            include="Movies",
            exclude="Books",
        )

    assert "include" in str(exc_info.value).lower()
    assert "exclude" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# create_export — success
# ---------------------------------------------------------------------------


def test_create_export_text_output(export_manager: ExportManager, capsys) -> None:
    """create_export emits text success message."""
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "my-export" in out
    assert "created successfully" in out


def test_create_export_json_output(export_manager: ExportManager, capsys) -> None:
    """create_export with json_output=True emits JSON with status=success."""
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert data["export_id"] == "test-export"
    assert data["collections"] == ["Movies", "Books"]


# ---------------------------------------------------------------------------
# create_export — argument passing
# ---------------------------------------------------------------------------


def test_create_export_passes_correct_args_with_include(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export passes include_collections as a list."""
    export_manager.create_export(
        export_id="my-export",
        backend="s3",
        file_format="parquet",
        include="Movies,Books",
    )

    mock_client_with_export.export.create.assert_called_once()
    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    assert call_kwargs["export_id"] == "my-export"
    assert call_kwargs["include_collections"] == ["Movies", "Books"]
    assert call_kwargs["exclude_collections"] is None


def test_create_export_passes_correct_args_with_exclude(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export passes exclude_collections as a list."""
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
        exclude="Movies",
    )

    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    assert call_kwargs["include_collections"] is None
    assert call_kwargs["exclude_collections"] == ["Movies"]


def test_create_export_passes_none_collections_when_not_specified(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export passes None for both when neither is specified."""
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
    )

    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    assert call_kwargs["include_collections"] is None
    assert call_kwargs["exclude_collections"] is None


def test_create_export_passes_config_with_path(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export passes ExportConfig when path is set."""
    export_manager.create_export(
        export_id="my-export",
        backend="s3",
        file_format="parquet",
        path="/my/path",
    )

    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    config = call_kwargs["config"]
    assert config is not None
    assert config.path == "/my/path"


def test_create_export_no_config_when_path_none(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export passes config=None when path is not set."""
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
    )

    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    assert call_kwargs["config"] is None


def test_create_export_with_wait(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export passes wait_for_completion=True."""
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
        wait=True,
    )

    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    assert call_kwargs["wait_for_completion"] is True


# ---------------------------------------------------------------------------
# get_export_status — success
# ---------------------------------------------------------------------------


def test_get_export_status_text_output(export_manager: ExportManager, capsys) -> None:
    """get_export_status emits text output."""
    export_manager.get_export_status(
        export_id="my-export",
        backend="filesystem",
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "test-export" in out
    assert "SUCCESS" in out
    assert "1234" in out


def test_get_export_status_json_output(export_manager: ExportManager, capsys) -> None:
    """get_export_status with json_output=True emits JSON."""
    export_manager.get_export_status(
        export_id="my-export",
        backend="filesystem",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["export_id"] == "test-export"
    assert data["status"] == "SUCCESS"
    assert data["took_in_ms"] == 1234


def test_get_export_status_passes_correct_args(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """get_export_status passes correct args to client, wrapping path in ExportConfig."""
    export_manager.get_export_status(
        export_id="my-export",
        backend="s3",
        path="/my/path",
    )

    mock_client_with_export.export.get_status.assert_called_once()
    call_kwargs = mock_client_with_export.export.get_status.call_args.kwargs
    assert call_kwargs["export_id"] == "my-export"
    assert call_kwargs["config"] is not None
    assert call_kwargs["config"].path == "/my/path"


def test_get_export_status_no_config_when_path_none(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """get_export_status passes config=None when path is not set."""
    export_manager.get_export_status(
        export_id="my-export",
        backend="filesystem",
    )

    call_kwargs = mock_client_with_export.export.get_status.call_args.kwargs
    assert call_kwargs["config"] is None


def test_get_export_status_with_shard_status_json(
    export_manager: ExportManager, mock_client_with_export: MagicMock, capsys
) -> None:
    """get_export_status includes shard_status in JSON output when present."""
    mock_shard_progress = MagicMock()
    mock_shard_progress.status = MagicMock(value="SUCCESS")
    mock_shard_progress.objects_exported = 500
    mock_shard_progress.error = None
    mock_shard_progress.skip_reason = None

    mock_status = mock_client_with_export.export.get_status.return_value
    mock_status.shard_status = {"Movies": {"shard1": mock_shard_progress}}

    export_manager.get_export_status(
        export_id="my-export",
        backend="filesystem",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert "shard_status" in data
    assert data["shard_status"]["Movies"]["shard1"]["status"] == "SUCCESS"
    assert data["shard_status"]["Movies"]["shard1"]["objects_exported"] == 500


def test_get_export_status_with_error_json(
    export_manager: ExportManager, mock_client_with_export: MagicMock, capsys
) -> None:
    """get_export_status includes error in JSON output when present."""
    mock_status = mock_client_with_export.export.get_status.return_value
    mock_status.status = MagicMock(value="FAILED")
    mock_status.error = "Something went wrong"

    export_manager.get_export_status(
        export_id="my-export",
        backend="filesystem",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "FAILED"
    assert data["error"] == "Something went wrong"


# ---------------------------------------------------------------------------
# cancel_export — success
# ---------------------------------------------------------------------------


def test_cancel_export_success_text_output(
    export_manager: ExportManager, capsys
) -> None:
    """cancel_export when successful emits text success message."""
    export_manager.cancel_export(
        export_id="my-export",
        backend="filesystem",
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "my-export" in out
    assert "canceled successfully" in out


def test_cancel_export_success_json_output(
    export_manager: ExportManager, capsys
) -> None:
    """cancel_export when successful and json_output=True emits JSON."""
    export_manager.cancel_export(
        export_id="my-export",
        backend="filesystem",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "my-export" in data["message"]


def test_cancel_export_passes_correct_args(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """cancel_export passes correct args to client, wrapping path in ExportConfig."""
    export_manager.cancel_export(
        export_id="my-export",
        backend="gcs",
        path="/my/path",
    )

    mock_client_with_export.export.cancel.assert_called_once()
    call_kwargs = mock_client_with_export.export.cancel.call_args.kwargs
    assert call_kwargs["export_id"] == "my-export"
    assert call_kwargs["config"] is not None
    assert call_kwargs["config"].path == "/my/path"


def test_cancel_export_no_config_when_path_none(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """cancel_export passes config=None when path is not set."""
    export_manager.cancel_export(
        export_id="my-export",
        backend="filesystem",
    )

    call_kwargs = mock_client_with_export.export.cancel.call_args.kwargs
    assert call_kwargs["config"] is None


# ---------------------------------------------------------------------------
# cancel_export — failure
# ---------------------------------------------------------------------------


def test_cancel_export_failure_raises(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """cancel_export when client returns False raises an exception."""
    mock_client_with_export.export.cancel.return_value = False

    with pytest.raises(Exception) as exc_info:
        export_manager.cancel_export(
            export_id="my-export",
            backend="filesystem",
        )

    assert "my-export" in str(exc_info.value)
    assert "could not be canceled" in str(exc_info.value)
