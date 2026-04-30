import json
import click
import pytest
from unittest.mock import MagicMock

from weaviate_cli.managers.export_manager import ExportManager


@pytest.fixture
def mock_client_with_export(mock_client: MagicMock) -> MagicMock:
    """Configure mock_client with sensible defaults for ExportManager tests."""
    mock_export = MagicMock()

    # Default create return
    mock_create_return = MagicMock()
    mock_create_return.export_id = "test-export"
    mock_create_return.backend = MagicMock(value="filesystem")
    mock_create_return.path = "/exports/test-export"
    mock_create_return.status = MagicMock(value="STARTED")
    mock_create_return.started_at = None
    mock_create_return.collections = ["Movies", "Books"]
    mock_export.create.return_value = mock_create_return

    # Default get_status return
    mock_status_return = MagicMock()
    mock_status_return.export_id = "test-export"
    mock_status_return.backend = MagicMock(value="filesystem")
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
    with pytest.raises(click.ClickException) as exc_info:
        export_manager.create_export(
            export_id="test",
            backend="filesystem",
            file_format="parquet",
            include="Movies",
            exclude="Books",
        )

    assert "include" in str(exc_info.value).lower()
    assert "exclude" in str(exc_info.value).lower()


def test_create_export_unknown_backend_raises(
    export_manager: ExportManager,
) -> None:
    """create_export raises ClickException with allowed values on unknown backend."""
    with pytest.raises(click.ClickException) as exc_info:
        export_manager.create_export(
            export_id="test",
            backend="bogus",
            file_format="parquet",
        )

    msg = str(exc_info.value)
    assert "bogus" in msg
    assert "filesystem" in msg and "s3" in msg


def test_create_export_unknown_file_format_raises(
    export_manager: ExportManager,
) -> None:
    """create_export raises ClickException with allowed values on unknown file format."""
    with pytest.raises(click.ClickException) as exc_info:
        export_manager.create_export(
            export_id="test",
            backend="filesystem",
            file_format="csv",
        )

    msg = str(exc_info.value)
    assert "csv" in msg
    assert "parquet" in msg


def test_get_export_status_unknown_backend_raises(
    export_manager: ExportManager,
) -> None:
    """get_export_status raises ClickException on unknown backend."""
    with pytest.raises(click.ClickException) as exc_info:
        export_manager.get_export_status(export_id="test", backend="bogus")
    assert "bogus" in str(exc_info.value)


def test_cancel_export_unknown_backend_raises(
    export_manager: ExportManager,
) -> None:
    """cancel_export raises ClickException on unknown backend."""
    with pytest.raises(click.ClickException) as exc_info:
        export_manager.cancel_export(export_id="test", backend="bogus")
    assert "bogus" in str(exc_info.value)


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


def test_create_export_json_output(
    export_manager: ExportManager,
    mock_client_with_export: MagicMock,
    capsys,
) -> None:
    """create_export with json_output=True emits JSON with status=success."""
    mock_client_with_export.export.create.return_value.export_id = "my-export"
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert data["export_id"] == "my-export"
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


def test_create_export_no_extra_kwargs(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export does not pass config or path to the client."""
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
    )

    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    assert "config" not in call_kwargs
    assert "path" not in call_kwargs


def test_create_export_with_wait(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export passes wait_for_completion=True."""
    mock_client_with_export.export.create.return_value.status = MagicMock(
        value="SUCCESS"
    )
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
        wait=True,
    )

    call_kwargs = mock_client_with_export.export.create.call_args.kwargs
    assert call_kwargs["wait_for_completion"] is True


def test_create_export_with_wait_raises_on_non_success(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export with wait=True raises when the export finishes non-SUCCESS."""
    mock_client_with_export.export.create.return_value.status = MagicMock(
        value="FAILED"
    )

    with pytest.raises(click.ClickException) as exc_info:
        export_manager.create_export(
            export_id="my-export",
            backend="filesystem",
            file_format="parquet",
            wait=True,
        )

    assert "FAILED" in str(exc_info.value)
    assert "my-export" in str(exc_info.value)


def test_create_export_without_wait_does_not_raise_on_started(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """create_export with wait=False does not raise even if status is STARTED."""
    mock_client_with_export.export.create.return_value.status = MagicMock(
        value="STARTED"
    )
    export_manager.create_export(
        export_id="my-export",
        backend="filesystem",
        file_format="parquet",
        wait=False,
    )


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


def test_get_export_status_json_output(
    export_manager: ExportManager,
    mock_client_with_export: MagicMock,
    capsys,
) -> None:
    """get_export_status with json_output=True emits JSON."""
    mock_client_with_export.export.get_status.return_value.export_id = "my-export"
    export_manager.get_export_status(
        export_id="my-export",
        backend="filesystem",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["export_id"] == "my-export"
    assert data["status"] == "SUCCESS"
    assert data["took_in_ms"] == 1234


def test_get_export_status_passes_correct_args(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """get_export_status passes only export_id and backend to client."""
    export_manager.get_export_status(
        export_id="my-export",
        backend="s3",
    )

    mock_client_with_export.export.get_status.assert_called_once()
    call_kwargs = mock_client_with_export.export.get_status.call_args.kwargs
    assert call_kwargs["export_id"] == "my-export"
    assert "config" not in call_kwargs
    assert "path" not in call_kwargs


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
    """cancel_export passes only export_id and backend to client."""
    export_manager.cancel_export(
        export_id="my-export",
        backend="gcs",
    )

    mock_client_with_export.export.cancel.assert_called_once()
    call_kwargs = mock_client_with_export.export.cancel.call_args.kwargs
    assert call_kwargs["export_id"] == "my-export"
    assert "config" not in call_kwargs
    assert "path" not in call_kwargs


# ---------------------------------------------------------------------------
# cancel_export — failure
# ---------------------------------------------------------------------------


def test_cancel_export_failure_raises(
    export_manager: ExportManager, mock_client_with_export: MagicMock
) -> None:
    """cancel_export when client returns False raises an exception."""
    mock_client_with_export.export.cancel.return_value = False

    with pytest.raises(click.ClickException) as exc_info:
        export_manager.cancel_export(
            export_id="my-export",
            backend="filesystem",
        )

    assert "my-export" in str(exc_info.value)
    assert "could not be canceled" in str(exc_info.value)
