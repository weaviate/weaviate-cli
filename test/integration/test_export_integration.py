import json
import click
import pytest
import weaviate
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.config_manager import ConfigManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate_cli.managers.export_manager import ExportManager


EXPORT_COLLECTION = "ExportTestCollection"


@pytest.fixture
def client() -> weaviate.WeaviateClient:
    config = ConfigManager()
    return config.get_client()


@pytest.fixture
def collection_manager(client: weaviate.WeaviateClient) -> CollectionManager:
    return CollectionManager(client)


@pytest.fixture
def data_manager(client: weaviate.WeaviateClient) -> DataManager:
    return DataManager(client)


@pytest.fixture
def export_manager(client: weaviate.WeaviateClient) -> ExportManager:
    return ExportManager(client)


@pytest.fixture
def setup_collection(collection_manager, data_manager):
    """Create a collection with data for export tests."""
    try:
        collection_manager.create_collection(
            collection=EXPORT_COLLECTION,
            replication_factor=1,
            vectorizer="none",
            force_auto_schema=True,
        )
        data_manager.create_data(
            collection=EXPORT_COLLECTION,
            limit=100,
            randomize=True,
            consistency_level="one",
        )
        yield
    finally:
        if collection_manager.client.collections.exists(EXPORT_COLLECTION):
            collection_manager.delete_collection(collection=EXPORT_COLLECTION)


def test_create_export_and_get_status(
    export_manager: ExportManager, setup_collection, capsys
):
    """Test creating an export and getting its status."""
    export_manager.create_export(
        export_id="integration-test-export",
        backend="s3",
        file_format="parquet",
        include=EXPORT_COLLECTION,
        wait=True,
        json_output=False,
    )

    out = capsys.readouterr().out
    assert "integration-test-export" in out
    assert "created successfully" in out

    export_manager.get_export_status(
        export_id="integration-test-export",
        backend="s3",
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["export_id"] == "integration-test-export"
    assert data["status"] == "SUCCESS"
    assert EXPORT_COLLECTION in data["collections"]
    assert "shard_status" in data


def test_create_export_json_output(
    export_manager: ExportManager, setup_collection, capsys
):
    """Test creating an export with JSON output."""
    export_manager.create_export(
        export_id="integration-json-export",
        backend="s3",
        file_format="parquet",
        wait=True,
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert data["export_id"] == "integration-json-export"
    assert data["export_status"] == "SUCCESS"


def test_create_export_with_exclude(
    export_manager: ExportManager, setup_collection, capsys
):
    """Test creating an export with exclude filter."""
    export_manager.create_export(
        export_id="integration-exclude-export",
        backend="s3",
        file_format="parquet",
        exclude=EXPORT_COLLECTION,
        wait=True,
        json_output=True,
    )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert EXPORT_COLLECTION not in data.get("collections", [])


def test_create_export_include_and_exclude_raises(
    export_manager: ExportManager, setup_collection
):
    """Test that specifying both include and exclude raises an error."""
    with pytest.raises(click.ClickException) as exc_info:
        export_manager.create_export(
            export_id="should-fail",
            backend="s3",
            file_format="parquet",
            include=EXPORT_COLLECTION,
            exclude="OtherCollection",
        )
    assert "include" in str(exc_info.value).lower()
    assert "exclude" in str(exc_info.value).lower()


def test_cancel_export(export_manager: ExportManager, setup_collection, capsys):
    """Test canceling an export."""
    # Create export without waiting
    export_manager.create_export(
        export_id="integration-cancel-export",
        backend="s3",
        file_format="parquet",
        wait=False,
    )
    capsys.readouterr()  # Clear output

    # Try to cancel — may succeed or fail depending on timing. Only tolerate
    # the specific "could not be canceled" path (export already finished);
    # anything else is a real failure.
    try:
        export_manager.cancel_export(
            export_id="integration-cancel-export",
            backend="s3",
            json_output=True,
        )
    except click.ClickException as e:
        assert "could not be canceled" in str(e)
        return

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
