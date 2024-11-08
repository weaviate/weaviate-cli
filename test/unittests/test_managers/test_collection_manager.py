import pytest
from unittest.mock import MagicMock, patch
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.managers.collection_manager import CollectionManager
import weaviate.classes.config as wvc


def test_create_collection(mock_client):
    # Setup the mock chain
    mock_collections = MagicMock()
    mock_client.collections = mock_collections

    # Mock exists to return False first (collection doesn't exist)
    # then True after creation (collection exists)
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    # Test successful collection creation
    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        async_enabled=True,
    )

    # Verify the collection creation was called with correct parameters
    assert (
        mock_collections.exists.call_count == 2
    )  # Called twice: before and after creation
    mock_collections.create.assert_called_once()

    # Verify the create call parameters
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["replication_config"].factor == 3
    assert create_call_kwargs["replication_config"].asyncEnabled is True
    assert create_call_kwargs["sharding_config"] is None  # since shards = 1


def test_create_existing_collection(mock_client):
    # Setup
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    manager = CollectionManager(mock_client)
    mock_collections.exists.return_value = True

    # Test that it raises the expected exception
    with pytest.raises(Exception) as exc_info:
        manager.create_collection(
            collection="TestCollection",
        )

    # Verify the error message
    assert (
        str(exc_info.value)
        == "Error: Collection 'TestCollection' already exists in Weaviate. Delete using <delete collection> command."
    )

    # Verify exists was called but create was not
    mock_collections.exists.assert_called_once_with("TestCollection")
    mock_collections.create.assert_not_called()


def test_create_collection_failure(mock_client):
    # Setup
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.return_value = False
    mock_client.collections.create.side_effect = WeaviateConnectionError
    manager = CollectionManager(mock_client)

    # Test that it raises the expected exception
    with pytest.raises(Exception) as exc_info:
        manager.create_collection(
            collection="TestCollection",
        )

    # Verify the error message
    assert (
        "Error creating Collection 'TestCollection': Connection to Weaviate failed"
        in str(exc_info.value)
    )
    # Verify exists was called but create was not
    mock_collections.exists.assert_called_once_with("TestCollection")
    mock_collections.create.assert_called_once()


def test_delete_single_collection(mock_client):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    manager = CollectionManager(mock_client)
    mock_client.collections.exists.side_effect = [True, False]

    # Test successful collection deletion
    manager.delete_collection(collection="TestCollection", all=False)

    mock_client.collections.delete.assert_called_once_with("TestCollection")


def test_delete_all_collections(mock_client):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    manager = CollectionManager(mock_client)
    mock_client.collections.list_all.return_value = [
        "Collection1",
        "Collection2",
        "Collection3",
    ]

    # Test deletion of all collections
    manager.delete_collection(collection="", all=True)

    assert mock_client.collections.delete.call_count == 3


def test_delete_nonexistent_collection(mock_client):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    manager = CollectionManager(mock_client)
    mock_client.collections.exists.return_value = False

    with pytest.raises(Exception) as exc_info:
        manager.delete_collection(collection="TestCollection", all=False)


def test_update_collection(mock_client):

    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=True, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)
    # Test collection update
    manager.update_collection(
        collection="TestCollection",
        description="Updated description",
        vector_index="hnsw",
        async_enabled=True,
        auto_tenant_creation=True,
        auto_tenant_activation=True,
        replication_deletion_strategy="delete_on_conflict",
    )

    mock_collection.config.update.assert_called_once()
    assert (
        mock_collection.config.update.call_args.kwargs["description"]
        == "Updated description"
    )
    assert (
        mock_collection.config.update.call_args.kwargs["replication_config"].factor == 3
    )
    assert (
        mock_collection.config.update.call_args.kwargs[
            "multi_tenancy_config"
        ].autoTenantCreation
        is True
    )
    assert (
        mock_collection.config.update.call_args.kwargs[
            "multi_tenancy_config"
        ].autoTenantActivation
        is True
    )


def test_update_nonexistent_collection(mock_client):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    manager = CollectionManager(mock_client)
    mock_client.collections.exists.return_value = False

    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            description="Updated description",
            vector_index="hnsw",
            training_limit=10000,
            async_enabled=True,
            auto_tenant_creation=True,
            auto_tenant_activation=True,
            replication_deletion_strategy="delete_on_conflict",
        )

    assert "Error: Collection 'TestCollection' does not exist in Weaviate." in str(
        exc_info.value
    )

    mock_collections.exists.assert_called_once_with("TestCollection")
    mock_collections.update.assert_not_called()
