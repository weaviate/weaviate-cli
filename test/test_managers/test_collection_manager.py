import pytest
from unittest.mock import MagicMock, patch
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
        async_enabled=True,
        vector_index="hnsw",
        inverted_index=None,
        training_limit=10000,
        multitenant=False,
        auto_tenant_creation=False,
        auto_tenant_activation=False,
        auto_schema=False,
        shards=1,
        vectorizer=None
    )
    
    # Verify the collection creation was called with correct parameters
    assert mock_collections.exists.call_count == 2  # Called twice: before and after creation
    mock_collections.create.assert_called_once()
    
    # Verify the create call parameters
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs['name'] == "TestCollection"
    assert create_call_kwargs['replication_config'].factor == 3
    assert create_call_kwargs['replication_config'].asyncEnabled is True
    assert create_call_kwargs['sharding_config'] is None  # since shards = 1

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
            replication_factor=3,
            async_enabled=True,
            vector_index="hnsw",
            inverted_index=None,
            training_limit=10000,
            multitenant=False,
            auto_tenant_creation=False,
            auto_tenant_activation=False,
            auto_schema=False,
            shards=1,
            vectorizer=None
        )
    
    # Verify the error message
    assert str(exc_info.value) == "Error: Collection 'TestCollection' already exists in Weaviate. Delete using <delete collection> command."
    
    # Verify exists was called but create was not
    mock_collections.exists.assert_called_once_with("TestCollection")
    mock_collections.create.assert_not_called()

def test_create_collection_failure(mock_client):
    #Setup
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client
    manager = CollectionManager(mock_client)
    mock_collections.create.side_effect = Exception("Test Error")
    
def test_delete_collection(mock_client):
    manager = CollectionManager(mock_client)
    mock_client.collections.exists.return_value = True
    
    # Test successful collection deletion
    manager.delete_collection(collection="TestCollection", all=False)
    
    mock_client.collections.delete.assert_called_once_with("TestCollection")

def test_delete_all_collections(mock_client):
    manager = CollectionManager(mock_client)
    mock_client.collections.list_all.return_value = ["Collection1", "Collection2"]
    
    # Test deletion of all collections
    manager.delete_collection(collection="", all=True)
    
    assert mock_client.collections.delete.call_count == 2

def test_update_collection(mock_client):
    manager = CollectionManager(mock_client)
    mock_client.collections.exists.return_value = True
    
    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=True,
            auto_tenant_creation=False,
            auto_tenant_activation=False
        )
    )
    
    # Test collection update
    manager.update_collection(
        collection="TestCollection",
        description="Updated description",
        vector_index="hnsw",
        training_limit=10000,
        async_enabled=True,
        auto_tenant_creation=True,
        auto_tenant_activation=True
    )
    
    mock_collection.config.update.assert_called_once() 