import pytest
from unittest.mock import MagicMock, patch
from weaviate_cli.managers.shard_manager import ShardManager


def test_get_shards_single_collection(mock_client):
    manager = ShardManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    mock_shard = MagicMock()
    mock_shard.name = "shard1"
    mock_shard.status = "READY"
    mock_shard.vector_queue_size = 0
    mock_collection.config.get_shards.return_value = [mock_shard]

    manager.get_shards("TestCollection")

    mock_client.collections.exists.assert_called_once_with("TestCollection")
    mock_client.collections.get.assert_called_once_with("TestCollection")
    mock_collection.config.get_shards.assert_called_once()


def test_get_shards_all_collections(mock_client):
    manager = ShardManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.list_all.return_value = ["Collection1", "Collection2"]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    mock_shard = MagicMock()
    mock_shard.name = "shard1"
    mock_shard.status = "READY"
    mock_shard.vector_queue_size = 0
    mock_collection.config.get_shards.return_value = [mock_shard]

    manager.get_shards(None)

    assert mock_client.collections.get.call_count == 2
    mock_collection.config.get_shards.assert_called()


def test_get_shards_nonexistent_collection(mock_client):
    manager = ShardManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = False

    with pytest.raises(Exception) as exc_info:
        manager.get_shards("NonexistentCollection")
    assert "does not exist" in str(exc_info.value)


def test_update_shards_single_collection(mock_client):
    manager = ShardManager(mock_client)
    mock_collection = MagicMock()
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True
    mock_client.collections.get.return_value = mock_collection

    mock_shard = MagicMock()
    mock_shard.name = "shard1"
    mock_collection.config.get_shards.return_value = [mock_shard]

    manager.update_shards("READY", "TestCollection", "shard1", False)

    mock_collection.config.update_shards.assert_called_once_with("READY", ["shard1"])


def test_update_shards_all_collections(mock_client):
    manager = ShardManager(mock_client)
    mock_collection = MagicMock()
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.list_all.return_value = ["Collection1", "Collection2"]
    mock_client.collections.get.return_value = mock_collection

    mock_shard = MagicMock()
    mock_shard.name = "shard1"
    mock_collection.config.get_shards.return_value = [mock_shard]

    manager.update_shards("READY", None, None, True)

    assert mock_client.collections.get.call_count == 2
    assert mock_collection.config.update_shards.call_count == 2


def test_update_shards_invalid_shard(mock_client):
    manager = ShardManager(mock_client)
    mock_collection = MagicMock()
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True
    mock_client.collections.get.return_value = mock_collection

    mock_shard = MagicMock()
    mock_shard.name = "shard1"
    mock_collection.config.get_shards.return_value = [mock_shard]

    with pytest.raises(Exception) as exc_info:
        manager.update_shards("READY", "TestCollection", "invalid_shard", False)
    assert "does not exist" in str(exc_info.value)


def test_update_shards_invalid_all_flags(mock_client):
    manager = ShardManager(mock_client)

    with pytest.raises(Exception):
        manager.update_shards("READY", "TestCollection", None, True)

    with pytest.raises(Exception):
        manager.update_shards("READY", None, "shard1", True)
