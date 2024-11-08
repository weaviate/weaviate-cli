import pytest
from unittest.mock import MagicMock, patch
from weaviate_cli.managers.data_manager import DataManager
import weaviate.classes.config as wvc


def test_ingest_data(mock_client):
    manager = DataManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    mock_collection.config.get.return_value = MagicMock(
        multi_tenancy_config=MagicMock(auto_tenant_creation=True)
    )

    # Test data ingestion
    manager.create_data(
        collection="TestCollection",
        limit=100,
        randomize=True,
    )

    mock_client.collections.get.assert_called_once_with("TestCollection")


def test_update_data(mock_client):
    manager = DataManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    # Test data update
    manager.update_data(
        collection="TestCollection",
        limit=100,
        randomize=True,
    )

    mock_client.collections.get.assert_called_once_with("TestCollection")


def test_delete_data(mock_client):
    manager = DataManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    # Test data deletion
    manager.delete_data(
        collection="TestCollection",
        limit=100,
    )

    mock_client.collections.get.assert_called_once_with("TestCollection")
