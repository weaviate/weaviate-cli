import pytest
import weaviate
import click
from click.testing import CliRunner
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.config_manager import ConfigManager
from weaviate_cli.managers.shard_manager import ShardManager
from weaviate_cli.managers.data_manager import DataManager
import weaviate.classes.config as wvc


@pytest.fixture
def client() -> weaviate.Client:
    config = ConfigManager()
    return weaviate.connect_to_local(
        host=config.config["host"],
        port=int(config.config["http_port"]),
        grpc_port=int(config.config["grpc_port"]),
    )


@pytest.fixture
def collection_manager(client: weaviate.Client) -> CollectionManager:
    return CollectionManager(client)


@pytest.fixture
def shard_manager(client: weaviate.Client) -> ShardManager:
    return ShardManager(client)


@pytest.fixture
def data_manager(client: weaviate.Client) -> DataManager:
    return DataManager(client)


def test_collection_lifecycle(collection_manager: CollectionManager):
    try:
        # Create collection
        collection_manager.create_collection(
            collection="Movies",
            vector_index="hnsw",
            inverted_index="timestamp",
            replication_factor=1,
            training_limit=100000,
            shards=1,
            async_enabled=True,
            multitenant=False,
            auto_tenant_creation=False,
            auto_tenant_activation=False,
            vectorizer="contextionary",
            force_auto_schema=True,
            replication_deletion_strategy=None,
        )

        # Verify collection exists
        assert collection_manager.client.collections.exists("Movies")

        # Update collection
        collection_manager.update_collection(
            collection="Movies",
            description="Updated movie collection",
            vector_index="hnsw_pq",
            training_limit=100000,
            async_enabled=False,
            auto_tenant_creation=None,
            auto_tenant_activation=None,
            replication_deletion_strategy=None,
        )

        # Get collection config to verify update
        movies_collection = collection_manager.client.collections.get("Movies")
        config = movies_collection.config.get()
        assert config.description == "Updated movie collection"
        assert config.replication_config.async_enabled is False

    finally:
        # Delete collection
        if collection_manager.client.collections.exists("Movies"):
            collection_manager.delete_collection(collection="Movies", all=False)
        assert not collection_manager.client.collections.exists("Movies")


def test_multiple_collections(collection_manager: CollectionManager):
    try:
        # Create multiple collections
        collections = ["Movies", "Books", "Music"]

        for col in collections:
            collection_manager.create_collection(
                collection=col,
                vector_index="flat",
                replication_factor=1,
                inverted_index=None,
                shards=2,
                training_limit=100000,
                async_enabled=True,
                multitenant=False,
                auto_tenant_creation=False,
                auto_tenant_activation=False,
                vectorizer="contextionary",
                force_auto_schema=True,
                replication_deletion_strategy=None,
            )
            assert collection_manager.client.collections.exists(col)
    finally:
        # Delete all collections
        collection_manager.delete_collection(collection="", all=True)

        # Verify all collections deleted
        for col in collections:
            assert not collection_manager.client.collections.exists(col)


def test_shard_operations(
    collection_manager: CollectionManager,
    shard_manager: ShardManager,
    data_manager: DataManager,
):
    try:
        # Create collection with multiple shards
        collection_manager.create_collection(
            collection="Movies",
            vector_index="hnsw",
            inverted_index="null",
            replication_factor=1,
            training_limit=1000,
            shards=3,
            async_enabled=True,
            multitenant=False,
            auto_tenant_creation=False,
            auto_tenant_activation=False,
            vectorizer="transformers",
            force_auto_schema=True,
            replication_deletion_strategy=None,
        )

        # Get shard info
        movies_collection = collection_manager.client.collections.get("Movies")

        data_manager.ingest_data(
            collection="Movies",
            limit=1000,
            consistency_level="one",
            randomize=False,
            auto_tenants=0,
        )

        shards = movies_collection.config.get_shards()
        assert len(shards) == 3

        objects = movies_collection.with_consistency_level(
            wvc.ConsistencyLevel.ONE
        ).query.fetch_objects(limit=1000)
        assert len(objects.objects) == 1000
    finally:
        # Clean up
        collection_manager.delete_collection(collection="Movies", all=False)


def test_error_handling(collection_manager: CollectionManager):
    try:
        # Test creating duplicate collection
        collection_manager.create_collection(
            collection="TestCol",
            vector_index="hnsw",
            inverted_index=None,
            replication_factor=1,
            training_limit=100000,
            shards=1,
            async_enabled=True,
            multitenant=False,
            auto_tenant_creation=False,
            auto_tenant_activation=False,
            vectorizer="transformers",
            force_auto_schema=True,
            replication_deletion_strategy=None,
        )

        with pytest.raises(Exception):
            collection_manager.create_collection(
                collection="TestCol",
                vector_index="hnsw",
                inverted_index=None,
                replication_factor=1,
                training_limit=100000,
                shards=1,
                async_enabled=True,
                multitenant=False,
                auto_tenant_creation=False,
                auto_tenant_activation=False,
                vectorizer="transformers",
                force_auto_schema=True,
                replication_deletion_strategy=None,
            )
    finally:
        # Test deleting non-existent collection
        collection_manager.delete_collection(collection="TestCol", all=False)

    with pytest.raises(Exception):
        collection_manager.delete_collection(collection="NonExistentCol", all=False)

    # Test updating non-existent collection
    with pytest.raises(Exception):
        collection_manager.update_collection(
            collection="NonExistentCol",
            description="Updated description",
            vector_index="hnsw",
            training_limit=100000,
            async_enabled=True,
            auto_tenant_creation=None,
            auto_tenant_activation=None,
            replication_deletion_strategy=None,
        )
