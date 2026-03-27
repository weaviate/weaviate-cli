"""Integration tests verifying that DataManager.create_data returns a usable
Collection object for both single-tenant and multi-tenant collections.

Regression guard for a bug where parallel multi-tenant ingestion returned the
base collection (without tenant context), causing ``len(collection)`` and
``batch.wait_for_vector_indexing()`` to fail with:

    "class X has multi-tenancy enabled, but request was without tenant"
"""

import pytest
import weaviate
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.config_manager import ConfigManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate_cli.managers.tenant_manager import TenantManager

NUM_OBJECTS = 20
NUM_TENANTS = 3


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
def tenant_manager(client: weaviate.WeaviateClient) -> TenantManager:
    return TenantManager(client)


def test_create_data_returns_usable_collection_single_tenant(
    client: weaviate.WeaviateClient,
    collection_manager: CollectionManager,
    data_manager: DataManager,
):
    """For a non-MT collection, the returned collection must support len() and
    batch.wait_for_vector_indexing() without errors."""
    collection_name = "TestReturnCollSingleTenant"

    try:
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)

        collection_manager.create_collection(
            collection=collection_name,
            vectorizer="none",
            replication_factor=1,
            async_enabled=True,
        )

        returned_col = data_manager.create_data(
            collection=collection_name,
            limit=NUM_OBJECTS,
            consistency_level="one",
            randomize=True,
            skip_seed=True,
            vector_dimensions=128,
        )

        # The returned collection must be usable for these operations
        returned_col.batch.wait_for_vector_indexing()
        assert (
            len(returned_col) == NUM_OBJECTS
        ), f"Expected {NUM_OBJECTS} objects via returned collection, got {len(returned_col)}"
    finally:
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)


def test_create_data_returns_usable_collection_multitenant_sequential(
    client: weaviate.WeaviateClient,
    collection_manager: CollectionManager,
    data_manager: DataManager,
    tenant_manager: TenantManager,
):
    """For a multi-tenant collection with sequential ingestion (parallel_workers=1),
    the returned collection must carry tenant context so len() works."""
    collection_name = "TestReturnCollMTSeq"

    try:
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)

        collection_manager.create_collection(
            collection=collection_name,
            vectorizer="none",
            replication_factor=1,
            async_enabled=True,
            multitenant=True,
        )

        tenant_manager.create_tenants(
            collection=collection_name,
            number_tenants=NUM_TENANTS,
        )

        returned_col = data_manager.create_data(
            collection=collection_name,
            limit=NUM_OBJECTS,
            consistency_level="one",
            randomize=True,
            skip_seed=True,
            vector_dimensions=128,
            parallel_workers=1,
        )

        # Must not raise "multi-tenancy enabled, but request was without tenant"
        returned_col.batch.wait_for_vector_indexing()
        count = len(returned_col)
        assert (
            count == NUM_OBJECTS
        ), f"Expected {NUM_OBJECTS} objects via returned collection, got {count}"
    finally:
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)


def test_create_data_returns_usable_collection_multitenant_parallel(
    client: weaviate.WeaviateClient,
    collection_manager: CollectionManager,
    data_manager: DataManager,
    tenant_manager: TenantManager,
):
    """For a multi-tenant collection with parallel ingestion (parallel_workers>1),
    the returned collection must carry tenant context so len() works.

    This is the exact scenario that was broken: parallel mode never assigned the
    tenant-scoped collection back to the return variable."""
    collection_name = "TestReturnCollMTParallel"

    try:
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)

        collection_manager.create_collection(
            collection=collection_name,
            vectorizer="none",
            replication_factor=1,
            async_enabled=True,
            multitenant=True,
        )

        tenant_manager.create_tenants(
            collection=collection_name,
            number_tenants=NUM_TENANTS,
        )

        returned_col = data_manager.create_data(
            collection=collection_name,
            limit=NUM_OBJECTS,
            consistency_level="one",
            randomize=True,
            skip_seed=True,
            vector_dimensions=128,
            parallel_workers=NUM_TENANTS,
        )

        # Must not raise "multi-tenancy enabled, but request was without tenant"
        returned_col.batch.wait_for_vector_indexing()
        count = len(returned_col)
        assert (
            count == NUM_OBJECTS
        ), f"Expected {NUM_OBJECTS} objects via returned collection, got {count}"
    finally:
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)
