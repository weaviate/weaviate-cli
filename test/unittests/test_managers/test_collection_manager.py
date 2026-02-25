import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.managers.collection_manager import CollectionManager
import weaviate.classes.config as wvc


@pytest.fixture
def mock_wvc_object_ttl():
    """Mock the ObjectTTL configuration classes."""
    with (
        patch.object(wvc.Configure, "ObjectTTL", create=True) as mock_configure_ttl,
        patch.object(wvc.Reconfigure, "ObjectTTL", create=True) as mock_reconfigure_ttl,
    ):
        # Setup Configure.ObjectTTL methods
        mock_configure_ttl.delete_by_creation_time = MagicMock(return_value=MagicMock())
        mock_configure_ttl.delete_by_update_time = MagicMock(return_value=MagicMock())
        mock_configure_ttl.delete_by_date_property = MagicMock(return_value=MagicMock())
        mock_configure_ttl.disable = MagicMock(return_value=MagicMock())

        # Setup Reconfigure.ObjectTTL methods
        mock_reconfigure_ttl.delete_by_creation_time = MagicMock(
            return_value=MagicMock()
        )
        mock_reconfigure_ttl.delete_by_update_time = MagicMock(return_value=MagicMock())
        mock_reconfigure_ttl.delete_by_date_property = MagicMock(
            return_value=MagicMock()
        )
        mock_reconfigure_ttl.disable = MagicMock(return_value=MagicMock())

        yield {"configure": mock_configure_ttl, "reconfigure": mock_reconfigure_ttl}


def test_create_collection(mock_client, mock_wvc_object_ttl):
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


def test_create_existing_collection(mock_client, mock_wvc_object_ttl):
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


def test_create_collection_failure(mock_client, mock_wvc_object_ttl):
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


def test_update_collection(mock_client, mock_wvc_object_ttl):

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
        replication_factor=5,
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
        mock_collection.config.update.call_args.kwargs["replication_config"].factor == 5
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


def test_create_collection_with_ttl_create_type(mock_client, mock_wvc_object_ttl):
    """Test creating a collection with object TTL using 'create' type."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        object_ttl_type="create",
        object_ttl_time=3600,
        object_ttl_filter_expired=True,
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["object_ttl_config"] is not None
    # Verify the correct TTL method was called
    mock_wvc_object_ttl["configure"].delete_by_creation_time.assert_called_once_with(
        time_to_live=3600,
        filter_expired_objects=True,
    )


def test_create_collection_with_ttl_update_type(mock_client, mock_wvc_object_ttl):
    """Test creating a collection with object TTL using 'update' type."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        object_ttl_type="update",
        object_ttl_time=7200,
        object_ttl_filter_expired=False,
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["object_ttl_config"] is not None
    # Verify the correct TTL method was called
    mock_wvc_object_ttl["configure"].delete_by_update_time.assert_called_once_with(
        time_to_live=7200,
        filter_expired_objects=False,
    )


def test_create_collection_with_ttl_property_type(mock_client, mock_wvc_object_ttl):
    """Test creating a collection with object TTL using 'property' type."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        object_ttl_type="property",
        object_ttl_time=86400,
        object_ttl_filter_expired=True,
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["object_ttl_config"] is not None
    # Verify the correct TTL method was called
    mock_wvc_object_ttl["configure"].delete_by_date_property.assert_called_once_with(
        property_name="releaseDate",
        ttl_offset=86400,
        filter_expired_objects=True,
    )


def test_create_collection_with_ttl_property_type_custom_property_name(
    mock_client, mock_wvc_object_ttl
):
    """Test creating a collection with object TTL 'property' type and custom property name."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        object_ttl_type="property",
        object_ttl_time=86400,
        object_ttl_filter_expired=True,
        object_ttl_property_name="expiresAt",
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["object_ttl_config"] is not None
    mock_wvc_object_ttl["configure"].delete_by_date_property.assert_called_once_with(
        property_name="expiresAt",
        ttl_offset=86400,
        filter_expired_objects=True,
    )


def test_create_collection_object_ttl_property_name_guardrail(mock_client):
    """Test that object_ttl_property_name is rejected when object_ttl_type is not 'property'."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = False

    manager = CollectionManager(mock_client)

    with pytest.raises(Exception) as exc_info:
        manager.create_collection(
            collection="TestCollection",
            replication_factor=3,
            vector_index="hnsw",
            object_ttl_type="create",
            object_ttl_time=3600,
            object_ttl_property_name="expiresAt",
        )

    assert (
        "object_ttl_property_name is only valid when object_ttl_type is 'property'"
        in str(exc_info.value)
    )
    mock_collections.create.assert_not_called()


def test_create_collection_with_ttl_property_type_default_property_name(
    mock_client, mock_wvc_object_ttl
):
    """Test that object_ttl_type=property with no property name uses default 'releaseDate'."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        object_ttl_type="property",
        object_ttl_time=86400,
        object_ttl_property_name=None,
    )

    mock_collections.create.assert_called_once()
    mock_wvc_object_ttl["configure"].delete_by_date_property.assert_called_once_with(
        property_name="releaseDate",
        ttl_offset=86400,
        filter_expired_objects=None,
    )


def test_create_collection_with_ttl_time_zero(mock_client, mock_wvc_object_ttl):
    """Test creating a collection with TTL time=0 applies TTL config (not treated as unset)."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        object_ttl_type="property",
        object_ttl_time=0,
        object_ttl_filter_expired=False,
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["object_ttl_config"] is not None
    mock_wvc_object_ttl["configure"].delete_by_date_property.assert_called_once_with(
        property_name="releaseDate",
        ttl_offset=0,
        filter_expired_objects=False,
    )


def test_create_collection_without_ttl_time(mock_client, mock_wvc_object_ttl):
    """Test creating a collection without TTL time results in no TTL config."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        object_ttl_type="create",
        object_ttl_time=None,
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["object_ttl_config"] is None


def test_update_collection_with_ttl_create_type(mock_client, mock_wvc_object_ttl):
    """Test updating a collection with object TTL using 'create' type."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="create",
        object_ttl_time=3600,
        object_ttl_filter_expired=True,
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is not None
    # Verify the correct TTL method was called
    mock_wvc_object_ttl["reconfigure"].delete_by_creation_time.assert_called_once_with(
        time_to_live=3600,
        filter_expired_objects=True,
    )


def test_update_collection_with_ttl_update_type(mock_client, mock_wvc_object_ttl):
    """Test updating a collection with object TTL using 'update' type."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="update",
        object_ttl_time=7200,
        object_ttl_filter_expired=False,
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is not None
    # Verify the correct TTL method was called
    mock_wvc_object_ttl["reconfigure"].delete_by_update_time.assert_called_once_with(
        time_to_live=7200,
        filter_expired_objects=False,
    )


def test_update_collection_with_ttl_property_type(mock_client, mock_wvc_object_ttl):
    """Test updating a collection with object TTL using 'property' type."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="property",
        object_ttl_time=86400,
        object_ttl_filter_expired=True,
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is not None
    # Verify the correct TTL method was called
    mock_wvc_object_ttl["reconfigure"].delete_by_date_property.assert_called_once_with(
        property_name="releaseDate",
        ttl_offset=86400,
        filter_expired_objects=True,
    )


def test_update_collection_with_ttl_property_type_custom_property_name(
    mock_client, mock_wvc_object_ttl
):
    """Test updating a collection with object TTL 'property' type and custom property name."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="property",
        object_ttl_time=86400,
        object_ttl_filter_expired=True,
        object_ttl_property_name="expiresAt",
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is not None
    mock_wvc_object_ttl["reconfigure"].delete_by_date_property.assert_called_once_with(
        property_name="expiresAt",
        ttl_offset=86400,
        filter_expired_objects=True,
    )


def test_update_collection_object_ttl_property_name_guardrail(mock_client):
    """Test that object_ttl_property_name is rejected when object_ttl_type is not 'property'."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            object_ttl_type="create",
            object_ttl_time=3600,
            object_ttl_filter_expired=True,
            object_ttl_property_name="expiresAt",
        )

    assert (
        "object_ttl_property_name is only valid when object_ttl_type is 'property'"
        in str(exc_info.value)
    )
    mock_collection.config.update.assert_not_called()


def test_update_collection_with_ttl_property_type_default_property_name(
    mock_client, mock_wvc_object_ttl
):
    """Test that object_ttl_type=property with no property name uses default 'releaseDate'."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="property",
        object_ttl_time=86400,
        object_ttl_property_name=None,
    )

    mock_collection.config.update.assert_called_once()
    mock_wvc_object_ttl["reconfigure"].delete_by_date_property.assert_called_once_with(
        property_name="releaseDate",
        ttl_offset=86400,
        filter_expired_objects=None,
    )


def test_update_collection_with_ttl_time_zero(mock_client, mock_wvc_object_ttl):
    """Test updating a collection with TTL time=0 applies TTL config (not treated as unset)."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="property",
        object_ttl_time=0,
        object_ttl_filter_expired=False,
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is not None
    mock_wvc_object_ttl["reconfigure"].delete_by_date_property.assert_called_once_with(
        property_name="releaseDate",
        ttl_offset=0,
        filter_expired_objects=False,
    )


def test_update_collection_without_ttl_time(mock_client, mock_wvc_object_ttl):
    """Test updating a collection without TTL time results in no TTL config."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        description="Updated description",
        object_ttl_type="create",
        object_ttl_time=None,
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is None


def test_update_collection_with_ttl_and_multitenancy(mock_client, mock_wvc_object_ttl):
    """Test updating a multitenant collection with object TTL."""
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

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="create",
        object_ttl_time=3600,
        object_ttl_filter_expired=True,
        auto_tenant_creation=True,
        auto_tenant_activation=True,
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is not None
    assert update_call_kwargs["multi_tenancy_config"].autoTenantCreation is True
    assert update_call_kwargs["multi_tenancy_config"].autoTenantActivation is True
    # Verify the correct TTL method was called
    mock_wvc_object_ttl["reconfigure"].delete_by_creation_time.assert_called_once_with(
        time_to_live=3600,
        filter_expired_objects=True,
    )


def test_update_collection_with_ttl_disable_type(mock_client, mock_wvc_object_ttl):
    """Test updating a collection with object TTL using 'disable' type."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    manager.update_collection(
        collection="TestCollection",
        object_ttl_type="disable",
        object_ttl_time=None,
    )

    mock_collection.config.update.assert_called_once()
    update_call_kwargs = mock_collection.config.update.call_args.kwargs
    assert update_call_kwargs["object_ttl_config"] is not None
    # Verify the disable method was called
    mock_wvc_object_ttl["reconfigure"].disable.assert_called_once()
