import json
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


def test_create_collection_multiple_named_vectors(mock_client, mock_wvc_object_ttl):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)
    manager.create_collection(
        collection="MultiVec",
        vectorizer="contextionary",
        vector_index="hnsw",
        named_vector=True,
        named_vector_name="vec_a,vec_b,vec_c",
    )

    create_call_kwargs = mock_collections.create.call_args.kwargs
    vectorizer_config = create_call_kwargs["vectorizer_config"]
    assert isinstance(vectorizer_config, list)
    assert [nv.name for nv in vectorizer_config] == ["vec_a", "vec_b", "vec_c"]
    # Named vectors carry their own index, so the top-level index must be None.
    assert create_call_kwargs["vector_index_config"] is None


def test_create_collection_single_named_vector_backward_compatible(
    mock_client, mock_wvc_object_ttl
):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)
    manager.create_collection(
        collection="SingleVec",
        vectorizer="contextionary",
        vector_index="hnsw",
        named_vector=True,
        named_vector_name="myvec",
    )

    vectorizer_config = mock_collections.create.call_args.kwargs["vectorizer_config"]
    assert isinstance(vectorizer_config, list)
    assert [nv.name for nv in vectorizer_config] == ["myvec"]


def test_create_collection_named_vectors_strip_whitespace(
    mock_client, mock_wvc_object_ttl
):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)
    manager.create_collection(
        collection="TrimVec",
        vectorizer="contextionary",
        vector_index="hnsw",
        named_vector=True,
        named_vector_name=" vec_a , vec_b ",
    )

    vectorizer_config = mock_collections.create.call_args.kwargs["vectorizer_config"]
    assert [nv.name for nv in vectorizer_config] == ["vec_a", "vec_b"]


def test_create_collection_named_vectors_duplicate_names_rejected(
    mock_client, mock_wvc_object_ttl
):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = False

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception, match="duplicate names"):
        manager.create_collection(
            collection="DupVec",
            vectorizer="contextionary",
            vector_index="hnsw",
            named_vector=True,
            named_vector_name="vec_a,vec_a",
        )
    mock_collections.create.assert_not_called()


def test_create_collection_non_named_vector_uses_single_config(
    mock_client, mock_wvc_object_ttl
):
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)
    manager.create_collection(
        collection="PlainVec",
        vectorizer="contextionary",
        vector_index="hnsw",
        named_vector=False,
    )

    create_call_kwargs = mock_collections.create.call_args.kwargs
    # Without named vectors the config is a single object, not a list.
    assert not isinstance(create_call_kwargs["vectorizer_config"], list)
    assert create_call_kwargs["vector_index_config"] is not None


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


def test_create_collection_with_async_replication_config(
    mock_client, mock_wvc_object_ttl
):
    """Test creating a collection with async replication config parameters."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]
    mock_client.get_meta.return_value = {"version": "1.36.0"}

    manager = CollectionManager(mock_client)

    async_config = {"max_workers": 10, "frequency": 60, "propagation_concurrency": 4}

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        async_enabled=True,
        async_replication_config=async_config,
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["replication_config"].asyncEnabled is True
    # Verify async_config is set on the replication config
    repl_config = create_call_kwargs["replication_config"]
    assert repl_config.asyncConfig is not None
    assert repl_config.asyncConfig.maxWorkers == 10
    assert repl_config.asyncConfig.frequency == 60
    assert repl_config.asyncConfig.propagationConcurrency == 4


def test_create_collection_without_async_replication_config(
    mock_client, mock_wvc_object_ttl
):
    """Test creating a collection without async replication config passes None."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        async_enabled=True,
    )

    mock_collections.create.assert_called_once()
    repl_config = mock_collections.create.call_args.kwargs["replication_config"]
    assert repl_config.asyncConfig is None


def test_create_collection_async_replication_config_requires_async_enabled(
    mock_client,
):
    """Test that async_replication_config is rejected when async_enabled is False."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = False

    manager = CollectionManager(mock_client)

    with pytest.raises(Exception, match="requires --async_enabled"):
        manager.create_collection(
            collection="TestCollection",
            replication_factor=3,
            vector_index="hnsw",
            async_enabled=False,
            async_replication_config={"max_workers": 10},
        )

    mock_collections.create.assert_not_called()


def test_update_collection_with_async_replication_config(
    mock_client, mock_wvc_object_ttl
):
    """Test updating a collection with async replication config parameters."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]
    mock_client.get_meta.return_value = {"version": "1.36.0"}

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    async_config = {"max_workers": 20, "propagation_batch_size": 100}

    manager.update_collection(
        collection="TestCollection",
        async_replication_config=async_config,
    )

    mock_collection.config.update.assert_called_once()
    repl_config = mock_collection.config.update.call_args.kwargs["replication_config"]
    assert repl_config.asyncConfig is not None
    assert repl_config.asyncConfig.maxWorkers == 20
    assert repl_config.asyncConfig.propagationBatchSize == 100


def test_update_collection_without_async_replication_config(
    mock_client, mock_wvc_object_ttl
):
    """Test updating a collection without async replication config passes None."""
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
        description="Updated",
    )

    mock_collection.config.update.assert_called_once()
    repl_config = mock_collection.config.update.call_args.kwargs["replication_config"]
    assert repl_config.asyncConfig is None


def test_create_collection_with_hfresh_defaults(mock_client, mock_wvc_object_ttl):
    """Test creating a collection with hfresh vector index using default parameters."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        vector_index="hfresh",
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["vector_index_config"] is not None


def test_create_collection_with_hfresh_all_params(mock_client, mock_wvc_object_ttl):
    """Test creating a collection with hfresh vector index with all parameters set."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        vector_index="hfresh",
        hfresh_max_posting_size_kb=64,
        hfresh_replicas=2,
        hfresh_search_probe=100,
        distance_metric="cosine",
        rescore_limit=200,
    )

    mock_collections.create.assert_called_once()
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["vector_index_config"] is not None


def test_create_collection_with_hfresh_valid_distance_metrics(
    mock_client, mock_wvc_object_ttl
):
    """Test creating an hfresh collection with each valid distance metric."""
    valid_metrics = ["cosine", "dot", "l2-squared", "hamming", "manhattan"]
    for metric in valid_metrics:
        mock_collections = MagicMock()
        mock_client.collections = mock_collections
        mock_collections.exists.side_effect = [False, True]

        manager = CollectionManager(mock_client)
        manager.create_collection(
            collection="TestCollection",
            vector_index="hfresh",
            distance_metric=metric,
        )
        mock_collections.create.assert_called_once()


def test_create_collection_with_hfresh_invalid_distance_metric(
    mock_client, mock_wvc_object_ttl
):
    """Test that an unsupported distance metric raises ValueError."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = False

    manager = CollectionManager(mock_client)

    with pytest.raises(ValueError) as exc_info:
        manager.create_collection(
            collection="TestCollection",
            vector_index="hfresh",
            distance_metric="invalid_metric",
        )

    assert "Invalid distance_metric: 'invalid_metric'" in str(exc_info.value)
    mock_collections.create.assert_not_called()


def test_update_collection_async_replication_config_rejected_when_async_false(
    mock_client,
):
    """Test that async_replication_config is rejected when async_enabled is explicitly False."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.return_value = True

    manager = CollectionManager(mock_client)

    with pytest.raises(Exception, match="cannot be used when --async_enabled is False"):
        manager.update_collection(
            collection="TestCollection",
            async_enabled=False,
            async_replication_config={"max_workers": 10},
        )

    mock_collections.get.return_value.config.update.assert_not_called()


def test_create_collection_async_replication_config_warns_on_old_version(
    mock_client, mock_wvc_object_ttl, capsys
):
    """Warn when async_replication_config is used against a server older than v1.36.0."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]
    mock_client.get_meta.return_value = {"version": "1.34.0"}

    manager = CollectionManager(mock_client)

    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        async_enabled=True,
        async_replication_config={"max_workers": 10},
    )

    captured = capsys.readouterr()
    assert "Warning: --async_replication_config requires Weaviate >= v1.36.0" in (
        captured.out + captured.err
    )
    mock_collections.create.assert_called_once()


def test_update_collection_async_replication_config_reset(
    mock_client, mock_wvc_object_ttl
):
    """Reset (empty dict) calls Reconfigure.Replication.async_config() with no kwargs."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]
    mock_client.get_meta.return_value = {"version": "1.36.0"}

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = MagicMock(
        replication_config=MagicMock(factor=3),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )

    manager = CollectionManager(mock_client)

    with patch.object(
        wvc.Reconfigure.Replication,
        "async_config",
        wraps=wvc.Reconfigure.Replication.async_config,
    ) as mock_async_config:
        manager.update_collection(
            collection="TestCollection",
            async_replication_config={},
        )

    mock_async_config.assert_called_once_with()
    mock_collection.config.update.assert_called_once()
    repl_config = mock_collection.config.update.call_args.kwargs["replication_config"]
    assert repl_config.asyncConfig is not None


def test_update_collection_async_replication_config_warns_on_old_version(
    mock_client, mock_wvc_object_ttl, capsys
):
    """Warn when async_replication_config is used against a server older than v1.36.0."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]
    mock_client.get_meta.return_value = {"version": "1.34.0"}

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
        async_replication_config={"max_workers": 10},
    )

    captured = capsys.readouterr()
    assert "Warning: --async_replication_config requires Weaviate >= v1.36.0" in (
        captured.out + captured.err
    )
    mock_collection.config.update.assert_called_once()


def _named_vector(index_type=None, vectorizer="none"):
    """Build a mock named vector config. `index_type=None` means the index was dropped."""
    named_vector = MagicMock()
    if index_type is None:
        named_vector.vector_index_config = None
    else:
        named_vector.vector_index_config.vector_index_type.return_value = index_type
    named_vector.vectorizer.vectorizer.value = vectorizer
    return named_vector


def _named_vector_schema(vector_config):
    """Build a mock collection config that uses named vectors."""
    return MagicMock(
        vector_config=vector_config,
        vectorizer=None,
        vector_index_type=None,
        replication_config=MagicMock(factor=1),
        multi_tenancy_config=MagicMock(
            enabled=False, auto_tenant_creation=False, auto_tenant_activation=False
        ),
    )


def _drop_ready_collection(mock_client, vector_config=None):
    """Wire a mock collection whose named vector index can be dropped."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_client.collections.exists.side_effect = [True, True]
    mock_client.get_meta.return_value = {"version": "1.39.0"}

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_collection.config.get.return_value = _named_vector_schema(
        vector_config
        if vector_config is not None
        else {"title_vector": _named_vector("hnsw")}
    )
    return mock_collection


def test_update_collection_drop_vector_index(mock_client, mock_wvc_object_ttl):
    """--drop_vector_index deletes the index of the named vector, after the config update."""
    mock_collection = _drop_ready_collection(mock_client)

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        drop_vector_index="title_vector",
    )

    mock_collection.config.delete_vector_index.assert_called_once_with(
        vector_name="title_vector"
    )

    # `config.update()` reads the whole schema and writes it back, so the drop has to
    # come last -- otherwise a `vectorIndexType: "none"` vector is sent back to Weaviate.
    call_names = [
        call[0]
        for call in mock_collection.config.mock_calls
        if call[0] in ("update", "delete_vector_index")
    ]
    assert call_names == ["update", "delete_vector_index"]


def test_update_collection_drop_vector_index_json_output(
    mock_client, mock_wvc_object_ttl, capsys
):
    """The JSON payload reports the dropped vector and that the removal is async."""
    _drop_ready_collection(mock_client)

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        drop_vector_index="title_vector",
        json_output=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["dropped_vector_index"] == "title_vector"
    assert "asynchronously" in payload["message"]


def test_update_collection_drop_vector_index_rejects_vector_index_combo(mock_client):
    """--drop_vector_index and --vector_index are mutually exclusive."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            vector_index="hnsw",
            drop_vector_index="title_vector",
        )

    assert "cannot be combined with --vector_index" in str(exc_info.value)
    mock_collections.get.assert_not_called()


def test_update_collection_add_vector(mock_client, mock_wvc_object_ttl):
    """--add_vector adds a named vector with a fresh index, after the config update."""
    mock_collection = _drop_ready_collection(mock_client)

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        add_vector="revived",
    )

    mock_collection.config.add_vector.assert_called_once()
    added = mock_collection.config.add_vector.call_args.kwargs["vector_config"]
    assert added.name == "revived"

    # The added vector is applied after the config update, like the drop path.
    call_names = [
        call[0]
        for call in mock_collection.config.mock_calls
        if call[0] in ("update", "add_vector")
    ]
    assert call_names == ["update", "add_vector"]


def test_update_collection_add_vector_json_output(
    mock_client, mock_wvc_object_ttl, capsys
):
    """The JSON payload reports the added vector."""
    _drop_ready_collection(mock_client)

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        add_vector="revived",
        json_output=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["added_vector"] == "revived"


def test_update_collection_add_vector_rejects_drop_combo(mock_client):
    """--add_vector and --drop_vector_index are mutually exclusive."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            add_vector="revived",
            drop_vector_index="title_vector",
        )

    assert "cannot be combined with --drop_vector_index" in str(exc_info.value)
    mock_collections.get.assert_not_called()


def test_update_collection_add_vector_rejects_vector_index_combo(mock_client):
    """--add_vector and --vector_index are mutually exclusive."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            add_vector="revived",
            vector_index="hnsw",
        )

    assert "--add_vector cannot be combined with --vector_index" in str(exc_info.value)
    mock_collections.get.assert_not_called()


def test_update_collection_add_vector_unsupported_vectorizer(mock_client):
    """An unsupported vectorizer for --add_vector fails before anything is changed."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            add_vector="revived",
            add_vector_vectorizer="openai",
        )

    assert "is not supported for --add_vector" in str(exc_info.value)
    mock_collections.get.assert_not_called()


def test_update_collection_add_vector_quantized_index(mock_client, mock_wvc_object_ttl):
    """--add_vector_index_type builds a quantized index that honors --training_limit."""
    mock_collection = _drop_ready_collection(mock_client)

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        add_vector="revived",
        add_vector_index_type="hnsw_pq",
        training_limit=777,
    )

    added = mock_collection.config.add_vector.call_args.kwargs["vector_config"]
    index_config = added._to_dict()["vectorIndexConfig"]
    assert index_config["pq"]["enabled"] is True
    assert index_config["pq"]["trainingLimit"] == 777


def test_update_collection_add_vector_rq_index(mock_client, mock_wvc_object_ttl):
    """--add_vector_index_type hnsw_rq builds an RQ-quantized index."""
    mock_collection = _drop_ready_collection(mock_client)

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        add_vector="revived",
        add_vector_index_type="hnsw_rq",
    )

    added = mock_collection.config.add_vector.call_args.kwargs["vector_config"]
    assert added._to_dict()["vectorIndexConfig"]["rq"]["enabled"] is True


def test_update_collection_add_vector_dynamic_index(mock_client, mock_wvc_object_ttl):
    """--add_vector_index_type supports the create-style dynamic variants."""
    mock_collection = _drop_ready_collection(mock_client)

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        add_vector="revived",
        add_vector_index_type="dynamic_hnsw_pq",
    )

    added = mock_collection.config.add_vector.call_args.kwargs["vector_config"]
    assert added._to_dict()["vectorIndexType"] == "dynamic"


def test_update_collection_add_vector_unsupported_index_type(mock_client):
    """An unsupported index type for --add_vector fails before anything is changed."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            add_vector="revived",
            add_vector_index_type="bogus",
        )

    assert "Index type 'bogus' is not supported" in str(exc_info.value)
    mock_collections.get.assert_not_called()


def test_update_collection_drop_vector_index_unknown_vector(
    mock_client, mock_wvc_object_ttl
):
    """An unknown vector name fails before anything is updated or dropped."""
    mock_collection = _drop_ready_collection(
        mock_client,
        vector_config={
            "title_vector": _named_vector("hnsw"),
            "body_vector": _named_vector("flat"),
        },
    )

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            drop_vector_index="missing_vector",
        )

    assert "Named vector 'missing_vector' does not exist" in str(exc_info.value)
    assert "body_vector, title_vector" in str(exc_info.value)
    mock_collection.config.update.assert_not_called()
    mock_collection.config.delete_vector_index.assert_not_called()


def test_update_collection_drop_vector_index_without_named_vectors(
    mock_client, mock_wvc_object_ttl
):
    """Only named vectors can be dropped, a legacy single-vector collection is rejected."""
    mock_collection = _drop_ready_collection(mock_client, vector_config={})

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            drop_vector_index="title_vector",
        )

    assert "has no named vectors" in str(exc_info.value)
    mock_collection.config.update.assert_not_called()
    mock_collection.config.delete_vector_index.assert_not_called()


def test_update_collection_drop_vector_index_already_dropped_re_triggers(
    mock_client, mock_wvc_object_ttl, capsys
):
    """Re-dropping an already-'none' vector is allowed: it re-triggers cleanup.

    The server returns 200 for a repeat drop (no-op while cleanup runs, or a fresh
    cleanup task if the previous one FAILED). The CLI must not block this -- it is the
    only way for an operator to recover a stalled drop.
    """
    mock_collection = _drop_ready_collection(
        mock_client, vector_config={"title_vector": _named_vector(None)}
    )

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        drop_vector_index="title_vector",
    )

    mock_collection.config.delete_vector_index.assert_called_once_with(
        vector_name="title_vector"
    )
    assert "already dropped" in capsys.readouterr().out


def test_update_collection_drop_vector_index_already_dropped_json_re_trigger(
    mock_client, mock_wvc_object_ttl, capsys
):
    """JSON output still reports success and the re-trigger note for a repeat drop."""
    _drop_ready_collection(
        mock_client, vector_config={"title_vector": _named_vector(None)}
    )

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        drop_vector_index="title_vector",
        json_output=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["dropped_vector_index"] == "title_vector"
    assert "re-trigger" in payload["message"]


def test_update_collection_drop_vector_index_warns_on_old_version(
    mock_client, mock_wvc_object_ttl, capsys
):
    """Warn when --drop_vector_index is used against a server older than v1.39.0."""
    mock_collection = _drop_ready_collection(mock_client)
    mock_client.get_meta.return_value = {"version": "1.38.0"}

    manager = CollectionManager(mock_client)
    manager.update_collection(
        collection="TestCollection",
        drop_vector_index="title_vector",
    )

    captured = capsys.readouterr()
    assert "Warning: --drop_vector_index requires Weaviate >= v1.39.0" in (
        captured.out + captured.err
    )
    mock_collection.config.delete_vector_index.assert_called_once()


def test_update_collection_drop_vector_index_server_error_hints_at_env_var(
    mock_client, mock_wvc_object_ttl
):
    """A server rejection points at the experimental feature flag it most likely needs."""
    mock_collection = _drop_ready_collection(mock_client)
    mock_collection.config.delete_vector_index.side_effect = Exception(
        "endpoint is experimental and disabled by default"
    )

    manager = CollectionManager(mock_client)
    with pytest.raises(Exception) as exc_info:
        manager.update_collection(
            collection="TestCollection",
            drop_vector_index="title_vector",
        )

    assert "Failed to drop the index of named vector 'title_vector'" in str(
        exc_info.value
    )
    assert "ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT=true" in str(
        exc_info.value
    )


def test_get_collection_lists_dropped_vector_index_as_none(mock_client, capsys):
    """A dropped index surfaces as `none` instead of crashing on a null index config."""
    mock_client.collections = MagicMock()
    mock_client.collections.list_all.return_value = ["Movies"]
    mock_client.collections.get.return_value.config.get.return_value = (
        _named_vector_schema({"title_vector": _named_vector(None)})
    )

    manager = CollectionManager(mock_client)
    manager.get_collection(collection=None, json_output=True)

    payload = json.loads(capsys.readouterr().out)
    assert payload["collections"][0]["vector_index"] == "none"


def test_get_collection_lists_all_distinct_vector_index_types(mock_client, capsys):
    """Every distinct index type is listed, so a per-vector drop stays visible."""
    mock_client.collections = MagicMock()
    mock_client.collections.list_all.return_value = ["Movies"]
    mock_client.collections.get.return_value.config.get.return_value = (
        _named_vector_schema(
            {
                "title_vector": _named_vector("hnsw"),
                "body_vector": _named_vector(None),
                "extra_vector": _named_vector("hnsw"),
            }
        )
    )

    manager = CollectionManager(mock_client)
    manager.get_collection(collection=None, json_output=True)

    payload = json.loads(capsys.readouterr().out)
    assert payload["collections"][0]["vector_index"] == "hnsw, none"
