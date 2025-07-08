import pytest
import weaviate
import numpy as np
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.config_manager import ConfigManager
from weaviate_cli.managers.data_manager import DataManager
import weaviate.classes.config as wvc


@pytest.fixture
def client() -> weaviate.Client:
    config = ConfigManager()
    return config.get_client()


@pytest.fixture
def collection_manager(client: weaviate.Client) -> CollectionManager:
    return CollectionManager(client)


@pytest.fixture
def data_manager(client: weaviate.Client) -> DataManager:
    return DataManager(client)


@pytest.mark.parametrize("randomize", [False, True])
@pytest.mark.parametrize("vectorizer", ["transformers", "contextionary"])
def test_data_creation_with_different_configs(
    collection_manager: CollectionManager,
    data_manager: DataManager,
    randomize: bool,
    vectorizer: str,
):
    """Test data creation with different randomize and vectorizer configurations."""
    collection_name = f"TestData{randomize}{vectorizer.capitalize()[:7]}"
    # Contextionary does not know the word "contextionary", therefore we truncate it to "context" [0:7]

    try:
        # Create collection with specified vectorizer
        collection_manager.create_collection(
            collection=collection_name,
            vectorizer=vectorizer,
            replication_factor=1,
            training_limit=1000,
            async_enabled=True,
        )

        # Verify collection exists
        assert collection_manager.client.collections.exists(collection_name)

        # Create data with specified randomize setting
        data_manager.create_data(
            collection=collection_name,
            limit=50,
            consistency_level="one",
            randomize=randomize,
            skip_seed=True,
        )

        # Get the collection and verify data was created
        collection = collection_manager.client.collections.get(collection_name)

        # Wait for indexing to complete
        collection.batch.wait_for_vector_indexing()

        # Query objects to verify they have content and vectors
        objects = collection.query.fetch_objects(limit=50, include_vector=True)

        # Verify we got the expected number of objects
        assert len(objects.objects) == 50

        # Verify each object has content and vectors
        for obj in objects.objects:
            # Check that object has properties
            assert hasattr(obj, "properties")
            assert obj.properties is not None

            # Check that object has a title (required field)
            assert "title" in obj.properties
            assert obj.properties["title"] is not None
            assert len(obj.properties["title"]) > 0

            # Check that object has genres
            assert "genres" in obj.properties
            assert obj.properties["genres"] is not None

            # Check that object has keywords
            assert "keywords" in obj.properties
            assert obj.properties["keywords"] is not None

            # Verify vector was created
            assert hasattr(obj, "vector")
            assert obj.vector is not None

            vector_dimensions = {
                "transformers": 384,
                "contextionary": 300,
            }

            # Check vector dimensions (should be 1536 for default)
            assert len(obj.vector["default"]) == vector_dimensions[vectorizer]

            # Verify vector is not all zeros (should have meaningful values)
            assert not np.allclose(
                obj.vector["default"], np.zeros(vector_dimensions[vectorizer])
            )

            # Verify vector has finite values
            assert np.all(np.isfinite(obj.vector["default"]))

    finally:
        # Clean up
        if collection_manager.client.collections.exists(collection_name):
            collection_manager.delete_collection(collection=collection_name)


@pytest.mark.parametrize("vectorizer", ["transformers", "none"])
@pytest.mark.parametrize(
    "named_vector_name", ["custom_vector", "movie_embedding", "content_vector"]
)
def test_data_creation_with_named_vectors(
    collection_manager: CollectionManager,
    data_manager: DataManager,
    named_vector_name: str,
    vectorizer: str,
):
    """Test data creation with named vectors and verify the correct vector name is set."""
    collection_name = f"TestNamedVector{named_vector_name}{vectorizer.capitalize()}"

    try:
        # Create collection with named vector
        collection_manager.create_collection(
            collection=collection_name,
            vectorizer=vectorizer,
            replication_factor=1,
            training_limit=1000,
            async_enabled=True,
            named_vector=True,
            named_vector_name=named_vector_name,
        )

        # Verify collection exists
        assert collection_manager.client.collections.exists(collection_name)

        # Create data
        data_manager.create_data(
            collection=collection_name,
            limit=30,
            consistency_level="one",
            vector_dimensions=384 if vectorizer == "none" else None,
            randomize=True,
            skip_seed=True,
        )

        # Get the collection and verify data was created
        collection = collection_manager.client.collections.get(collection_name)

        # Wait for indexing to complete
        collection.batch.wait_for_vector_indexing()

        # Query objects to verify they have content and named vectors
        objects = collection.query.fetch_objects(limit=30, include_vector=True)

        # Verify we got the expected number of objects
        assert len(objects.objects) == 30

        # Verify each object has content and the correct named vector
        for obj in objects.objects:
            # Check that object has properties
            assert hasattr(obj, "properties")
            assert obj.properties is not None

            # Check that object has a title
            assert "title" in obj.properties
            assert obj.properties["title"] is not None

            # Check that object has genres
            assert "genres" in obj.properties
            assert obj.properties["genres"] is not None

            # Check that object has keywords
            assert "keywords" in obj.properties
            assert obj.properties["keywords"] is not None

            # Verify named vector was created with correct name
            assert hasattr(obj, "vector")
            assert obj.vector is not None

            # Check that the named vector exists
            assert named_vector_name in obj.vector

            # Get the named vector
            named_vector = obj.vector[named_vector_name]
            assert named_vector is not None

            # Check vector dimensions (should be 768 for transformers)
            assert len(named_vector) == 384

            # Verify vector is not all zeros
            assert not np.allclose(named_vector, np.zeros(384))

            # Verify vector has finite values
            assert np.all(np.isfinite(named_vector))

    finally:
        # Clean up
        if collection_manager.client.collections.exists(collection_name):
            collection_manager.delete_collection(collection=collection_name)


def test_data_creation_with_multi_vector(
    collection_manager: CollectionManager,
    data_manager: DataManager,
):
    """Test data creation with multi-vector enabled."""
    collection_name = "TestMultiVector"

    try:
        # Create collection
        collection_manager.create_collection(
            collection=collection_name,
            vectorizer="none",
            replication_factor=1,
            training_limit=1000,
            async_enabled=True,
            vector_index="hnsw_multivector",
            named_vector=True,
        )

        # Verify collection exists
        assert collection_manager.client.collections.exists(collection_name)

        # Create data with multi-vector enabled
        data_manager.create_data(
            collection=collection_name,
            limit=25,
            consistency_level="one",
            randomize=True,
            skip_seed=True,
            multi_vector=True,
            vector_dimensions=1536,
        )

        # Get the collection and verify data was created
        collection = collection_manager.client.collections.get(collection_name)

        # Wait for indexing to complete
        collection.batch.wait_for_vector_indexing()

        # Query objects to verify they have content and vectors
        objects = collection.query.fetch_objects(limit=25, include_vector=True)

        # Verify we got the expected number of objects
        assert len(objects.objects) == 25

        # Verify each object has content and vectors
        for obj in objects.objects:
            # Check that object has properties
            assert hasattr(obj, "properties")
            assert obj.properties is not None

            # Check that object has a title
            assert "title" in obj.properties
            assert obj.properties["title"] is not None

            # Check that object has genres
            assert "genres" in obj.properties
            assert obj.properties["genres"] is not None

            # Check that object has keywords
            assert "keywords" in obj.properties
            assert obj.properties["keywords"] is not None

            # Verify vector was created
            assert hasattr(obj, "vector")
            assert obj.vector is not None

            # Check vector dimensions
            for vector in obj.vector["default"]:
                assert len(vector) == 1536

            # Verify vector is not all zeros
            for vector in obj.vector["default"]:
                assert not np.allclose(vector, np.zeros(1536))

            # Verify vector has finite values
            for vector in obj.vector["default"]:
                assert np.all(np.isfinite(vector))

    finally:
        # Clean up
        if collection_manager.client.collections.exists(collection_name):
            collection_manager.delete_collection(collection=collection_name)


def test_data_creation_with_custom_vector_dimensions(
    collection_manager: CollectionManager,
    data_manager: DataManager,
):
    """Test data creation with custom vector dimensions."""
    collection_name = "TestCustomDimensions"
    custom_dimensions = 768  # Different from default 1536

    try:
        # Create collection
        collection_manager.create_collection(
            collection=collection_name,
            vectorizer="none",
            replication_factor=1,
            training_limit=1000,
            async_enabled=True,
        )

        # Verify collection exists
        assert collection_manager.client.collections.exists(collection_name)

        # Create data with custom vector dimensions
        data_manager.create_data(
            collection=collection_name,
            limit=20,
            consistency_level="one",
            randomize=True,
            skip_seed=True,
            vector_dimensions=custom_dimensions,
        )

        # Get the collection and verify data was created
        collection = collection_manager.client.collections.get(collection_name)

        # Wait for indexing to complete
        collection.batch.wait_for_vector_indexing()

        # Query objects to verify they have content and vectors with custom dimensions
        objects = collection.query.fetch_objects(limit=20, include_vector=True)

        # Verify we got the expected number of objects
        assert len(objects.objects) == 20

        # Verify each object has content and vectors with correct dimensions
        for obj in objects.objects:
            # Check that object has properties
            assert hasattr(obj, "properties")
            assert obj.properties is not None

            # Check that object has a title
            assert "title" in obj.properties
            assert obj.properties["title"] is not None

            # Check that object has genres
            assert "genres" in obj.properties
            assert obj.properties["genres"] is not None

            # Check that object has keywords
            assert "keywords" in obj.properties
            assert obj.properties["keywords"] is not None

            # Verify vector was created with custom dimensions
            assert hasattr(obj, "vector")
            assert obj.vector is not None

            # Check vector dimensions (should be custom_dimensions)
            assert len(obj.vector["default"]) == custom_dimensions

            # Verify vector is not all zeros
            assert not np.allclose(obj.vector["default"], np.zeros(custom_dimensions))

            # Verify vector has finite values
            assert np.all(np.isfinite(obj.vector["default"]))

    finally:
        # Clean up
        if collection_manager.client.collections.exists(collection_name):
            collection_manager.delete_collection(collection=collection_name)


def test_data_creation_error_handling(
    collection_manager: CollectionManager,
    data_manager: DataManager,
):
    """Test error handling when creating data in non-existent collection."""

    # Try to create data in non-existent collection
    with pytest.raises(Exception) as exc_info:
        data_manager.create_data(
            collection="NonExistentCollection",
            limit=10,
            consistency_level="one",
        )

    # Verify error message
    assert "does not exist in Weaviate" in str(exc_info.value)
