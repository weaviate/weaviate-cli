import click
from typing import Dict, List, Optional
import weaviate.classes.config as wvc
from weaviate.client import WeaviateClient


class BatchManager:
    def __init__(self, client: WeaviateClient) -> None:
        self.client = client

    def create_collection(
        self,
        collection: str,
        vectorizer: str = "contextionary",
        shards: int = 1,
        replication_factor: int = 1,
        force_auto_schema: bool = True,
    ) -> None:
        """
        Create a collection dynamically for batch insertion.

        Args:
            collection (str): Name of the collection to create.
            vectorizer (str): Vectorizer type (e.g., openai, transformers).
            shards (int): Number of shards for the collection.
            replication_factor (int): Replication factor for the collection.
            force_auto_schema (bool): Whether to let Weaviate infer schema from inserted data.
        """
        if self.client.collections.exists(collection):
            click.echo(f"Collection '{collection}' already exists. Skipping creation.")
            return

        # Map vectorizers to Weaviate configurations
        vectorizer_map: Dict[str, wvc.VectorizerConfig] = {
            "contextionary": wvc.Configure.Vectorizer.text2vec_contextionary(),
            "transformers": wvc.Configure.Vectorizer.text2vec_transformers(),
            "openai": wvc.Configure.Vectorizer.text2vec_openai(),
            "ollama": wvc.Configure.Vectorizer.text2vec_ollama(
                model="snowflake-arctic-embed:33m"
            ),
            "cohere": wvc.Configure.Vectorizer.text2vec_cohere(),
            "jinaai": wvc.Configure.Vectorizer.text2vec_jinaai(),
        }

        # Validate vectorizer
        if vectorizer not in vectorizer_map:
            raise ValueError(
                f"Invalid vectorizer '{vectorizer}'. Choose from: {list(vectorizer_map.keys())}"
            )

        try:
            # Create collection with configuration
            self.client.collections.create(
                name=collection,
                vector_index_config=wvc.Configure.VectorIndex.hnsw(),
                replication_config=wvc.Configure.replication(
                    factor=replication_factor,
                    async_enabled=False,
                    deletion_strategy=wvc.ReplicationDeletionStrategy.DELETE_ON_CONFLICT,
                ),
                sharding_config=wvc.Configure.sharding(desired_count=shards),
                vectorizer_config=vectorizer_map[vectorizer],
                properties=None if force_auto_schema else [],
            )
            click.echo(
                f"Collection '{collection}' created successfully with vectorizer '{vectorizer}'."
            )
        except Exception as e:
            raise Exception(f"Error creating collection '{collection}': {e}")

    def batch_insert(
        self,
        collection: str,
        data: List[Dict],
    ) -> None:
        """
        Insert data into a collection in batch.

        Args:
            collection (str): Name of the collection.
            data (List[Dict]): Data to be inserted.
        """
        if not self.client.collections.exists(collection):
            raise Exception(
                f"Collection '{collection}' does not exist. Cannot insert data."
            )

        try:
            # Perform batch insertion using Weaviate's dynamic batch
            with self.client.batch.dynamic() as batch:
                for record in data:
                    # Remove the reserved 'id' key, if present - to avoid Error message: WeaviateInsertInvalidPropertyError("It is forbidden to insert `id` or `vector`
                    if "id" in record:
                        record.pop("id")

                    # Add the object to the batch
                    batch.add_object(
                        collection=collection,
                        properties=record,
                    )
                    click.echo(
                        f"Processed record"
                    )  # add '{record}' <- if you would like to see the record being processed
        except Exception as e:
            raise Exception(f"Batch insertion failed: {e}")

        # Check for failed objects
        failed_objects = self.client.batch.failed_objects
        if failed_objects:
            click.echo(f"Number of failed objects: {len(failed_objects)}")
            for i, failed_obj in enumerate(failed_objects, 1):
                click.echo(f"Failed object {i}: {failed_obj}")
        else:
            click.echo(f"All objects successfully inserted into '{collection}'.")
