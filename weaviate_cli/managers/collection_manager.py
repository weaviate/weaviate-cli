import click
import json
from typing import Dict, List, Optional
from weaviate.client import WeaviateClient
from weaviate.collections import Collection
from weaviate.collections.classes.config import _CollectionConfigSimple
from weaviate.collections.classes.tenants import TenantActivityStatus
from weaviate.classes.config import VectorFilterStrategy
from weaviate_cli.defaults import (
    CreateCollectionDefaults,
    UpdateCollectionDefaults,
    DeleteCollectionDefaults,
    GetCollectionDefaults,
)
import weaviate.classes.config as wvc
from prettytable import PrettyTable


class CollectionManager:
    def __init__(self, client: WeaviateClient) -> None:
        self.client = client

    def __get_total_objects_with_multitenant(self, col_obj: Collection) -> int:
        acc = 0
        for tenant_name, tenant in col_obj.tenants.get().items():
            acc += (
                len(col_obj.with_tenant(tenant))
                if tenant.activity_status == TenantActivityStatus.ACTIVE
                else 0
            )
        return acc

    def get_collection(
        self, collection: Optional[str] = GetCollectionDefaults.collection
    ) -> None:

        if collection is not None:
            if not self.client.collections.exists(collection):

                raise Exception(f"Collection '{collection}' does not exist")
            col_obj: Collection = self.client.collections.get(collection)
            # Pretty print the dict structure
            click.echo(json.dumps(col_obj.config.get().to_dict(), indent=4))
        else:
            collections = self.client.collections.list_all()

            if not collections:
                click.echo("No collections found")
                return

            table = PrettyTable()
            table.field_names = [
                "Collection",
                "Multitenancy",
                "Tenants",
                "Objects",
                "Repl. Factor",
                "Vector Index",
                "Vectorizer",
            ]
            table.align = "l"

            for col_name in collections:
                col_obj = self.client.collections.get(col_name)
                schema = col_obj.config.get()

                table.add_row(
                    [
                        col_name,
                        "True" if schema.multi_tenancy_config.enabled else "False",
                        (
                            len(col_obj.tenants.get())
                            if schema.multi_tenancy_config.enabled
                            else 0
                        ),
                        (
                            self.__get_total_objects_with_multitenant(col_obj)
                            if schema.multi_tenancy_config.enabled
                            else len(col_obj)
                        ),
                        schema.replication_config.factor,
                        (
                            schema.vector_index_type
                            if schema.vector_index_type
                            else "None"
                        ),
                        schema.vectorizer if schema.vectorizer else "None",
                    ]
                )

            print("\nCollections:")
            print(table)
            print(f"\nTotal: {len(collections)} collections")

    def get_all_collections(self) -> dict[str, _CollectionConfigSimple]:
        return self.client.collections.list_all()

    def create_collection(
        self,
        collection: str = CreateCollectionDefaults.collection,
        replication_factor: int = CreateCollectionDefaults.replication_factor,
        async_enabled: bool = CreateCollectionDefaults.async_enabled,
        vector_index: str = CreateCollectionDefaults.vector_index,
        inverted_index: Optional[str] = CreateCollectionDefaults.inverted_index,
        training_limit: int = CreateCollectionDefaults.training_limit,
        multitenant: bool = CreateCollectionDefaults.multitenant,
        auto_tenant_creation: bool = CreateCollectionDefaults.auto_tenant_creation,
        auto_tenant_activation: bool = CreateCollectionDefaults.auto_tenant_activation,
        force_auto_schema: bool = CreateCollectionDefaults.force_auto_schema,
        shards: int = CreateCollectionDefaults.shards,
        vectorizer: Optional[str] = CreateCollectionDefaults.vectorizer,
        replication_deletion_strategy: Optional[
            str
        ] = CreateCollectionDefaults.replication_deletion_strategy,
    ) -> None:

        if self.client.collections.exists(collection):

            raise Exception(
                f"Error: Collection '{collection}' already exists in Weaviate. Delete using <delete collection> command."
            )

        vector_index_map: Dict[str, wvc.VectorIndexConfig] = {
            "hnsw": wvc.Configure.VectorIndex.hnsw(),
            "flat": wvc.Configure.VectorIndex.flat(),
            "dynamic": wvc.Configure.VectorIndex.dynamic(),
            "hnsw_pq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.pq(
                    training_limit=training_limit
                )
            ),
            "hnsw_bq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq()
            ),
            "hnsw_bq_cache": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq(cache=True)
            ),
            "hnsw_sq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.sq(
                    training_limit=training_limit
                )
            ),
            "hnsw_acorn": wvc.Configure.VectorIndex.hnsw(
                filter_strategy=VectorFilterStrategy.ACORN
            ),
            # Should fail at the moment as Flat and PQ are not compatible
            "flat_pq": wvc.Configure.VectorIndex.flat(
                quantizer=wvc.Configure.VectorIndex.Quantizer.pq()
            ),
            # Should fail at the moment as Flat and PQ are not compatible
            "flat_sq": wvc.Configure.VectorIndex.flat(
                quantizer=wvc.Configure.VectorIndex.Quantizer.sq()
            ),
            "flat_bq": wvc.Configure.VectorIndex.flat(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq()
            ),
            "flat_bq_cache": wvc.Configure.VectorIndex.flat(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq(cache=True)
            ),
        }

        vectorizer_map: Dict[str, wvc.VectorizerConfig] = {
            "contextionary": wvc.Configure.Vectorizer.text2vec_contextionary(),
            "transformers": wvc.Configure.Vectorizer.text2vec_transformers(),
            "openai": wvc.Configure.Vectorizer.text2vec_openai(),
            "ollama": wvc.Configure.Vectorizer.text2vec_ollama(
                model="snowflake-arctic-embed:33m"
            ),
            "cohere": wvc.Configure.Vectorizer.text2vec_cohere(),
            "jinaai": wvc.Configure.Vectorizer.text2vec_jinaai(),
            "weaviate": wvc.Configure.Vectorizer.text2vec_weaviate(),
        }

        inverted_index_map: Dict[str, wvc.InvertedIndexConfig] = {
            "timestamp": wvc.Configure.inverted_index(index_timestamps=True),
            "null": wvc.Configure.inverted_index(index_null_state=True),
            "length": wvc.Configure.inverted_index(index_property_length=True),
        }

        # Collection schema
        properties: List[wvc.Property] = [
            wvc.Property(name="title", data_type=wvc.DataType.TEXT),
            wvc.Property(name="genres", data_type=wvc.DataType.TEXT),
            wvc.Property(name="keywords", data_type=wvc.DataType.TEXT),
            wvc.Property(name="director", data_type=wvc.DataType.TEXT),
            wvc.Property(name="popularity", data_type=wvc.DataType.NUMBER),
            wvc.Property(name="runtime", data_type=wvc.DataType.TEXT),
            wvc.Property(name="cast", data_type=wvc.DataType.TEXT),
            wvc.Property(name="originalLanguage", data_type=wvc.DataType.TEXT),
            wvc.Property(
                name="productionCountries",
                data_type=wvc.DataType.OBJECT_ARRAY,
                nested_properties=[
                    wvc.Property(name="iso_3166_1", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="name", data_type=wvc.DataType.TEXT),
                ],
            ),
            wvc.Property(
                name="spokenLanguages",
                data_type=wvc.DataType.OBJECT_ARRAY,
                nested_properties=[
                    wvc.Property(name="iso_639_1", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="name", data_type=wvc.DataType.TEXT),
                ],
            ),
            wvc.Property(name="tagline", data_type=wvc.DataType.TEXT),
            wvc.Property(name="budget", data_type=wvc.DataType.NUMBER),
            wvc.Property(name="releaseDate", data_type=wvc.DataType.DATE),
            wvc.Property(name="revenue", data_type=wvc.DataType.NUMBER),
            wvc.Property(name="status", data_type=wvc.DataType.TEXT),
        ]

        rds_map = {
            "delete_on_conflict": wvc.ReplicationDeletionStrategy.DELETE_ON_CONFLICT,
            "no_automated_resolution": wvc.ReplicationDeletionStrategy.NO_AUTOMATED_RESOLUTION,
            "time_based_resolution": wvc.ReplicationDeletionStrategy.TIME_BASED_RESOLUTION,
        }

        try:
            self.client.collections.create(
                name=collection,
                vector_index_config=vector_index_map[vector_index],
                inverted_index_config=(
                    inverted_index_map[inverted_index] if inverted_index else None
                ),
                replication_config=wvc.Configure.replication(
                    factor=replication_factor,
                    async_enabled=async_enabled,
                    deletion_strategy=(
                        rds_map[replication_deletion_strategy]
                        if replication_deletion_strategy
                        else None
                    ),
                ),
                sharding_config=(
                    wvc.Configure.sharding(desired_count=shards) if shards > 1 else None
                ),
                multi_tenancy_config=wvc.Configure.multi_tenancy(
                    enabled=multitenant,
                    auto_tenant_creation=auto_tenant_creation,
                    auto_tenant_activation=auto_tenant_activation,
                ),
                vectorizer_config=(vectorizer_map[vectorizer] if vectorizer else None),
                properties=properties if not force_auto_schema else None,
            )
        except Exception as e:

            raise Exception(f"Error creating Collection '{collection}': {e}")

        assert self.client.collections.exists(collection)

        click.echo(f"Collection '{collection}' created successfully in Weaviate.")

    def update_collection(
        self,
        collection: str = UpdateCollectionDefaults.collection,
        description: Optional[str] = UpdateCollectionDefaults.description,
        vector_index: Optional[str] = UpdateCollectionDefaults.vector_index,
        training_limit: int = UpdateCollectionDefaults.training_limit,
        async_enabled: Optional[bool] = UpdateCollectionDefaults.async_enabled,
        auto_tenant_creation: Optional[
            bool
        ] = UpdateCollectionDefaults.auto_tenant_creation,
        auto_tenant_activation: Optional[
            bool
        ] = UpdateCollectionDefaults.auto_tenant_activation,
        replication_deletion_strategy: Optional[
            str
        ] = UpdateCollectionDefaults.replication_deletion_strategy,
    ) -> None:

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Error: Collection '{collection}' does not exist in Weaviate. Create first using ./create_collection.py"
            )

        vector_index_map: Dict[str, wvc.VectorIndexConfig] = {
            "hnsw": wvc.Reconfigure.VectorIndex.hnsw(),
            "flat": wvc.Reconfigure.VectorIndex.flat(),
            "hnsw_pq": wvc.Reconfigure.VectorIndex.hnsw(
                quantizer=wvc.Reconfigure.VectorIndex.Quantizer.pq(
                    training_limit=training_limit
                )
            ),
            "hnsw_sq": wvc.Reconfigure.VectorIndex.hnsw(
                quantizer=wvc.Reconfigure.VectorIndex.Quantizer.sq(
                    training_limit=training_limit
                )
            ),
            "hnsw_bq": wvc.Reconfigure.VectorIndex.hnsw(
                quantizer=wvc.Reconfigure.VectorIndex.Quantizer.bq()
            ),
            "hnsw_acorn": wvc.Reconfigure.VectorIndex.hnsw(
                filter_strategy=VectorFilterStrategy.ACORN
            ),
            "flat_bq": wvc.Reconfigure.VectorIndex.flat(
                quantizer=wvc.Reconfigure.VectorIndex.Quantizer.bq()
            ),
        }

        col_obj: Collection = self.client.collections.get(collection)
        rf = col_obj.config.get().replication_config.factor
        rds_map = {
            "delete_on_conflict": wvc.ReplicationDeletionStrategy.DELETE_ON_CONFLICT,
            "no_automated_resolution": wvc.ReplicationDeletionStrategy.NO_AUTOMATED_RESOLUTION,
            "time_based_resolution": wvc.ReplicationDeletionStrategy.TIME_BASED_RESOLUTION,
        }
        mt = col_obj.config.get().multi_tenancy_config.enabled
        auto_tenant_creation = (
            auto_tenant_creation
            if auto_tenant_creation is not None
            else col_obj.config.get().multi_tenancy_config.auto_tenant_creation
        )
        auto_tenant_activation = (
            auto_tenant_activation
            if auto_tenant_activation is not None
            else col_obj.config.get().multi_tenancy_config.auto_tenant_activation
        )

        col_obj.config.update(
            description=description,
            vectorizer_config=(
                vector_index_map[vector_index] if vector_index else None
            ),
            replication_config=(
                wvc.Reconfigure.replication(
                    factor=rf,
                    async_enabled=async_enabled if async_enabled is not None else None,
                    deletion_strategy=(
                        rds_map[replication_deletion_strategy]
                        if replication_deletion_strategy
                        else None
                    ),
                )
            ),
            multi_tenancy_config=(
                wvc.Reconfigure.multi_tenancy(
                    auto_tenant_creation=auto_tenant_creation,
                    auto_tenant_activation=auto_tenant_activation,
                )
                if mt
                else None
            ),
        )

        assert self.client.collections.exists(collection)

        click.echo(f"Collection '{collection}' modified successfully in Weaviate.")

    def delete_collection(
        self,
        collection: str = DeleteCollectionDefaults.collection,
        all: bool = DeleteCollectionDefaults.all,
    ) -> None:
        if all:
            collections: List[str] = self.client.collections.list_all()
            for collection in collections:
                click.echo(f"Deleting collection '{collection}'")
                self.client.collections.delete(collection)
            click.echo("All collections deleted successfully in Weaviate.")
        else:
            if self.client.collections.exists(collection):
                try:
                    self.client.collections.delete(collection)
                except Exception as e:

                    raise Exception(
                        f"Failed to delete collection '{collection}' in Weaviate.: {e}"
                    )
            else:
                raise Exception(f"Collection '{collection}' doesn't exist in Weaviate.")

            assert not self.client.collections.exists(collection)

            click.echo(f"Collection '{collection}' deleted successfully in Weaviate.")
