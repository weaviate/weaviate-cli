import click
import weaviate.classes.config as wvc

class CollectionManager:
    def __init__(self, client):
        self.client = client



    def create_collection(
        self,
        collection,
        replication_factor,
        async_enabled,
        vector_index,
        inverted_index,
        training_limit,
        multitenant,
        auto_tenant_creation,
        auto_tenant_activation,
        auto_schema,
        shards,
        vectorizer,
    ):

        if self.client.collections.exists(collection):

            raise Exception(
                f"Error: Collection '{collection}' already exists in Weaviate. Delete using <delete collection> command."
            )

        vector_index_map = {
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

        vectorizer_map = {
            "contextionary": wvc.Configure.Vectorizer.text2vec_contextionary(),
            "transformers": wvc.Configure.Vectorizer.text2vec_transformers(),
            "openai": wvc.Configure.Vectorizer.text2vec_openai(),
            "ollama": wvc.Configure.Vectorizer.text2vec_ollama(
                model="snowflake-arctic-embed:33m"
            ),
        }

        inverted_index_map = {
            "timestamp": wvc.Configure.inverted_index(index_timestamps=True),
            "null": wvc.Configure.inverted_index(index_null_state=True),
            "length": wvc.Configure.inverted_index(index_property_length=True),
        }

        properties = [
            wvc.Property(name="title", data_type=wvc.DataType.TEXT),
            wvc.Property(name="genres", data_type=wvc.DataType.TEXT),
            wvc.Property(name="keywords", data_type=wvc.DataType.TEXT),
            wvc.Property(name="director", data_type=wvc.DataType.TEXT),
            wvc.Property(name="popularity", data_type=wvc.DataType.NUMBER),
            wvc.Property(name="runtime", data_type=wvc.DataType.TEXT),
            wvc.Property(name="cast", data_type=wvc.DataType.TEXT),
            wvc.Property(name="originalLanguage", data_type=wvc.DataType.TEXT),
            wvc.Property(name="tagline", data_type=wvc.DataType.TEXT),
            wvc.Property(name="budget", data_type=wvc.DataType.NUMBER),
            wvc.Property(name="releaseDate", data_type=wvc.DataType.DATE),
            wvc.Property(name="revenue", data_type=wvc.DataType.NUMBER),
            wvc.Property(name="status", data_type=wvc.DataType.TEXT),
        ]

        try:
            self.client.collections.create(
                name=collection,
                vector_index_config=vector_index_map[vector_index],
                inverted_index_config=(
                    inverted_index_map[inverted_index] if inverted_index else None
                ),
                replication_config=wvc.Configure.replication(
                    factor=replication_factor, async_enabled=async_enabled
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
                properties=properties if auto_schema else None,
            )
        except Exception as e:

            raise Exception(f"Error creating Collection '{collection}': {e}")

        assert self.client.collections.exists(collection)

        click.echo(f"Collection '{collection}' created successfully in Weaviate.")