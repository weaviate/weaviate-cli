import click
import json
from typing import Dict, List, Optional
from weaviate.client import WeaviateClient
from weaviate.collections import Collection
from weaviate.collections.classes.config import _CollectionConfigSimple
from weaviate.collections.classes.tenants import TenantActivityStatus
from weaviate.collections.classes.config_vector_index import VectorFilterStrategy
from weaviate_cli.defaults import (
    CreateCollectionDefaults,
    UpdateCollectionDefaults,
    DeleteCollectionDefaults,
    GetCollectionDefaults,
)
from weaviate_cli.utils import print_json_or_text, older_than_version
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

    @staticmethod
    def __named_vector_index_types(vector_config: Dict) -> str:
        """Summarize the index types used by a collection's named vectors.

        Every distinct type is reported, in schema order, so that a per-vector
        difference stays visible in the collection listing. A vector whose index was
        dropped with `update collection --drop_vector_index` is reported by Weaviate as
        `vectorIndexType: "none"` and surfaces here as a `vector_index_config` of `None`.
        """
        types: List[str] = []
        for named_vector in vector_config.values():
            index_config = named_vector.vector_index_config
            index_type = (
                "none"
                if index_config is None
                else str(index_config.vector_index_type())
            )
            if index_type not in types:
                types.append(index_type)
        return ", ".join(types) if types else "None"

    def get_collection(
        self,
        collection: Optional[str] = GetCollectionDefaults.collection,
        json_output: bool = False,
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
                if json_output:
                    click.echo(json.dumps({"collections": [], "total": 0}, indent=2))
                else:
                    click.echo("No collections found")
                return

            rows = []
            for col_name in collections:
                col_obj = self.client.collections.get(col_name)
                schema = col_obj.config.get()
                vectorizer = "None"
                vector_index_type = "None"
                named_vectors = "False"
                if schema.vector_config and not schema.vectorizer:
                    named_vectors = "True"
                    vectorizer = schema.vector_config[
                        list(schema.vector_config.keys())[0]
                    ].vectorizer.vectorizer.value
                    if not schema.vector_index_type:
                        vector_index_type = self.__named_vector_index_types(
                            schema.vector_config
                        )
                    else:
                        vector_index_type = schema.vector_index_type
                else:
                    vectorizer = (
                        schema.vectorizer.value if schema.vectorizer else "None"
                    )
                    vector_index_type = (
                        schema.vector_index_type.value
                        if schema.vector_index_type
                        else "None"
                    )

                tenant_count = (
                    len(col_obj.tenants.get())
                    if schema.multi_tenancy_config.enabled
                    else 0
                )
                object_count = (
                    self.__get_total_objects_with_multitenant(col_obj)
                    if schema.multi_tenancy_config.enabled
                    else len(col_obj)
                )

                rows.append(
                    {
                        "name": col_name,
                        "multitenancy": schema.multi_tenancy_config.enabled,
                        "tenant_count": tenant_count,
                        "object_count": object_count,
                        "replication_factor": schema.replication_config.factor,
                        "vector_index": (
                            str(vector_index_type) if vector_index_type else "None"
                        ),
                        "named_vectors": named_vectors == "True",
                        "vectorizer": vectorizer if vectorizer else "None",
                    }
                )

            def _print_text():
                table = PrettyTable()
                table.field_names = [
                    "Collection",
                    "Multitenancy",
                    "Tenants",
                    "Objects",
                    "Repl. Factor",
                    "Vector Index",
                    "Named Vectors",
                    "Vectorizer",
                ]
                table.align = "l"
                for row in rows:
                    table.add_row(
                        [
                            row["name"],
                            "True" if row["multitenancy"] else "False",
                            row["tenant_count"],
                            row["object_count"],
                            row["replication_factor"],
                            row["vector_index"],
                            "True" if row["named_vectors"] else "False",
                            row["vectorizer"],
                        ]
                    )
                print("\nCollections:")
                print(table)
                print(f"\nTotal: {len(collections)} collections")

            print_json_or_text(
                {"collections": rows, "total": len(rows)},
                json_output,
                _print_text,
            )

    def get_all_collections(self) -> dict[str, _CollectionConfigSimple]:
        return self.client.collections.list_all()

    _DISTANCE_METRIC_MAP = {
        "cosine": wvc.VectorDistances.COSINE,
        "dot": wvc.VectorDistances.DOT,
        "l2-squared": wvc.VectorDistances.L2_SQUARED,
        "hamming": wvc.VectorDistances.HAMMING,
        "manhattan": wvc.VectorDistances.MANHATTAN,
    }

    def _resolve_distance_metric(
        self, distance_metric: Optional[str]
    ) -> Optional[wvc.VectorDistances]:
        """Convert a distance metric string to its VectorDistances enum value."""
        if distance_metric is None:
            return None
        if distance_metric not in self._DISTANCE_METRIC_MAP:
            raise ValueError(
                f"Invalid distance_metric: '{distance_metric}'. "
                f"Must be one of: {list(self._DISTANCE_METRIC_MAP.keys())}"
            )
        return self._DISTANCE_METRIC_MAP[distance_metric]

    def _build_hfresh_config(
        self,
        max_posting_size_kb: Optional[int] = None,
        distance_metric: Optional[wvc.VectorDistances] = None,
        rescore_limit: Optional[int] = None,
        replicas: Optional[int] = None,
        search_probe: Optional[int] = None,
    ):
        """Build hfresh configuration with provided parameters."""
        kwargs = {}

        if max_posting_size_kb is not None:
            kwargs["max_posting_size_kb"] = max_posting_size_kb
        if distance_metric is not None:
            kwargs["distance_metric"] = distance_metric
        if replicas is not None:
            kwargs["replicas"] = replicas
        if search_probe is not None:
            kwargs["search_probe"] = search_probe
        if rescore_limit is not None:
            kwargs["quantizer"] = wvc.Configure.VectorIndex.Quantizer.rq(
                bits=8, rescore_limit=rescore_limit
            )

        return wvc.Configure.VectorIndex.hfresh(**kwargs)

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
        vectorizer: str = CreateCollectionDefaults.vectorizer,
        vectorizer_base_url: Optional[
            str
        ] = CreateCollectionDefaults.vectorizer_base_url,
        replication_deletion_strategy: Optional[
            str
        ] = CreateCollectionDefaults.replication_deletion_strategy,
        named_vector: bool = CreateCollectionDefaults.named_vector,
        named_vector_name: Optional[str] = CreateCollectionDefaults.named_vector_name,
        hfresh_max_posting_size_kb: Optional[
            int
        ] = CreateCollectionDefaults.hfresh_max_posting_size_kb,
        hfresh_replicas: Optional[int] = CreateCollectionDefaults.hfresh_replicas,
        hfresh_search_probe: Optional[
            int
        ] = CreateCollectionDefaults.hfresh_search_probe,
        distance_metric: Optional[str] = CreateCollectionDefaults.distance_metric,
        rescore_limit: Optional[int] = CreateCollectionDefaults.rescore_limit,
        json_output: bool = False,
        object_ttl_type: str = CreateCollectionDefaults.object_ttl_type,
        object_ttl_time: Optional[int] = CreateCollectionDefaults.object_ttl_time,
        object_ttl_filter_expired: Optional[
            bool
        ] = CreateCollectionDefaults.object_ttl_filter_expired,
        object_ttl_property_name: Optional[
            str
        ] = CreateCollectionDefaults.object_ttl_property_name,
        async_replication_config: Optional[Dict[str, int]] = None,
    ) -> None:

        if (
            object_ttl_type != "property"
            and object_ttl_property_name
            != CreateCollectionDefaults.object_ttl_property_name
        ):
            raise Exception(
                "object_ttl_property_name is only valid when object_ttl_type is 'property'."
            )
        if self.client.collections.exists(collection):

            raise Exception(
                f"Error: Collection '{collection}' already exists in Weaviate. Delete using <delete collection> command."
            )

        if async_replication_config is not None and not async_enabled:
            raise Exception(
                "Error: --async_replication_config requires --async_enabled to be set."
            )

        if async_replication_config is not None and older_than_version(
            self.client, "1.36.0"
        ):
            click.echo(
                "Warning: --async_replication_config requires Weaviate >= v1.36.0. "
                "The server may ignore or reject these settings."
            )

        if named_vector_name != "default" and not named_vector:
            raise Exception(
                "Error: Named vector name is only supported with named vectors. Please use --named_vector to enable named vectors."
            )

        # A comma-separated --named_vector_name creates one named vector per name.
        named_vector_names = [
            name.strip()
            for name in (named_vector_name or "").split(",")
            if name.strip()
        ]
        if named_vector:
            if not named_vector_names:
                raise Exception(
                    "Error: --named_vector_name must contain at least one non-empty name."
                )
            if len(named_vector_names) != len(set(named_vector_names)):
                raise Exception(
                    "Error: --named_vector_name contains duplicate names; each named vector must have a unique name."
                )

        distance_metric_enum = self._resolve_distance_metric(distance_metric)

        vector_index_map: Dict[str, wvc.VectorIndexConfig] = {
            "hnsw": wvc.Configure.VectorIndex.hnsw(
                distance_metric=distance_metric_enum
            ),
            "flat": wvc.Configure.VectorIndex.flat(
                distance_metric=distance_metric_enum
            ),
            "dynamic": wvc.Configure.VectorIndex.dynamic(),
            "dynamic_flat_bq": wvc.Configure.VectorIndex.dynamic(
                flat=wvc.Configure.VectorIndex.flat(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.bq(),
                    distance_metric=distance_metric_enum,
                )
            ),
            "dynamic_flat_bq_hnsw_pq": wvc.Configure.VectorIndex.dynamic(
                flat=wvc.Configure.VectorIndex.flat(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                        rescore_limit=rescore_limit
                    ),
                    distance_metric=distance_metric_enum,
                ),
                hnsw=wvc.Configure.VectorIndex.hnsw(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.pq(
                        training_limit=training_limit
                    ),
                    distance_metric=distance_metric_enum,
                ),
            ),
            "dynamic_flat_bq_hnsw_sq": wvc.Configure.VectorIndex.dynamic(
                flat=wvc.Configure.VectorIndex.flat(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                        rescore_limit=rescore_limit
                    ),
                    distance_metric=distance_metric_enum,
                ),
                hnsw=wvc.Configure.VectorIndex.hnsw(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.sq(
                        rescore_limit=rescore_limit, training_limit=training_limit
                    ),
                    distance_metric=distance_metric_enum,
                ),
            ),
            "dynamic_flat_bq_hnsw_bq": wvc.Configure.VectorIndex.dynamic(
                flat=wvc.Configure.VectorIndex.flat(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                        rescore_limit=rescore_limit
                    ),
                    distance_metric=distance_metric_enum,
                ),
                hnsw=wvc.Configure.VectorIndex.hnsw(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                        rescore_limit=rescore_limit
                    ),
                    distance_metric=distance_metric_enum,
                ),
            ),
            "dynamic_hnsw_pq": wvc.Configure.VectorIndex.dynamic(
                hnsw=wvc.Configure.VectorIndex.hnsw(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.pq(
                        training_limit=training_limit
                    )
                )
            ),
            "dynamic_hnsw_sq": wvc.Configure.VectorIndex.dynamic(
                hnsw=wvc.Configure.VectorIndex.hnsw(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.sq(
                        rescore_limit=rescore_limit, training_limit=training_limit
                    ),
                    distance_metric=distance_metric_enum,
                )
            ),
            "dynamic_hnsw_bq": wvc.Configure.VectorIndex.dynamic(
                hnsw=wvc.Configure.VectorIndex.hnsw(
                    quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                        rescore_limit=rescore_limit
                    ),
                    distance_metric=distance_metric_enum,
                )
            ),
            "hnsw_pq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.pq(
                    training_limit=training_limit
                ),
                distance_metric=distance_metric_enum,
            ),
            "hnsw_bq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                    rescore_limit=rescore_limit
                ),
                distance_metric=distance_metric_enum,
            ),
            "hnsw_bq_cache": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                    cache=True, rescore_limit=rescore_limit
                ),
                distance_metric=distance_metric_enum,
            ),
            "hnsw_sq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.sq(
                    rescore_limit=rescore_limit, training_limit=training_limit
                ),
                distance_metric=distance_metric_enum,
            ),
            "hnsw_rq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.rq(
                    rescore_limit=rescore_limit
                ),
                distance_metric=distance_metric_enum,
            ),
            "hnsw_acorn": wvc.Configure.VectorIndex.hnsw(
                filter_strategy=VectorFilterStrategy.ACORN,
                distance_metric=distance_metric_enum,
            ),
            "hnsw_multivector": wvc.Configure.VectorIndex.hnsw(
                multi_vector=wvc.Configure.VectorIndex.MultiVector.multi_vector(),
                distance_metric=distance_metric_enum,
            ),
            "flat_bq": wvc.Configure.VectorIndex.flat(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                    rescore_limit=rescore_limit
                ),
                distance_metric=distance_metric_enum,
            ),
            "flat_bq_cache": wvc.Configure.VectorIndex.flat(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq(
                    cache=True, rescore_limit=rescore_limit
                ),
                distance_metric=distance_metric_enum,
            ),
            "hfresh": self._build_hfresh_config(
                max_posting_size_kb=hfresh_max_posting_size_kb,
                distance_metric=distance_metric_enum,
                rescore_limit=rescore_limit,
                replicas=hfresh_replicas,
                search_probe=hfresh_search_probe,
            ),
        }

        # Vectorizer configurations
        vectorizers_config = {
            "contextionary": (
                wvc.Configure.NamedVectors.text2vec_contextionary,
                wvc.Configure.Vectorizer.text2vec_contextionary,
                {},
            ),
            "transformers": (
                wvc.Configure.NamedVectors.text2vec_transformers,
                wvc.Configure.Vectorizer.text2vec_transformers,
                {},
            ),
            "openai": (
                wvc.Configure.NamedVectors.text2vec_openai,
                wvc.Configure.Vectorizer.text2vec_openai,
                {"base_url": vectorizer_base_url if vectorizer_base_url else None},
            ),
            "ollama": (
                wvc.Configure.NamedVectors.text2vec_ollama,
                wvc.Configure.Vectorizer.text2vec_ollama,
                {
                    "model": "snowflake-arctic-embed:33m",
                    "api_endpoint": "http://ollama.weaviate.svc.cluster.local:11434",
                },
            ),
            "cohere": (
                wvc.Configure.NamedVectors.text2vec_cohere,
                wvc.Configure.Vectorizer.text2vec_cohere,
                {"base_url": vectorizer_base_url if vectorizer_base_url else None},
            ),
            "jinaai": (
                wvc.Configure.NamedVectors.text2vec_jinaai,
                wvc.Configure.Vectorizer.text2vec_jinaai,
                {
                    "base_url": vectorizer_base_url if vectorizer_base_url else None,
                },
            ),
            "weaviate": (
                wvc.Configure.NamedVectors.text2vec_weaviate,
                wvc.Configure.Vectorizer.text2vec_weaviate,
                {"base_url": vectorizer_base_url if vectorizer_base_url else None},
            ),
            "weaviate-1.5": (
                wvc.Configure.NamedVectors.text2vec_weaviate,
                wvc.Configure.Vectorizer.text2vec_weaviate,
                {
                    "base_url": vectorizer_base_url if vectorizer_base_url else None,
                    "model": "Snowflake/snowflake-arctic-embed-m-v1.5",
                },
            ),
            "model2vec": (
                wvc.Configure.NamedVectors.text2vec_model2vec,
                wvc.Configure.Vectorizer.text2vec_model2vec,
                {},
            ),
            "none": (
                wvc.Configure.NamedVectors.none,
                wvc.Configure.Vectorizer.none,
                {},
            ),
        }

        object_ttl_type_map: Dict[str, wvc.ObjectTTLType] = {
            "create": wvc.Configure.ObjectTTL.delete_by_creation_time(
                time_to_live=object_ttl_time,
                filter_expired_objects=object_ttl_filter_expired,
            ),
            "update": wvc.Configure.ObjectTTL.delete_by_update_time(
                time_to_live=object_ttl_time,
                filter_expired_objects=object_ttl_filter_expired,
            ),
            "property": wvc.Configure.ObjectTTL.delete_by_date_property(
                property_name=object_ttl_property_name
                or CreateCollectionDefaults.object_ttl_property_name,
                ttl_offset=object_ttl_time,
                filter_expired_objects=object_ttl_filter_expired,
            ),
        }

        # jinaai_colbert is only available as a named vector, so the set of
        # supported vectorizers depends on whether named vectors are enabled.
        if named_vector:
            named_vector_factories = {
                name: (named_func, params)
                for name, (named_func, _, params) in vectorizers_config.items()
            }
            named_vector_factories["jinaai_colbert"] = (
                wvc.Configure.NamedVectors.text2colbert_jinaai,
                {},
            )
            supported_vectorizers = list(named_vector_factories.keys())
        else:
            supported_vectorizers = list(vectorizers_config.keys())

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
            wvc.Property(name="coverImage", data_type=wvc.DataType.BLOB),
        ]

        rds_map = {
            "delete_on_conflict": wvc.ReplicationDeletionStrategy.DELETE_ON_CONFLICT,
            "no_automated_resolution": wvc.ReplicationDeletionStrategy.NO_AUTOMATED_RESOLUTION,
            "time_based_resolution": wvc.ReplicationDeletionStrategy.TIME_BASED_RESOLUTION,
        }

        try:
            if vectorizer not in supported_vectorizers:
                raise Exception(
                    f"Error: Vectorizer '{vectorizer}' is not supported. Please use one of the following: {supported_vectorizers}"
                )

            if named_vector:
                named_func, params = named_vector_factories[vectorizer]
                vectorizer_config = [
                    named_func(
                        name=name,
                        vector_index_config=vector_index_map[vector_index],
                        **params,
                    )
                    for name in named_vector_names
                ]
            else:
                _, default_func, params = vectorizers_config[vectorizer]
                vectorizer_config = default_func(**params)

            self.client.collections.create(
                name=collection,
                vector_index_config=(
                    vector_index_map[vector_index] if not named_vector else None
                ),
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
                    async_config=(
                        wvc.Configure.Replication.async_config(
                            **async_replication_config
                        )
                        if async_replication_config is not None
                        else None
                    ),
                ),
                sharding_config=(
                    wvc.Configure.sharding(desired_count=shards) if shards > 0 else None
                ),
                multi_tenancy_config=wvc.Configure.multi_tenancy(
                    enabled=multitenant,
                    auto_tenant_creation=auto_tenant_creation,
                    auto_tenant_activation=auto_tenant_activation,
                ),
                vectorizer_config=vectorizer_config,
                object_ttl_config=(
                    object_ttl_type_map[object_ttl_type]
                    if object_ttl_time is not None
                    else None
                ),
                properties=(properties if not force_auto_schema else None),
            )
        except Exception as e:

            raise Exception(f"Error creating Collection '{collection}': {e}")

        assert self.client.collections.exists(collection)

        if json_output:
            click.echo(
                json.dumps(
                    {
                        "status": "success",
                        "message": f"Collection '{collection}' created successfully in Weaviate.",
                    },
                    indent=2,
                )
            )
        else:
            click.echo(f"Collection '{collection}' created successfully in Weaviate.")

    @staticmethod
    def __check_drop_target(config, collection: str, vector_name: str) -> bool:
        """Validate the drop target and report whether its index is already marked dropped.

        Returns True when the vector's index was already dropped (server reports it as
        `vectorIndexType: "none"`). Re-issuing the drop is intentionally allowed in that
        case: it is a no-op while cleanup is in flight and re-enqueues a fresh cleanup task
        if the previous one FAILED — the only way for an operator to recover a stuck drop.
        Only the two genuinely invalid states raise.
        """
        vector_config = config.vector_config
        if not vector_config:
            raise Exception(
                f"Collection '{collection}' has no named vectors. Only the index "
                "of a named vector can be dropped."
            )
        if vector_name not in vector_config:
            raise Exception(
                f"Named vector '{vector_name}' does not exist in collection "
                f"'{collection}'. Available named vectors: "
                f"{', '.join(sorted(vector_config))}."
            )
        return vector_config[vector_name].vector_index_config is None

    @staticmethod
    def __drop_vector_index(
        col_obj: Collection, collection: str, vector_name: str
    ) -> None:
        try:
            col_obj.config.delete_vector_index(vector_name=vector_name)
        except Exception as e:
            raise Exception(
                f"Failed to drop the index of named vector '{vector_name}' in collection "
                f"'{collection}': {e}. This endpoint is experimental, make sure Weaviate "
                "is started with "
                "ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT=true."
            )

    # Local, no-API-key vectorizers usable for a freshly added named vector.
    _ADD_VECTOR_FACTORIES = {
        "none": wvc.Configure.Vectors.self_provided,
        "contextionary": wvc.Configure.Vectors.text2vec_contextionary,
        "transformers": wvc.Configure.Vectors.text2vec_transformers,
        "model2vec": wvc.Configure.Vectors.text2vec_model2vec,
    }
    _ADD_VECTOR_INDEX_TYPES = (
        "hnsw",
        "flat",
        "hnsw_pq",
        "hnsw_sq",
        "hnsw_bq",
        "hnsw_rq",
        "hfresh",
        "flat_bq",
        "hnsw_acorn",
    )

    @staticmethod
    def __add_vector_index_config(
        index_type: str, training_limit: int
    ) -> "wvc.VectorIndexConfig":
        """Build a create-style index config for a freshly added named vector."""
        index_map: Dict[str, wvc.VectorIndexConfig] = {
            "hnsw": wvc.Configure.VectorIndex.hnsw(),
            "flat": wvc.Configure.VectorIndex.flat(),
            "hnsw_pq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.pq(
                    training_limit=training_limit
                )
            ),
            "hnsw_sq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.sq(
                    training_limit=training_limit
                )
            ),
            "hnsw_bq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq()
            ),
            "hnsw_rq": wvc.Configure.VectorIndex.hnsw(
                quantizer=wvc.Configure.VectorIndex.Quantizer.rq()
            ),
            "hfresh": wvc.Configure.VectorIndex.hfresh(),
            "flat_bq": wvc.Configure.VectorIndex.flat(
                quantizer=wvc.Configure.VectorIndex.Quantizer.bq()
            ),
            "hnsw_acorn": wvc.Configure.VectorIndex.hnsw(
                filter_strategy=VectorFilterStrategy.ACORN
            ),
        }
        return index_map[index_type]

    @staticmethod
    def __add_vector(
        col_obj: Collection,
        collection: str,
        vector_name: str,
        vectorizer: str,
        index_type: str,
        training_limit: int,
    ) -> None:
        factory = CollectionManager._ADD_VECTOR_FACTORIES[vectorizer]
        index_config = CollectionManager.__add_vector_index_config(
            index_type, training_limit
        )
        try:
            col_obj.config.add_vector(
                vector_config=factory(
                    name=vector_name,
                    vector_index_config=index_config,
                )
            )
        except Exception as e:
            raise Exception(
                f"Failed to add named vector '{vector_name}' to collection "
                f"'{collection}': {e}."
            )

    def update_collection(
        self,
        collection: str = UpdateCollectionDefaults.collection,
        description: Optional[str] = UpdateCollectionDefaults.description,
        vector_index: Optional[str] = UpdateCollectionDefaults.vector_index,
        training_limit: int = UpdateCollectionDefaults.training_limit,
        async_enabled: Optional[bool] = UpdateCollectionDefaults.async_enabled,
        replication_factor: Optional[int] = UpdateCollectionDefaults.replication_factor,
        auto_tenant_creation: Optional[
            bool
        ] = UpdateCollectionDefaults.auto_tenant_creation,
        auto_tenant_activation: Optional[
            bool
        ] = UpdateCollectionDefaults.auto_tenant_activation,
        replication_deletion_strategy: Optional[
            str
        ] = UpdateCollectionDefaults.replication_deletion_strategy,
        json_output: bool = False,
        object_ttl_type: str = UpdateCollectionDefaults.object_ttl_type,
        object_ttl_time: Optional[int] = UpdateCollectionDefaults.object_ttl_time,
        object_ttl_filter_expired: Optional[
            bool
        ] = UpdateCollectionDefaults.object_ttl_filter_expired,
        object_ttl_property_name: Optional[
            str
        ] = UpdateCollectionDefaults.object_ttl_property_name,
        async_replication_config: Optional[Dict[str, int]] = None,
        drop_vector_index: Optional[str] = UpdateCollectionDefaults.drop_vector_index,
        add_vector: Optional[str] = UpdateCollectionDefaults.add_vector,
        add_vector_vectorizer: str = UpdateCollectionDefaults.add_vector_vectorizer,
        add_vector_index_type: str = UpdateCollectionDefaults.add_vector_index_type,
    ) -> None:

        if (
            object_ttl_type not in ("property", "disable")
            and object_ttl_property_name
            != UpdateCollectionDefaults.object_ttl_property_name
        ):
            raise Exception(
                "object_ttl_property_name is only valid when object_ttl_type is 'property'."
            )
        if async_replication_config is not None and async_enabled is False:
            raise Exception(
                "Error: --async_replication_config cannot be used when --async_enabled is False."
            )
        if drop_vector_index is not None and vector_index is not None:
            raise Exception(
                "--drop_vector_index cannot be combined with --vector_index. "
                "Dropping an index and reconfiguring it in the same call is contradictory."
            )
        if add_vector is not None and drop_vector_index is not None:
            raise Exception(
                "--add_vector cannot be combined with --drop_vector_index in the same call."
            )
        if add_vector is not None and vector_index is not None:
            raise Exception("--add_vector cannot be combined with --vector_index.")
        if (
            add_vector is not None
            and add_vector_vectorizer not in self._ADD_VECTOR_FACTORIES
        ):
            raise Exception(
                f"Vectorizer '{add_vector_vectorizer}' is not supported for --add_vector. "
                f"Choose one of: {list(self._ADD_VECTOR_FACTORIES)}."
            )
        if (
            add_vector is not None
            and add_vector_index_type not in self._ADD_VECTOR_INDEX_TYPES
        ):
            raise Exception(
                f"Index type '{add_vector_index_type}' is not supported for --add_vector. "
                f"Choose one of: {list(self._ADD_VECTOR_INDEX_TYPES)}."
            )

        if async_replication_config is not None and older_than_version(
            self.client, "1.36.0"
        ):
            click.echo(
                "Warning: --async_replication_config requires Weaviate >= v1.36.0. "
                "The server may ignore or reject these settings."
            )

        if drop_vector_index is not None and older_than_version(self.client, "1.39.0"):
            click.echo(
                "Warning: --drop_vector_index requires Weaviate >= v1.39.0. "
                "The server may reject this request."
            )

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

        object_ttl_type_map: Dict[str, wvc.ObjectTTLType] = {
            "create": wvc.Reconfigure.ObjectTTL.delete_by_creation_time(
                time_to_live=object_ttl_time,
                filter_expired_objects=object_ttl_filter_expired,
            ),
            "update": wvc.Reconfigure.ObjectTTL.delete_by_update_time(
                time_to_live=object_ttl_time,
                filter_expired_objects=object_ttl_filter_expired,
            ),
            "property": wvc.Reconfigure.ObjectTTL.delete_by_date_property(
                property_name=object_ttl_property_name
                or UpdateCollectionDefaults.object_ttl_property_name,
                ttl_offset=object_ttl_time,
                filter_expired_objects=object_ttl_filter_expired,
            ),
            "disable": wvc.Reconfigure.ObjectTTL.disable(),
        }

        col_obj: Collection = self.client.collections.get(collection)
        current_config = col_obj.config.get()
        drop_already_marked = False
        if drop_vector_index is not None:
            drop_already_marked = self.__check_drop_target(
                current_config, collection, drop_vector_index
            )
        rf = (
            replication_factor
            if replication_factor is not None
            else current_config.replication_config.factor
        )
        rds_map = {
            "delete_on_conflict": wvc.ReplicationDeletionStrategy.DELETE_ON_CONFLICT,
            "no_automated_resolution": wvc.ReplicationDeletionStrategy.NO_AUTOMATED_RESOLUTION,
            "time_based_resolution": wvc.ReplicationDeletionStrategy.TIME_BASED_RESOLUTION,
        }
        mt = current_config.multi_tenancy_config.enabled
        auto_tenant_creation = (
            auto_tenant_creation
            if auto_tenant_creation is not None
            else current_config.multi_tenancy_config.auto_tenant_creation
        )
        auto_tenant_activation = (
            auto_tenant_activation
            if auto_tenant_activation is not None
            else current_config.multi_tenancy_config.auto_tenant_activation
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
                    async_config=(
                        wvc.Reconfigure.Replication.async_config(
                            **async_replication_config
                        )
                        if async_replication_config is not None
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
            object_ttl_config=(
                object_ttl_type_map[object_ttl_type]
                if object_ttl_time is not None or object_ttl_type == "disable"
                else None
            ),
        )

        assert self.client.collections.exists(collection)

        # Dropped last: `config.update()` reads the whole schema and writes it back, so
        # doing this first would send a `vectorIndexType: "none"` vector back to Weaviate.
        if drop_vector_index is not None:
            self.__drop_vector_index(col_obj, collection, drop_vector_index)

        if add_vector is not None:
            self.__add_vector(
                col_obj,
                collection,
                add_vector,
                add_vector_vectorizer,
                add_vector_index_type,
                training_limit,
            )

        message = f"Collection '{collection}' modified successfully in Weaviate."
        if drop_vector_index is not None:
            if drop_already_marked:
                message += (
                    f" The index of named vector '{drop_vector_index}' was already dropped; "
                    "the request was re-issued to re-trigger cleanup if it had stalled."
                )
            else:
                message += (
                    f" Dropping the index of named vector '{drop_vector_index}' was accepted; "
                    "the removal runs asynchronously. Once it finalizes the vector can be "
                    "re-created as a fresh, empty index."
                )
        if add_vector is not None:
            message += (
                f" Named vector '{add_vector}' was added with a fresh "
                f"'{add_vector_index_type}' index (vectorizer: {add_vector_vectorizer})."
            )

        if json_output:
            result: Dict[str, str] = {"status": "success", "message": message}
            if drop_vector_index is not None:
                result["dropped_vector_index"] = drop_vector_index
            if add_vector is not None:
                result["added_vector"] = add_vector
            click.echo(json.dumps(result, indent=2))
        else:
            click.echo(message)

    def delete_collection(
        self,
        collection: str = DeleteCollectionDefaults.collection,
        all: bool = DeleteCollectionDefaults.all,
        json_output: bool = False,
    ) -> None:
        if all:
            collections: List[str] = self.client.collections.list_all()
            for collection in collections:
                if not json_output:
                    click.echo(f"Deleting collection '{collection}'")
                self.client.collections.delete(collection)
            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "message": "All collections deleted successfully in Weaviate.",
                        },
                        indent=2,
                    )
                )
            else:
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

            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "message": f"Collection '{collection}' deleted successfully in Weaviate.",
                        },
                        indent=2,
                    )
                )
            else:
                click.echo(
                    f"Collection '{collection}' deleted successfully in Weaviate."
                )
