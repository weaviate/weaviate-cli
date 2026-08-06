# Collections Reference

Manage Weaviate collections (schemas).

## List All Collections
```bash
weaviate-cli get collection --json
```

## Get Specific Collection
```bash
weaviate-cli get collection --collection "CollectionName" --json
```

## Create Collection
```bash
weaviate-cli create collection \
  --collection "MyCollection" \
  --replication_factor 3 \
  --vector_index hnsw \
  --vectorizer none \
  --json
```

**Full options:**
- `--collection` -- Name (default: "Movies")
- `--replication_factor` -- Number of replicas (default: 3)
- `--async_enabled` -- Enable async replication
- `--vector_index` -- Index type: hnsw, flat, dynamic, hnsw_pq, hnsw_bq, hnsw_sq, hnsw_rq, hnsw_acorn, hnsw_multivector, flat_bq, dynamic_*, hfresh
- `--inverted_index` -- Inverted index: timestamp, null, length
- `--training_limit` -- PQ/SQ training limit (default: 10000)
- `--multitenant` -- Enable multi-tenancy
- `--auto_tenant_creation` -- Auto-create tenants on data ingestion
- `--auto_tenant_activation` -- Auto-activate tenants on access
- `--force_auto_schema` -- Let auto-schema infer properties
- `--shards` -- Number of shards (default: 0, meaning Weaviate auto-determines)
- `--vectorizer` -- Vectorizer: contextionary, transformers, openai, ollama, cohere, jinaai, jinaai_colbert, weaviate, weaviate-1.5, model2vec, none
- `--vectorizer_base_url` -- Custom vectorizer URL
- `--named_vector` -- Enable named vectors
- `--named_vector_name` -- Name(s) of the named vector(s). Comma-separate to create several, e.g. `"vec_a,vec_b"` (default: "default"). All named vectors share the chosen `--vectorizer` and `--vector_index`.
- `--replication_deletion_strategy` -- delete_on_conflict, no_automated_resolution, time_based_resolution
- `--object_ttl_type` -- TTL event type: create, update, property (default: "create")
- `--object_ttl_time` -- Time to live in seconds (default: None, TTL disabled when omitted)
- `--object_ttl_filter_expired` -- Filter expired-but-not-yet-deleted objects from queries
- `--object_ttl_property_name` -- Date property name for TTL when `object_ttl_type=property` (default: "releaseDate"). **Only valid when `--object_ttl_type=property`**; rejected otherwise.
- `--hfresh_max_posting_size_kb` -- (hfresh only) Max posting list size in KB (default: None, uses server default)
- `--hfresh_replicas` -- (hfresh only) Number of replicas per element across posting lists (default: None, uses server default)
- `--hfresh_search_probe` -- (hfresh only) Search probe size (default: None, uses server default)
- `--distance_metric` -- Distance metric: cosine, dot, l2-squared, hamming, manhattan (default: None, uses server default). Applies to all vector index types.
- `--rescore_limit` -- Rescore limit for quantized indexes (default: None, uses server default)
- `--async_replication_config` -- Async replication tuning as `key=value` pairs (repeatable). Valid keys: `max_workers`, `hashtree_height`, `frequency`, `frequency_while_propagating`, `alive_nodes_checking_frequency`, `logging_frequency`, `diff_batch_size`, `diff_per_node_timeout`, `pre_propagation_timeout`, `propagation_timeout`, `propagation_limit`, `propagation_delay`, `propagation_concurrency`, `propagation_batch_size`. All values must be integers. Use `reset` to revert all to server defaults. Requires `--async_enabled` on create and Weaviate >= v1.36.0.

**hfresh examples:**
```bash
# Basic hfresh collection
weaviate-cli create collection --collection Movies --vector_index hfresh --json

# hfresh with all tuning parameters
weaviate-cli create collection \
  --collection Movies \
  --vector_index hfresh \
  --hfresh_max_posting_size_kb 64 \
  --hfresh_replicas 2 \
  --hfresh_search_probe 100 \
  --distance_metric cosine \
  --rescore_limit 200 \
  --json
```

**Async replication config examples:**
```bash
# Create with custom async replication tuning
weaviate-cli create collection --collection MyCol --async_enabled \
  --async_replication_config max_workers=10 \
  --async_replication_config frequency=60 \
  --async_replication_config propagation_concurrency=4

# Set a single parameter
weaviate-cli create collection --collection MyCol --async_enabled \
  --async_replication_config propagation_batch_size=200
```

**Object TTL examples:**
```bash
# Delete objects 1 hour after creation
weaviate-cli create collection --collection Movies --object_ttl_type create --object_ttl_time 3600

# Delete objects 24 hours after last update, filtering expired objects
weaviate-cli create collection --collection Movies --object_ttl_type update --object_ttl_time 86400 --object_ttl_filter_expired

# Delete objects based on default date property (releaseDate)
weaviate-cli create collection --collection Movies --object_ttl_type property --object_ttl_time 0

# Delete objects based on a custom date property (e.g. for clusters not using weaviate-cli schema)
weaviate-cli create collection --collection MyCollection --object_ttl_type property --object_ttl_time 86400 --object_ttl_property_name expiresAt --json
```

## Update Collection (mutable fields only)
```bash
weaviate-cli update collection \
  --collection "MyCollection" \
  --description "Updated description" \
  --replication_factor 5 \
  --json
```

Mutable fields: `--async_enabled`, `--replication_factor`, `--vector_index`, `--description`, `--training_limit`, `--auto_tenant_creation`, `--auto_tenant_activation`, `--replication_deletion_strategy`, `--async_replication_config`, `--object_ttl_type`, `--object_ttl_time`, `--object_ttl_filter_expired`, `--object_ttl_property_name` (only when `object_ttl_type=property`), `--drop_vector_index`, `--add_vector`

**Immutable (cannot change after creation):** multitenant, vectorizer, named_vector, shards

## Drop a Named Vector Index (destructive)
```bash
weaviate-cli update collection --collection Movies --drop_vector_index title_vector --json
```

Removes the index of one named vector. The index is deleted from disk and the vector can no
longer be searched; the stored vectors are stripped by background cleanup. **The vector can be
re-created afterwards** as a fresh, empty index with `--add_vector` (see below) once the drop
has finalized -- you then re-ingest to repopulate it.

- Named vectors only -- a collection with a single legacy vector is rejected.
- Cannot be combined with `--vector_index` (reconfiguring an index you are deleting is contradictory).
- Requires Weaviate **>= v1.39.0** started with `ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT=true`.
  The endpoint is experimental and disabled by default; without the flag the server answers with a 500.
- The drop is applied **asynchronously**. A success message means Weaviate accepted the request,
  not that the index is already gone. Poll `get collection --collection <name>` to observe completion.

Once dropped, the vector is reported by the server as `vectorIndexType: "none"` with no
`vectorIndexConfig`. In the collection listing it shows up as `none` in the **Vector Index**
column (e.g. `hnsw, none` when only some of the named vectors were dropped).

**Re-creation timing.** While the drop is in progress (vector shows `none`), re-creating the
same name is rejected. Once it finalizes (the vector disappears from the schema), the name can
be added again as a brand-new, empty index; the original index and any vectors already stripped
are not restored.

**Re-triggering a stalled drop.** Re-issuing `--drop_vector_index` on a vector that already
shows `none` is allowed and returns success. While cleanup is still running it is a no-op;
if the background cleanup had FAILED, it re-enqueues a fresh cleanup task. This is the only
way to recover a drop whose marker is stuck at `none`. The command reports that the vector
was already dropped and that it re-triggered cleanup.

## Add a Named Vector
```bash
# Re-create a vector after its drop finalized, or add a brand-new one
weaviate-cli update collection --collection Movies --add_vector title_vector --json
weaviate-cli update collection --collection Movies --add_vector title_vector \
  --add_vector_vectorizer model2vec --add_vector_index_type hnsw_pq
```

Adds a named vector with a fresh index (the client's `config.add_vector()` under the hood).
This is the CLI-native way to re-create a vector whose index was dropped, once the drop has
finalized -- then re-ingest to repopulate it.

- `--add_vector_vectorizer` -- `none` (default, self-provided vectors), `contextionary`,
  `transformers`, or `model2vec`. Only the local, no-API-key vectorizers are offered.
- `--add_vector_index_type` -- `hnsw` (default), `flat`, `hnsw_pq`, `hnsw_sq`, `hnsw_bq`,
  `hnsw_rq`, `hfresh`, `flat_bq`, or `hnsw_acorn`. The `pq`/`sq` variants use `--training_limit`.
- Cannot be combined with `--drop_vector_index` or `--vector_index`.
- Re-adding a name that still shows `none` (drop not finalized) is rejected by the server; wait
  for the vector to disappear from the schema first.

**Async replication config examples (update):**
```bash
# Update async replication tuning on existing collection
weaviate-cli update collection --collection MyCol \
  --async_replication_config max_workers=20 \
  --async_replication_config propagation_batch_size=100
```

**Object TTL options for update:**
- `--object_ttl_type` -- TTL event type: create, update, property, **disable** (default: "create")
- `--object_ttl_time` -- Time to live in seconds (set together with type to enable TTL)
- `--object_ttl_filter_expired` -- Filter expired-but-not-yet-deleted objects (type: bool)
- `--object_ttl_property_name` -- Date property name when `object_ttl_type=property` (default: "releaseDate"). **Only valid when `--object_ttl_type=property`**; rejected otherwise.

**Object TTL examples:**
```bash
# Enable TTL: delete objects 2 hours after creation
weaviate-cli update collection --collection Movies --object_ttl_type create --object_ttl_time 7200

# Disable TTL on an existing collection
weaviate-cli update collection --collection Movies --object_ttl_type disable

# Set TTL by custom date property on an existing collection
weaviate-cli update collection --collection MyCollection --object_ttl_type property --object_ttl_time 86400 --object_ttl_property_name expiresAt --json
```

## Delete Collection
```bash
weaviate-cli delete collection --collection "MyCollection" --json
weaviate-cli delete collection --all --json
```

## Prerequisites

- Weaviate cluster must be reachable
- For RBAC clusters: user needs collection permissions

## Notes

- Collection names are case-sensitive
- Deleting a collection removes all its data, tenants, and shards
- `--all` deletes every collection -- use with caution
