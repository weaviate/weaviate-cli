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
- `--vector_index` -- Index type: hnsw, flat, dynamic, hnsw_pq, hnsw_bq, hnsw_sq, hnsw_rq, hnsw_acorn, hnsw_multivector, flat_bq, dynamic_*
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
- `--named_vector_name` -- Named vector name (default: "default")
- `--replication_deletion_strategy` -- delete_on_conflict, no_automated_resolution, time_based_resolution
- `--object_ttl_type` -- TTL event type: create, update, property (default: "create")
- `--object_ttl_time` -- Time to live in seconds (default: None, TTL disabled when omitted)
- `--object_ttl_filter_expired` -- Filter expired-but-not-yet-deleted objects from queries
- `--object_ttl_property_name` -- Date property name for TTL when `object_ttl_type=property` (default: "releaseDate"). **Only valid when `--object_ttl_type=property`**; rejected otherwise.

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

Mutable fields: `--async_enabled`, `--replication_factor`, `--vector_index`, `--description`, `--training_limit`, `--auto_tenant_creation`, `--auto_tenant_activation`, `--replication_deletion_strategy`, `--object_ttl_type`, `--object_ttl_time`, `--object_ttl_filter_expired`, `--object_ttl_property_name` (only when `object_ttl_type=property`)

**Immutable (cannot change after creation):** multitenant, vectorizer, named_vector, shards

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
