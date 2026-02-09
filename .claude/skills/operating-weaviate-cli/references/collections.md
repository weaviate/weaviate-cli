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

## Update Collection (mutable fields only)
```bash
weaviate-cli update collection \
  --collection "MyCollection" \
  --description "Updated description" \
  --replication_factor 5 \
  --json
```

Mutable fields: `--async_enabled`, `--replication_factor`, `--vector_index`, `--description`, `--training_limit`, `--auto_tenant_creation`, `--auto_tenant_activation`, `--replication_deletion_strategy`

**Immutable (cannot change after creation):** multitenant, vectorizer, named_vector, shards

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
