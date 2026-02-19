# Data Reference

Ingest, update, or delete data in Weaviate collections.

## Ingest Data
```bash
weaviate-cli create data \
  --collection "Movies" \
  --limit 1000 \
  --randomize \
  --json
```

**Options:**
- `--collection` -- Target collection (default: "Movies")
- `--limit` -- Number of objects (default: 1000)
- `--consistency_level` -- quorum, all, one (default: quorum)
- `--randomize` -- Generate random data
- `--skip-seed` -- Skip seeding random data
- `--vector_dimensions` -- Dimensions for random vectors (default: 1536, requires --randomize)
- `--uuid` -- Specific UUID (requires --randomize and --limit=1)
- `--batch_size` -- Objects per batch (default: 1000)
- `--concurrent_requests` -- Parallel requests (default: CPU+4, max 32)
- `--multi_vector` -- Enable multi-vector ingestion
- `--dynamic_batch` -- Enable dynamic batching (requires --randomize)
- `--wait_for_indexing` -- Wait for indexing to complete
- `--verbose` -- Show detailed progress

**Multi-tenant ingestion:**
- `--auto_tenants N` -- Send data to N auto-created tenants (requires collection with --auto_tenant_creation)
- `--tenants "T1,T2"` -- Send data to specific tenants
- `--tenant_suffix` -- Prefix for auto-tenant names (default: "Tenant")

## Update Data
```bash
weaviate-cli update data --collection "Movies" --limit 100 --randomize --json
```

## Delete Data
```bash
weaviate-cli delete data --collection "Movies" --limit 100 --json
weaviate-cli delete data --collection "Movies" --uuid "abc-123" --json
weaviate-cli delete data --collection "Movies" --tenants "T1,T2" --limit 50 --json
```

## Prerequisites

1. **Collection must exist** -- run `create collection` first
2. **For multi-tenant collections:** tenants must exist OR `--auto_tenant_creation` must be enabled on the collection
3. **For multi-tenant data ingestion:** tenants must be in `hot`/`active` state
4. **For vectorizer-based ingestion (non-randomize):** the collection uses a built-in dataset; vectorizer API keys may be needed

## Notes

- `--randomize` generates synthetic data with random vectors -- useful for testing
- Without `--randomize`, the CLI uses a built-in Movies dataset
- `--uuid` is only valid with `--randomize` and `--limit=1`
- `--dynamic_batch` is only valid with `--randomize`
