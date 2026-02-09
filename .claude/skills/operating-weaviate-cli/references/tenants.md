# Tenants Reference

Manage tenants in multi-tenant Weaviate collections.

## Create Tenants
```bash
weaviate-cli create tenants \
  --collection "Movies" \
  --number_tenants 100 \
  --tenant_suffix "Tenant" \
  --state active \
  --json
```
Creates tenants named `Tenant-0` through `Tenant-99`.

Options: `--tenant_batch_size N` for batched creation.

## Get Tenants
```bash
weaviate-cli get tenants --collection "Movies" --json
weaviate-cli get tenants --collection "Movies" --tenant_id "Tenant-1" --verbose --json
```

## Update Tenant State
```bash
weaviate-cli update tenants --collection "Movies" --state cold --number_tenants 100 --json
weaviate-cli update tenants --collection "Movies" --state hot --tenants "Tenant-1,Tenant-2" --json
```

## Delete Tenants
```bash
weaviate-cli delete tenants --collection "Movies" --number_tenants 100 --json
weaviate-cli delete tenants --collection "Movies" --tenants "Tenant-1,Tenant-2" --json
weaviate-cli delete tenants --collection "Movies" --tenant_suffix "*" --number_tenants 50 --json
```

Using `--tenant_suffix "*"` ignores the suffix pattern and selects from **all** existing tenants regardless of name. It then deletes up to `--number_tenants` of them. Use with caution as it may delete tenants with any name.

## Tenant State Machine

```
hot/active  <-->  cold/inactive
    ^   \              ^
    |    \             |
    v     v            v
      frozen/offloaded
```

| State | Aliases | Description |
|-------|---------|-------------|
| `hot` / `active` | Equivalent | Data in memory, queryable, accepts writes |
| `cold` / `inactive` | Equivalent | Data on disk, not queryable |
| `frozen` / `offloaded` | Equivalent | Data in cloud storage, not queryable |

- Tenants can be offloaded directly from `hot`/`active` or from `cold`/`inactive`
- Tenants can be activated back to `hot` from any state
- Data operations (create/query/update/delete data) require `hot`/`active` state
- Transitioning to `hot` from `frozen` may take time (download from cloud storage)

## Prerequisites

1. Collection must exist with `--multitenant` enabled
2. For state transitions: tenants must already exist
3. For data operations on tenants: tenants must be in `hot`/`active` state

## Notes

- Tenant names are case-sensitive
- The `--tenant_suffix` default is "Tenant", creating names like "Tenant-0", "Tenant-1", etc.
- Deleting tenants also deletes all data within them
