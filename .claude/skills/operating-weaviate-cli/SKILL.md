---
name: operating-weaviate-cli
description: >-
  Operates Weaviate vector databases through the weaviate-cli tool. Use this
  skill whenever "weaviate-cli" appears in the prompt, or when the user wants
  to interact with a running Weaviate cluster. Trigger if the request:
  mentions weaviate-cli explicitly; asks to create, query, update, or delete
  collections, tenants, or data in Weaviate; needs to manage RBAC roles and
  users; wants to run, restore, or cancel backups; asks about cluster health,
  node status, or shards; needs to manage replication or aliases; wants to
  benchmark a Weaviate cluster; or is testing/verifying Weaviate functionality
  from the command line (even if the primary task is testing code changes).
  Do NOT use for developing or modifying the weaviate-cli source code — use
  contributing-to-weaviate-cli for that.
---

# Operating weaviate-cli

Manage Weaviate vector databases through the `weaviate-cli` command-line tool.

## Prerequisites

Install the CLI:
```bash
pip install weaviate-cli
# or
brew install weaviate/tap/weaviate-cli
```

Verify connectivity:
```bash
weaviate-cli get nodes --minimal --json
```

## Configuration

The CLI reads configuration from `~/.config/weaviate/config.json` by default. Use `--config-file <path>` to override.

**Local development (no config needed):** The CLI defaults to `localhost:8080` (HTTP) and `localhost:50051` (gRPC). No config file is required for local clusters.

**When authentication is needed**, create a config file under `~/.config/weaviate/` with a descriptive filename:

```bash
# API key auth
cat > ~/.config/weaviate/cloud-staging.json << 'EOF'
{
    "host": "https://your-cluster.weaviate.cloud",
    "auth": {
        "type": "api_key",
        "api_key": "your-api-key"
    }
}
EOF
weaviate-cli --config-file ~/.config/weaviate/cloud-staging.json get nodes --minimal --json
```

```bash
# RBAC / per-user auth (requires --user flag on every command)
cat > ~/.config/weaviate/local-rbac.json << 'EOF'
{
    "host": "localhost",
    "http_port": "8080",
    "grpc_port": "50051",
    "auth": {
        "type": "user",
        "admin-user": "admin-key",
        "readonly-user": "readonly-key"
    }
}
EOF
weaviate-cli --config-file ~/.config/weaviate/local-rbac.json --user admin-user get nodes --minimal --json
```

```bash
# Custom host with separate gRPC endpoint
cat > ~/.config/weaviate/custom-host.json << 'EOF'
{
    "host": "https://custom-host.example.com",
    "grpc_host": "https://custom-grpc.example.com",
    "http_port": 443,
    "grpc_port": 443,
    "auth": {
        "type": "api_key",
        "api_key": "your-api-key"
    }
}
EOF
```

### Config management rules

- **Never overwrite** `~/.config/weaviate/config.json` without explicit user consent.
- When creating configs for the agent, use descriptive filenames (e.g., `local-rbac.json`, `cloud-prod.json`).
- If the user's prompt specifies a config file, use it. Otherwise, assume local defaults (no `--config-file`).
- Always verify connectivity after creating or switching configs: `weaviate-cli [--config-file ...] get nodes --minimal --json`

## Command Syntax

```
weaviate-cli [--config-file FILE] [--user USER] <group> <command> [--json] [options]
```

### Global Options

| Option | Description |
|--------|-------------|
| `--config-file <path>` | Path to config JSON file (overrides default) |
| `--user <name>` | User for RBAC-enabled clusters (required when auth type is `user`) |
| `--version` | Print CLI version |
| `--json` | Output in JSON format (use on every command for agent-friendly output) |

### Command Groups

| Group | Description |
|-------|-------------|
| `create` | Create collections, tenants, data, backups, roles, users, aliases, replications |
| `get` | Inspect collections, tenants, shards, backups, roles, users, nodes, aliases, replications |
| `update` | Update collections, tenants, shards, data, users, aliases |
| `delete` | Delete collections, tenants, data, roles, users, aliases, replications |
| `query` | Query data (fetch/vector/keyword/hybrid/uuid), replications, sharding state |
| `restore` | Restore backups |
| `cancel` | Cancel backups and replications |
| `assign` | Assign roles to users, permissions to roles |
| `revoke` | Revoke roles from users, permissions from roles |
| `benchmark` | Run QPS benchmarks |

## Command Reference

**Always use `--json` for programmatic consumption.**

### Collections

```bash
weaviate-cli create collection --collection MyCollection --replication_factor 3 --vector_index hnsw --vectorizer none --json
weaviate-cli get collection --json                          # List all
weaviate-cli get collection --collection MyCollection --json # Specific
weaviate-cli update collection --collection MyCollection --description "Updated" --replication_factor 5 --json
weaviate-cli delete collection --collection MyCollection --json
weaviate-cli delete collection --all --json
```

Key create options: `--multitenant`, `--auto_tenant_creation`, `--auto_tenant_activation`, `--shards N`, `--vectorizer <type>`, `--named_vector`, `--replication_deletion_strategy`

Mutable fields: `--async_enabled`, `--replication_factor`, `--vector_index`, `--description`, `--training_limit`, `--auto_tenant_creation`, `--auto_tenant_activation`, `--replication_deletion_strategy`

See [references/collections.md](references/collections.md) for full options.

### Data

```bash
weaviate-cli create data --collection Movies --limit 1000 --randomize --json
weaviate-cli query data --collection Movies --search_type hybrid --query "Action movie" --limit 10 --json
weaviate-cli update data --collection Movies --limit 100 --randomize --json
weaviate-cli delete data --collection Movies --limit 100 --json
weaviate-cli delete data --collection Movies --uuid "abc-123" --json
```

Search types: `fetch`, `vector` (near_text), `keyword` (bm25), `hybrid`, `uuid`

Key data options: `--consistency_level`, `--auto_tenants N`, `--tenants "T1,T2"`, `--vector_dimensions`, `--batch_size`, `--concurrent_requests`, `--wait_for_indexing`, `--dynamic_batch`, `--multi_vector`

Key query options: `--properties "title,keywords"`, `--tenants "T1"`, `--target_vector "default"`, `--consistency_level`

See [references/data.md](references/data.md) and [references/search.md](references/search.md).

### Tenants

```bash
weaviate-cli create tenants --collection Movies --number_tenants 100 --tenant_suffix "Tenant" --state active --json
weaviate-cli get tenants --collection Movies --json
weaviate-cli get tenants --collection Movies --tenant_id "Tenant-1" --verbose --json
weaviate-cli update tenants --collection Movies --state cold --number_tenants 100 --json
weaviate-cli update tenants --collection Movies --state hot --tenants "Tenant-1,Tenant-2" --json
weaviate-cli delete tenants --collection Movies --number_tenants 100 --json
weaviate-cli delete tenants --collection Movies --tenants "Tenant-1,Tenant-2" --json
```

States: `hot`/`active`, `cold`/`inactive`, `frozen`/`offloaded`

See [references/tenants.md](references/tenants.md).

### Backups

```bash
weaviate-cli create backup --backend s3 --backup_id my-backup --wait --json
weaviate-cli create backup --backend s3 --backup_id my-backup --include "Movies,Books" --wait --json
weaviate-cli get backup --backend s3 --backup_id my-backup --json
weaviate-cli get backup --backend s3 --backup_id my-backup --restore --json
weaviate-cli restore backup --backend s3 --backup_id my-backup --wait --json
weaviate-cli cancel backup --backend s3 --backup_id my-backup --json
```

Backends: `s3`, `gcs`, `filesystem`. Options: `--include`, `--exclude`, `--wait`, `--cpu_for_backup N`

See [references/backups.md](references/backups.md).

### RBAC (Roles, Users, Permissions)

```bash
weaviate-cli create role --role_name MoviesAdmin -p crud_collections:Movies -p crud_data:Movies --json
weaviate-cli create user --user_name test-user --json
weaviate-cli create user --user_name test-user --store --json  # Store API key in config
weaviate-cli assign role --role_name MoviesAdmin --user_name test-user --json
weaviate-cli assign role --role_name MoviesAdmin --role_name TenantAdmin --user_name test-user --json  # Multiple roles
weaviate-cli assign permission -p read_data:Movies --role_name MoviesAdmin --json
weaviate-cli get role --all --json
weaviate-cli get role --role_name MoviesAdmin --json
weaviate-cli get role --user_name test-user --json
weaviate-cli get user --all --json
weaviate-cli get user --user_name test-user --json
weaviate-cli get user --role_name MoviesAdmin --json
weaviate-cli update user --user_name test-user --rotate_api_key --json
weaviate-cli update user --user_name test-user --activate --json
weaviate-cli update user --user_name test-user --deactivate --json
weaviate-cli revoke role --role_name MoviesAdmin --user_name test-user --json
weaviate-cli revoke role --role_name MoviesAdmin --user_name oidc-user --user_type oidc --json  # OIDC user
weaviate-cli revoke permission -p read_data:Movies --role_name MoviesAdmin --json
weaviate-cli delete role --role_name MoviesAdmin --json
weaviate-cli delete user --user_name test-user --json
```

Permission format: `action:target`. See [references/rbac.md](references/rbac.md) for full permission reference.

### Cluster & Nodes

```bash
weaviate-cli get nodes --json                          # Default view
weaviate-cli get nodes --minimal --json                # Minimal (fast for large clusters)
weaviate-cli get nodes --shards --json                 # Per-shard details
weaviate-cli get nodes --collections --json            # Per-collection details
weaviate-cli get nodes --collection Movies --json      # Specific collection
weaviate-cli get shards --json                         # All collections
weaviate-cli get shards --collection Movies --json     # Specific collection
weaviate-cli update shards --collection Movies --status READY --json
weaviate-cli update shards --all --status READY --json
weaviate-cli query sharding-state Movies --json
```

### Replication

```bash
weaviate-cli create replication --collection Movies --shard shard-1 --source-node node-1 --target-node node-2 --type COPY --json
weaviate-cli get replication <UUID> --json
weaviate-cli get replication <UUID> --history --json
weaviate-cli get all-replications --json
weaviate-cli query replications --collection Movies --json
weaviate-cli query replications --collection Movies --shard shard-1 --json
weaviate-cli query replications --target-node node-2 --json
weaviate-cli query replications --collection Movies --history --json  # Include history
weaviate-cli cancel replication <UUID> --json
weaviate-cli delete replication <UUID> --json
weaviate-cli delete all-replications --json
```

Types: `COPY` (duplicate shard), `MOVE` (migrate shard)

See [references/cluster.md](references/cluster.md).

### Aliases

```bash
weaviate-cli create alias MyAlias Movies --json
weaviate-cli get alias --alias_name MyAlias --json
weaviate-cli get alias --collection Movies --json
weaviate-cli get alias --all --json
weaviate-cli update alias MyAlias Movies_v2 --json
weaviate-cli delete alias MyAlias --json
```

### Benchmark

```bash
weaviate-cli benchmark qps --collection Movies --query-type hybrid --max-duration 300 --json
weaviate-cli benchmark qps --collection Movies --qps 100 --json              # Fixed QPS
weaviate-cli benchmark qps --collection Movies --tenant "Tenant-0" --json    # Multi-tenant
weaviate-cli benchmark qps --collection Movies --output csv --generate-graph --file-alias staging --json
```

Key options: `--query-type` (hybrid/bm25/near_text), `--qps N`, `--max-duration`, `--test-duration`, `--warmup-duration`, `--limit`, `--consistency-level`, `--latency-threshold`, `--concurrency`, `--query-terms`

See [references/benchmark.md](references/benchmark.md).

## Workflow Dependencies

### Collection Lifecycle
1. `create collection` -- must exist before any operations on it
2. `create tenants` -- only if collection has `--multitenant` (or use `--auto_tenant_creation`)
3. `create data` -- collection must exist; for MT collections, tenants must exist or auto-creation enabled
4. `query data` -- collection must have data
5. `delete data` -> `delete tenants` -> `delete collection` (reverse order)

### Multi-Tenancy State Machine
```
hot/active  <-->  cold/inactive
    ^   \              ^
    |    \             |
    v     v            v
      frozen/offloaded
```
- `hot`/`active`: data in memory, queryable, accepts writes
- `cold`/`inactive`: data on disk, not queryable
- `frozen`/`offloaded`: data in cloud storage, not queryable
- Tenants can be offloaded directly from `hot`/`active` or from `cold`/`inactive`
- Data operations require tenants in `hot`/`active` state

### RBAC Workflow
1. `create role --role_name X -p <permission>` -- create role with permissions
2. `create user --user_name Y` -- create user (returns API key)
3. `assign role --role_name X --user_name Y` -- assign role to user
4. Verify: `get role --role_name X` and `get user --user_name Y`
5. Cleanup: `revoke role` -> `delete role` / `delete user`

### Backup Workflow
1. `create backup --backend s3 --backup_id my-backup --wait` -- wait for completion
2. `get backup --backend s3 --backup_id my-backup` -- check status
3. `restore backup --backend s3 --backup_id my-backup --wait` -- restore
4. `cancel backup` -- cancel in-progress backup

### Replication Workflow
1. `get nodes` -- identify source and target nodes
2. `get shards --collection X` -- identify shard to replicate
3. `create replication --collection X --shard S --source-node A --target-node B --type COPY`
4. `get replication <UUID>` -- monitor progress
5. `cancel replication <UUID>` -- cancel if needed
6. `delete replication <UUID>` -- cleanup completed operation

### Alias Workflow
1. `create collection --collection Movies_v1` -- create the target collection
2. `create alias Movies Movies_v1` -- create alias pointing to collection
3. `update alias Movies Movies_v2` -- repoint alias to new collection
4. `get alias --alias_name Movies` -- inspect alias
5. `delete alias Movies` -- remove alias

## JSON Output

All commands support `--json` for machine-readable output. Always use `--json` when invoking commands programmatically.

**Success responses** return structured data or:
```json
{"status": "success", "message": "..."}
```

**Error responses** are printed to stderr and the process exits with code 1:
```
Error: <description>
```

## Error Handling

| Error | Cause | Solution |
|-------|-------|----------|
| `Config file does not exist` | Missing config | Create config or use `--config-file` |
| `User must be specified when auth type is 'user'` | Missing `--user` flag | Add `--user <name>` |
| `User 'X' not found in config file` | User not in config auth section | Add user to config or check spelling |
| `GRPC connection seems to be unavailable` | gRPC port blocked | Check firewall/network; CLI retries skipping checks |
| `Error: Collection 'X' does not exist` | Collection not created yet | Run `create collection` first |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SLOW_CONNECTION` | Set to `1`/`true` to double all client timeouts |
| `HOME` | Used to locate default config at `~/.config/weaviate/config.json` |

## Maintaining This Skill

When new commands or options are added to `weaviate-cli`:
1. Update the **Command Reference** section above with the new command syntax
2. Update or create the relevant reference file in `references/`
3. If a new command group is added, update the **Command Groups** table
4. Ensure all examples include `--json`
5. Update workflow dependency sections if the new command introduces dependencies

## References

- [references/collections.md](references/collections.md) -- Full collection options and notes
- [references/data.md](references/data.md) -- Data ingestion options and notes
- [references/search.md](references/search.md) -- Search types, options, and selection guide
- [references/tenants.md](references/tenants.md) -- Tenant state machine and management
- [references/backups.md](references/backups.md) -- Backup/restore options and notes
- [references/rbac.md](references/rbac.md) -- Permission format, actions, and examples
- [references/cluster.md](references/cluster.md) -- Nodes, shards, replication operations
- [references/benchmark.md](references/benchmark.md) -- Benchmark options and output modes
- [references/config-management.md](references/config-management.md) -- Config patterns and decision tree
