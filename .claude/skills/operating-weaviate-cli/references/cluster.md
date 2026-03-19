# Cluster Operations Reference

Inspect cluster health, node status, shard state, and manage replication operations.

## Node Information
```bash
weaviate-cli get nodes --json                          # Default view
weaviate-cli get nodes --minimal --json                # Minimal (fast, good for large clusters)
weaviate-cli get nodes --shards --json                 # Per-shard breakdown
weaviate-cli get nodes --collections --json            # Per-collection breakdown
weaviate-cli get nodes --collection Movies --json      # Specific collection
```
Only one of `--minimal`, `--shards`, `--collections`, `--collection` can be used at a time.

## Shard Information
```bash
weaviate-cli get shards --json                         # All collections
weaviate-cli get shards --collection Movies --json     # Specific collection
```

## Update Shard Status
```bash
weaviate-cli update shards --collection Movies --status READY --json
weaviate-cli update shards --collection Movies --status READONLY --shards "shard1,shard2" --json
weaviate-cli update shards --all --status READY --json
```
Statuses: `READY`, `READONLY`

## Sharding State
```bash
weaviate-cli query sharding-state Movies --json
weaviate-cli query sharding-state Movies --shard shard-1 --json
```
Shows which nodes hold replicas of each shard.

## Replication Operations

**Create (COPY or MOVE a shard):**
```bash
weaviate-cli create replication \
  --collection Movies \
  --shard shard-1 \
  --source-node node-1 \
  --target-node node-2 \
  --type COPY \
  --json
```
Types: `COPY` (duplicate shard to target), `MOVE` (migrate shard to target, remove from source)

**Inspect:**
```bash
weaviate-cli get replication <UUID> --json
weaviate-cli get replication <UUID> --history --json
weaviate-cli get all-replications --json
```

**Query replications with filters:**
```bash
weaviate-cli query replications --json                                    # All
weaviate-cli query replications --collection Movies --json                # By collection
weaviate-cli query replications --collection Movies --shard shard-1 --json  # By shard
weaviate-cli query replications --target-node node-2 --json               # By target node
weaviate-cli query replications --history --json                          # Include history
```

**Cancel/Delete:**
```bash
weaviate-cli cancel replication <UUID> --json
weaviate-cli delete replication <UUID> --json
weaviate-cli delete all-replications --json
```

## Replication Workflow

1. Identify nodes: `get nodes --minimal --json`
2. Identify shards: `get shards --collection X --json` or `query sharding-state X --json`
3. Start replication: `create replication --collection X --shard S --source-node A --target-node B --type COPY --json`
4. Monitor: `get replication <UUID> --json`
5. Cleanup: `delete replication <UUID> --json`

## Prerequisites

1. For replication: source node must have the shard, target node must not (for COPY)
2. `--shard` requires `--collection` when querying replications
3. `--target-node` cannot be combined with `--collection` or `--shard`

## Notes

- Use `--minimal` for large clusters to avoid slow node enumeration
- `delete all-replications` removes all replication operations -- use with caution
- Replication operations are asynchronous; use `get replication` to monitor progress
