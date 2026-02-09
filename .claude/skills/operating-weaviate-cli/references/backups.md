# Backups Reference

Create, inspect, restore, and cancel Weaviate backups.

## Create Backup
```bash
weaviate-cli create backup --backend s3 --backup_id my-backup --wait --json
weaviate-cli create backup --backend s3 --backup_id my-backup --include "Movies,Books" --wait --json
weaviate-cli create backup --backend gcs --backup_id my-backup --exclude "TempData" --cpu_for_backup 60 --json
```

## Check Backup Status
```bash
weaviate-cli get backup --backend s3 --backup_id my-backup --json
```

## Check Restore Status
```bash
weaviate-cli get backup --backend s3 --backup_id my-backup --restore --json
```

## Restore Backup
```bash
weaviate-cli restore backup --backend s3 --backup_id my-backup --wait --json
weaviate-cli restore backup --backend s3 --backup_id my-backup --include "Movies" --wait --json
```

## Cancel Backup
```bash
weaviate-cli cancel backup --backend s3 --backup_id my-backup --json
```

## Options

**Create:**
- `--backend` -- s3, gcs, filesystem (default: s3)
- `--backup_id` -- Backup identifier (default: "test-backup")
- `--include` -- Comma-separated collections to include
- `--exclude` -- Comma-separated collections to exclude
- `--wait` -- Wait for completion
- `--cpu_for_backup` -- CPU percentage for backup (default: 40)

**Restore:**
- `--backend`, `--backup_id` -- Same as create
- `--include`, `--exclude` -- Filter which collections to restore
- `--wait` -- Wait for completion

## Prerequisites

1. Backup backend must be configured on the Weaviate cluster
2. Collections in `--include`/`--exclude` must exist
3. For restore: backup must exist and target collections must not already exist (unless overwriting)

## Notes

- `--wait` blocks until the operation completes -- recommended for scripted workflows
- Without `--wait`, the command returns immediately and you must poll with `get backup`
- `--cpu_for_backup` controls backup speed vs. resource consumption tradeoff
- `--include` and `--exclude` are mutually exclusive
