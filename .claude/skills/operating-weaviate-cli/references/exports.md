# Collection Export Reference

Export collections from Weaviate to external storage backends in Parquet format.

## Create Export
```bash
weaviate-cli create export-collection --export_id my-export --backend s3 --file_format parquet --wait --json
weaviate-cli create export-collection --export_id my-export --backend s3 --include "Movies,Books" --json
weaviate-cli create export-collection --export_id my-export --backend gcs --exclude "TempData" --json
weaviate-cli create export-collection --export_id my-export --backend s3 --bucket my-bucket --path /exports --wait --json
```

## Check Export Status
```bash
weaviate-cli get export-collection --export_id my-export --backend s3 --json
```

Returns shard-level progress including objects exported per shard, errors, and timing.

## Cancel Export
```bash
weaviate-cli cancel export-collection --export_id my-export --backend s3 --json
```

Only works while the export is in progress. Returns an error if the export has already completed.

## Options

**Create:**
- `--export_id` -- Export identifier (default: "test-export")
- `--backend` -- filesystem, s3, gcs, azure (default: filesystem)
- `--file_format` -- Export format: parquet (default: parquet)
- `--include` -- Comma-separated collections to include
- `--exclude` -- Comma-separated collections to exclude
- `--wait` -- Wait for completion
- `--bucket` -- Bucket name for cloud storage backends
- `--path` -- Path within the storage backend

**Get Status:**
- `--export_id`, `--backend` -- Same as create
- `--bucket`, `--path` -- Optional, for locating the export

**Cancel:**
- `--export_id`, `--backend` -- Same as create
- `--bucket`, `--path` -- Optional, for locating the export

## Prerequisites

1. The export backend must be configured on the Weaviate cluster
2. For local-k8s, deploy with `ENABLE_BACKUP=true` to enable S3 via MinIO
3. `--include` and `--exclude` are mutually exclusive

## Notes

- `--wait` blocks until the export completes (SUCCESS, FAILED, or CANCELED)
- Without `--wait`, the command returns immediately with status STARTED
- Poll progress with `get export-collection` to monitor shard-level status
- Export uses the same storage backends as backups (S3, GCS, Azure, filesystem)
- The `--bucket` defaults to the cluster's configured backup bucket if not specified
