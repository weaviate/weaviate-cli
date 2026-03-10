import json
import click
import sys
from typing import Optional
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.backup_manager import BackupManager
from weaviate_cli.managers.cluster_manager import ClusterManager
from weaviate_cli.managers.export_manager import ExportManager
from weaviate_cli.defaults import CancelBackupDefaults, CancelExportCollectionDefaults


# Create Group
@click.group()
def cancel():
    """Cancel operations in Weaviate."""
    pass


@cancel.command("backup")
@click.option(
    "--backend",
    default=CancelBackupDefaults.backend,
    type=click.Choice(["s3", "gcs", "filesystem"]),
    help="The backend used for storing the backups (default: s3).",
)
@click.option(
    "--backup_id",
    default=CancelBackupDefaults.backup_id,
    help="Identifier used for the backup (default: test-backup).",
)
@click.option(
    "--json", "json_output", is_flag=True, default=False, help="Output in JSON format."
)
@click.pass_context
def cancel_backup_cli(
    ctx: click.Context, backend: str, backup_id: str, json_output: bool
) -> None:
    """Cancel a backup in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        backup_manager = BackupManager(client)
        backup_manager.cancel_backup(
            backend=backend, backup_id=backup_id, json_output=json_output
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@cancel.command("replication", help="Cancel a replication operation in Weaviate.")
@click.argument("op_id")
@click.option(
    "--json", "json_output", is_flag=True, default=False, help="Output in JSON format."
)
@click.pass_context
def cancel_replication_cli(ctx: click.Context, op_id: str, json_output: bool) -> None:
    """Cancel a replication operation in Weaviate by OP_ID."""
    client = None
    try:
        client = get_client_from_context(ctx)
        manager = ClusterManager(client)
        manager.cancel_replication(op_id=op_id)
        if json_output:
            click.echo(
                json.dumps(
                    {
                        "status": "success",
                        "message": f"Replication with UUID '{op_id}' cancelled successfully.",
                    },
                    indent=2,
                )
            )
        else:
            click.echo(f"Replication with UUID '{op_id}' cancelled successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@cancel.command("export-collection")
@click.option(
    "--export_id",
    default=CancelExportCollectionDefaults.export_id,
    help=f"Identifier for the export (default: {CancelExportCollectionDefaults.export_id}).",
)
@click.option(
    "--backend",
    default=CancelExportCollectionDefaults.backend,
    type=click.Choice(["filesystem", "s3", "gcs", "azure"]),
    help=f"The backend used for storing the export (default: {CancelExportCollectionDefaults.backend}).",
)
@click.option(
    "--bucket",
    default=CancelExportCollectionDefaults.bucket,
    help="Bucket name for cloud storage backends.",
)
@click.option(
    "--path",
    default=CancelExportCollectionDefaults.path,
    help="Path within the storage backend.",
)
@click.option(
    "--json", "json_output", is_flag=True, default=False, help="Output in JSON format."
)
@click.pass_context
def cancel_export_collection_cli(
    ctx: click.Context,
    export_id: str,
    backend: str,
    bucket: Optional[str],
    path: Optional[str],
    json_output: bool,
) -> None:
    """Cancel a collection export in Weaviate."""
    client = None
    try:
        client = get_client_from_context(ctx)
        export_manager = ExportManager(client)
        export_manager.cancel_export(
            export_id=export_id,
            backend=backend,
            bucket=bucket,
            path=path,
            json_output=json_output,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
