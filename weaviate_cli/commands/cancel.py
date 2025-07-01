import click
import sys
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.backup_manager import BackupManager
from weaviate_cli.managers.cluster_manager import ClusterManager
from weaviate_cli.defaults import CancelBackupDefaults


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
@click.pass_context
def cancel_backup_cli(ctx: click.Context, backend: str, backup_id: str) -> None:
    """Cancel a backup in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        backup_manager = BackupManager(client)
        backup_manager.cancel_backup(backend=backend, backup_id=backup_id)
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
@click.pass_context
def cancel_replication_cli(ctx: click.Context, op_id: str) -> None:
    """Cancel a replication operation in Weaviate by OP_ID."""
    client = None
    try:
        client = get_client_from_context(ctx)
        manager = ClusterManager(client)
        manager.cancel_replication(op_id=op_id)
        click.echo(f"Replication with UUID '{op_id}' cancelled successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
