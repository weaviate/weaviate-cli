import sys
import click
from weaviate_cli.managers.backup_manager import BackupManager
from weaviate_cli.utils import get_client_from_context
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.defaults import RestoreBackupDefaults


# Restore Group
@click.group()
def restore() -> None:
    """Restore backups in Weaviate."""
    pass


@restore.command("backup")
@click.option(
    "--backend",
    default=RestoreBackupDefaults.backend,
    type=click.Choice(["s3", "gcs", "filesystem"]),
    help="The backend used for storing the backups (default: s3).",
)
@click.option(
    "--backup_id",
    default=RestoreBackupDefaults.backup_id,
    help="Identifier used for the backup (default: test-backup).",
)
@click.option(
    "--wait", is_flag=True, help="Wait for the backup to complete before returning."
)
@click.option(
    "--include",
    default=RestoreBackupDefaults.include,
    help="Collection to include in backup (default: None).",
)
@click.option(
    "--exclude",
    default=RestoreBackupDefaults.exclude,
    help="Collection to exclude in backup (default: None).",
)
@click.pass_context
def restore_backup_cli(ctx, backend, include, exclude, backup_id, wait):
    """Restore a backup in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        backup_manager = BackupManager(client)
        backup_manager.restore_backup(
            backend=backend,
            backup_id=backup_id,
            include=include,
            exclude=exclude,
            wait=wait,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
