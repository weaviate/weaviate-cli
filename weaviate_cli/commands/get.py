import sys
import click
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.backup_manager import BackupManager
from weaviate_cli.managers.shard_manager import ShardManager
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.defaults import GetBackupDefaults
from weaviate_cli.defaults import GetTenantsDefaults
from weaviate_cli.defaults import GetShardsDefaults
from weaviate_cli.defaults import GetCollectionDefaults


# Get Group
@click.group()
def get():
    """Create resources in Weaviate."""
    pass


@get.command("collection")
@click.option(
    "--collection",
    default=GetCollectionDefaults.collection,
    help="The name of the collection to get.",
)
@click.pass_context
def get_collection_cli(ctx, collection):
    """Get all collections in Weaviate. If --collection is provided, get the specific collection."""

    client = None
    try:
        client = get_client_from_context(ctx)
        collection_man = CollectionManager(client)
        # Call the function from get_collections.py with general arguments
        collection_man.get_collection(collection=collection)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@get.command("tenants")
@click.option(
    "--collection",
    default=GetTenantsDefaults.collection,
    help="The name of the collection to get tenants from.",
)
@click.option(
    "--tenant_id",
    help="The tenant ID to get.",
)
@click.option("--verbose", is_flag=True, help="Print verbose output.")
@click.pass_context
def get_tenants_cli(ctx, collection, tenant_id, verbose):
    """Get tenants from a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        tenant_manager = TenantManager(client)
        tenant_manager.get_tenants(
            collection=collection,
            tenant_id=tenant_id,
            verbose=verbose,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@get.command("shards")
@click.option(
    "--collection",
    default=GetShardsDefaults.collection,
    help="The name of the collection to get tenants from.",
)
@click.pass_context
def get_shards_cli(ctx, collection):
    """Get shards from a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        client.collections.list_all()
        shard_man = ShardManager(client)
        shard_man.get_shards(
            collection=collection,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@get.command("backup")
@click.option(
    "--backend",
    default=GetBackupDefaults.backend,
    type=click.Choice(["s3", "gcs", "filesystem"]),
    help="The backend used for storing the backups (default: s3).",
)
@click.option(
    "--backup_id",
    default=GetBackupDefaults.backup_id,
    help="Identifier for the backup you want to get its status.",
)
@click.option(
    "--restore",
    is_flag=True,
    help="Get the status of the restoration job for the backup.",
)
@click.pass_context
def get_backup_cli(ctx, backend, backup_id, restore):
    """Get backup status for a given backup ID. If --restore is provided, get the status of the restoration job for the backup."""

    client = None
    try:
        client = get_client_from_context(ctx)
        backup_man = BackupManager(client)
        backup_man.get_backup(backend=backend, backup_id=backup_id, restore=restore)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
