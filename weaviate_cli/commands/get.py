import sys
import click
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.shard_manager import ShardManager
from weaviate.exceptions import WeaviateConnectionError


# Get Group
@click.group()
def get():
    """Create resources in Weaviate."""
    pass

@get.command("collection")
@click.option("--collection", default=None, help="The name of the collection to get.")
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
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)

@get.command("tenants")
@click.option(
    "--collection",
    default="Movies",
    help="The name of the collection to get tenants from.",
)
@click.option("--verbose", is_flag=True, help="Print verbose output.")
@click.pass_context
def get_tenants_cli(ctx, collection, verbose):
    """Get tenants from a collection in Weaviate."""
    
    client = None
    try:
        client = get_client_from_context(ctx)
        tenant_manager = TenantManager(client)
        tenant_manager.get_tenants(
            collection=collection,
            verbose=verbose,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)

@get.command("shards")
@click.option(
    "--collection",
    default=None,
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
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)