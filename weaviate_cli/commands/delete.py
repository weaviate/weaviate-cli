import sys
import click
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate.exceptions import WeaviateConnectionError
# Delete Group
@click.group()
def delete() -> None:
    """Delete resources in Weaviate."""
    pass

@delete.command("collection")
@click.option(
    "--collection", default="Movies", help="The name of the collection to delete."
)
@click.option("--all", is_flag=True, help="Delete all collections (default: False).")
@click.pass_context
def delete_collection_cli(ctx: click.Context, collection: str, all: bool) -> None:
    """Delete a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        collection_man = CollectionManager(client)
        # Call the function from delete_collection.py with general and specific arguments
        collection_man.delete_collection(collection=collection, all=all)
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)


@delete.command("tenants")
@click.option(
    "--collection",
    default="Movies",
    help="The name of the collection to delete tenants from.",
)
@click.option(
    "--tenant_suffix",
    default="Tenant--",
    help="The suffix to add to the tenant name (default: 'Tenant--').",
)
@click.option(
    "--number_tenants", default=100, help="Number of tenants to delete (default: 100)."
)
@click.pass_context
def delete_tenants_cli(ctx: click.Context, collection: str, tenant_suffix: str, number_tenants: int) -> None:
    """Delete tenants from a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        tenant_manager = TenantManager(client)
        tenant_manager.delete_tenants(
            collection=collection,
            tenant_suffix=tenant_suffix,
            number_tenants=number_tenants,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)

@delete.command("data")
@click.option(
    "--collection",
    default="Movies",
    help="The name of the collection to delete tenants from.",
)
@click.option(
    "--limit", default=100, help="Number of objects to delete (default: 100)."
)
@click.option(
    "--consistency_level",
    default="quorum",
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.pass_context
def delete_data_cli(ctx, collection, limit, consistency_level):
    """Delete data from a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        data_manager = DataManager(client)
        # Call the function from delete_data.py with general and specific arguments
        data_manager.delete_data(
            collection=collection,
            limit=limit,
            consistency_level=consistency_level,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)