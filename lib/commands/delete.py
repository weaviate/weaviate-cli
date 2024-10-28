import sys
import click
from lib.managers.tenant_manager import TenantManager
from lib.utils import get_client_from_context
from lib.managers.collection_manager import CollectionManager

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

    try:
        client = get_client_from_context(ctx)
        collection_man = CollectionManager(client)
        # Call the function from delete_collection.py with general and specific arguments
        collection_man.delete_collection(collection=collection, all=all)
    except Exception as e:
        click.echo(f"Error: {e}")
        client.close()
        sys.exit(1)  # Return a non-zero exit code on failure

    client.close()


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

    try:
        client = get_client_from_context(ctx)
        tenant_manager = TenantManager(client)
        tenant_manager.delete_tenants(
            collection=collection,
            tenant_suffix=tenant_suffix,
            number_tenants=number_tenants,
        )
    except Exception as e:
        print(f"Error: {e}")
        client.close()
        sys.exit(1)  # Return a non-zero exit code on failure

    client.close()