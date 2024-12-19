import sys
import click

from weaviate_cli.completion.complete import collection_name_complete
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate_cli.managers.role_manager import RoleManager
from weaviate_cli.defaults import (
    DeleteCollectionDefaults,
    DeleteTenantsDefaults,
    DeleteDataDefaults,
    DeleteRoleDefaults,
)


# Delete Group
@click.group()
def delete() -> None:
    """Delete resources in Weaviate."""
    pass


@delete.command("collection")
@click.option(
    "--collection",
    default=DeleteCollectionDefaults.collection,
    help="The name of the collection to delete.",
    shell_complete=collection_name_complete,
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
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@delete.command("tenants")
@click.option(
    "--collection",
    default=DeleteTenantsDefaults.collection,
    help="The name of the collection to delete tenants from.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--tenant_suffix",
    default=DeleteTenantsDefaults.tenant_suffix,
    help="The suffix to add to the tenant name (default: 'Tenant--').",
)
@click.option(
    "--number_tenants",
    default=DeleteTenantsDefaults.number_tenants,
    help="Number of tenants to delete (default: 100).",
)
@click.pass_context
def delete_tenants_cli(
    ctx: click.Context, collection: str, tenant_suffix: str, number_tenants: int
) -> None:
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
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@delete.command("data")
@click.option(
    "--collection",
    default=DeleteDataDefaults.collection,
    help="The name of the collection to delete tenants from.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--limit",
    default=DeleteDataDefaults.limit,
    help="Number of objects to delete (default: 100).",
)
@click.option(
    "--consistency_level",
    default=DeleteDataDefaults.consistency_level,
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.option(
    "--uuid",
    default=DeleteDataDefaults.uuid,
    help="UUID of the oject to be deleted. If provided, --limit will be ignored.",
)
@click.pass_context
def delete_data_cli(ctx, collection, limit, consistency_level, uuid):
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
            uuid=uuid,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@delete.command("role")
@click.option(
    "--role_name",
    default=DeleteRoleDefaults.role_name,
    help="The name of the role to delete.",
)
@click.pass_context
def delete_role_cli(ctx, role_name):
    """Delete a role in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        role_manager = RoleManager(client)
        role_manager.delete_role(role_name=role_name)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
