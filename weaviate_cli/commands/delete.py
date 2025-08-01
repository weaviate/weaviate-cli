import sys
import click

from typing import Optional
from weaviate import WeaviateClient

from weaviate_cli.completion.complete import collection_name_complete
from weaviate_cli.managers.alias_manager import AliasManager
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate_cli.managers.cluster_manager import ClusterManager
from weaviate_cli.managers.role_manager import RoleManager
from weaviate_cli.managers.user_manager import UserManager
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
    help="The suffix to add to the tenant name (default: 'Tenant-'). If passing the asterisk as a suffix * it will delete as many tenants as specified in --number_tenants, it will not take into account the tenant suffix.",
)
@click.option(
    "--number_tenants",
    default=DeleteTenantsDefaults.number_tenants,
    help="Number of tenants to delete (default: 100).",
)
@click.option(
    "--tenants",
    default=DeleteTenantsDefaults.tenants,
    help="Comma separated list of tenants to delete. Example: --tenants 'Tenant-1,Tenant-2'",
)
@click.pass_context
def delete_tenants_cli(
    ctx: click.Context,
    collection: str,
    tenant_suffix: str,
    number_tenants: int,
    tenants: str,
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
            tenants_list=tenants.split(",") if tenants else None,
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
    "--tenants",
    default=None,
    help="Comma separated list of tenants to delete data from.",
)
@click.option(
    "--uuid",
    default=DeleteDataDefaults.uuid,
    help="UUID of the oject to be deleted. If provided, --limit will be ignored.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=DeleteDataDefaults.verbose,
    help="Show detailed progress information (default: False).",
)
@click.pass_context
def delete_data_cli(ctx, collection, limit, consistency_level, tenants, uuid, verbose):
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
            tenants_list=tenants.split(",") if tenants else None,
            uuid=uuid,
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
        click.echo(f"Role '{role_name}' deleted successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@delete.command("user")
@click.option(
    "--user_name",
    default=None,
    help="The name of the user to delete.",
)
@click.pass_context
def delete_user_cli(ctx, user_name):
    """Delete a user in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        user_manager = UserManager(client)
        user_manager.delete_user(user_name=user_name)
        click.echo(f"User '{user_name}' deleted successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@delete.command("alias")
@click.argument("alias_name")
@click.pass_context
def delete_alias_cli(ctx: click.Context, alias_name: str) -> None:
    """Delete an alias for a collection in Weaviate."""
    client = None
    try:
        client = get_client_from_context(ctx)
        alias_man = AliasManager(client)
        alias_man.delete_alias(alias_name=alias_name)
        click.echo(f"Alias '{alias_name}' deleted successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@delete.command("replication", help="Delete a replication operation in Weaviate.")
@click.argument("op_id")
@click.pass_context
def delete_replication_cli(ctx: click.Context, op_id: str) -> None:
    """Delete a replication operation in Weaviate."""
    client: Optional[WeaviateClient] = None
    try:
        client = get_client_from_context(ctx)
        manager = ClusterManager(client)
        manager.delete_replication(op_id=op_id)
        click.echo(
            f"Replication scheduled for deletion successfully with UUID: {op_id}"
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@delete.command(
    "all-replications", help="Delete all replication operations in Weaviate."
)
@click.pass_context
def delete_all_replications_cli(ctx: click.Context) -> None:
    """Delete all replication operations in Weaviate."""
    client: Optional[WeaviateClient] = None
    try:
        client = get_client_from_context(ctx)
        manager = ClusterManager(client)
        manager.delete_all_replication()
        click.echo("All replications scheduled for deletion successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
