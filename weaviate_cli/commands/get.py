import sys
from typing import List, Optional
import click

from weaviate_cli.completion.complete import (
    role_name_complete,
    collection_name_complete,
)
from weaviate_cli.managers.role_manager import RoleManager
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.managers.user_manager import UserManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.backup_manager import BackupManager
from weaviate_cli.managers.shard_manager import ShardManager
from weaviate.exceptions import WeaviateConnectionError
from weaviate.rbac.models import Role
from weaviate_cli.defaults import (
    GetBackupDefaults,
    GetTenantsDefaults,
    GetShardsDefaults,
    GetCollectionDefaults,
    GetRoleDefaults,
    GetUserDefaults,
    GetNodesDefaults,
)
from weaviate_cli.managers.node_manager import NodeManager


# Get Group
@click.group()
def get():
    """Get resources from Weaviate."""
    pass


@get.command("collection")
@click.option(
    "--collection",
    default=GetCollectionDefaults.collection,
    help="The name of the collection to get.",
    shell_complete=collection_name_complete,
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
    shell_complete=collection_name_complete,
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
    shell_complete=collection_name_complete,
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


@get.command("role")
@click.option(
    "--role_name",
    default=GetRoleDefaults.role_name,
    help="The name of the role to get.",
    shell_complete=role_name_complete,
)
@click.option(
    "--user_name",
    default=GetRoleDefaults.user_name,
    help="The user to get the roles of.",
)
@click.option(
    "--all",
    is_flag=True,
    help="Get all roles in Weaviate.",
)
@click.pass_context
def get_role_cli(ctx, role_name, user_name, all):
    """Get a specific role or all roles in Weaviate. If no arguments are provided, get the roles of the current user. If --role_name is provided, get the specific role. If --user_name is provided, get the roles of the specific user."""

    client = None
    try:
        if all and role_name:
            raise Exception(
                "Either --all or --role_name is required. Cannot provide both."
            )
        if all and user_name:
            raise Exception(
                "Either --all or --user_name is required. Cannot provide both."
            )
        if role_name and user_name:
            raise Exception(
                "Either --role_name or --user_name is required. Cannot provide both."
            )
        client = get_client_from_context(ctx)
        role_man = RoleManager(client)
        roles: List[Role] = []
        if all:
            roles = role_man.get_all_roles().values()
        else:
            if role_name:
                roles.append(role_man.get_role(role_name=role_name))
            elif user_name:
                roles = role_man.get_roles_from_user(user_name=user_name).values()
            else:
                roles = role_man.role_of_current_user().values()
        for role in roles:
            role_man.print_role(role)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@get.command("user")
@click.option(
    "--role_name",
    default=GetUserDefaults.role_name,
    help="The name of the role to get the users assigned to.",
)
@click.pass_context
def get_user_cli(ctx, role_name: Optional[str]):
    """Get users of a specific role."""

    client = None
    try:
        if not role_name:
            raise Exception("Currently only --role_name is supported.")
        client = get_client_from_context(ctx)
        user_man = UserManager(client)
        users = user_man.get_user_from_role(role_name=role_name).values()
        print(f"Users of role '{role_name}':")
        separator = "-" * 50
        print(f"\n{separator}")
        for user in users:
            user_man.print_user(user)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@get.command("nodes")
@click.option(
    "--minimal",
    is_flag=True,
    help="Get minimal node information. Use for large clusters, as verbose output will take a long time to load.",
)
@click.option(
    "--shards",
    is_flag=True,
    help="Get nodes information for each shard.",
)
@click.option(
    "--collections",
    is_flag=True,
    help="Get nodes information for each collection.",
)
@click.option(
    "--collection",
    default=GetNodesDefaults.collection,
    help="The name of the collection to get shards information from.",
    shell_complete=collection_name_complete,
)
@click.pass_context
def get_nodes_cli(ctx, minimal, shards, collections, collection):
    """Get the node information."""
    client = None
    try:
        if sum([bool(collection), shards, collections, minimal]) > 1:
            raise Exception(
                "Only one of --collection, --shards, --minimal, or --collections can be provided."
            )
        client = get_client_from_context(ctx)
        node_man = NodeManager(client)
        node_man.get_nodes(
            minimal=minimal,
            shards=shards,
            collections=collections,
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
