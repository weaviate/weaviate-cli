import sys
from typing import List, Optional
import click
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
)


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


@get.command("role")
@click.option(
    "--name",
    default=GetRoleDefaults.name,
    help="The name of the role to get.",
)
@click.option(
    "--for-user",
    default=GetRoleDefaults.for_user,
    help="The user to get the roles of.",
)
@click.option(
    "--all",
    is_flag=True,
    help="Get all roles in Weaviate.",
)
@click.pass_context
def get_role_cli(ctx, name, for_user, all):
    """Get a specific role or all roles in Weaviate. If --role is provided, get the specific role. If --for-user is provided, get the roles of the specific user."""

    client = None
    try:
        if all and name:
            raise Exception("Either --all or --name is required. Cannot provide both.")
        if all and for_user:
            raise Exception(
                "Either --all or --for-user is required. Cannot provide both."
            )
        if not all and not name and not for_user:
            raise Exception("Either --all or --name or --for-user is required.")
        if name and for_user:
            raise Exception(
                "Either --name or --for-user is required. Cannot provide both."
            )
        client = get_client_from_context(ctx)
        role_man = RoleManager(client)
        roles: List[Role] = []
        if all:
            roles = role_man.get_all_roles()
        else:
            if name:
                roles.append(role_man.get_role(name=name))
            elif for_user:
                roles = role_man.get_roles_from_user(for_user=for_user).values()
            else:
                raise Exception("Either --name or --for-user is required.")
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
# @click.option(
#     "--user",
#     default=GetUserDefaults.user_id,
#     help="The ID of the user to get.",
# )
@click.option(
    "--of-role",
    default=GetUserDefaults.of_role,
    help="The name of the role to get the users of.",
)
# @click.option(
#     "--all",
#     is_flag=True,
#     help="Get all users in Weaviate.",
# )
@click.pass_context
def get_user_cli(ctx, of_role: Optional[str]):
    """Get users of a specific role."""

    client = None
    try:
        if not of_role:
            raise Exception("Currently only --of-role is supported.")
        client = get_client_from_context(ctx)
        user_man = UserManager(client)
        users = user_man.get_user_from_role(of_role=of_role).values()
        print(f"Users of role '{of_role}':")
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
