import click
import sys
from weaviate_cli.managers.user_manager import UserManager
from weaviate_cli.managers.role_manager import RoleManager
from weaviate_cli.utils import get_client_from_context


@click.group()
def revoke() -> None:
    """Revoke resources from other resources in Weaviate."""
    pass


@revoke.command("role")
@click.option(
    "--role",
    multiple=True,
    required=True,
    help="The name of the role to revoke. Can be specified multiple times. Example: --role MoviesAdmin --role TenantAdmin",
)
@click.option(
    "--from-user",
    required=True,
    help="The user to revoke the role from.",
)
@click.pass_context
def revoke_role_cli(ctx: click.Context, role: tuple[str], from_user: str) -> None:
    """Revoke a role from a user."""
    client = None
    try:
        client = get_client_from_context(ctx)
        user_manager = UserManager(client)
        user_manager.revoke_role(role=role, from_user=from_user)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@revoke.command("permission")
@click.option(
    "-p",
    "--permission",
    multiple=True,
    required=True,
    help="""Permission in format action:collection. Can be specified multiple times.

    Allowed actions:
    - User management: manage_users
    - Role management: manage_roles, read_roles
    - Cluster statistics read: read_cluster
    - Backup management: manage_backups
    - Collections permissions: create_collections, read_collections, update_collections, delete_collections, manage_collections
    - Data permissions: create_data, read_data, update_data, delete_data
    - CRUD shorthands: crud_collections, crud_data
    - Nodes read: read_nodes

    Example: --permission crud_collections:* --permission read_data:Movies --permission manage_backups:Movies --permission read_cluster --permission read_nodes:verbose:Movies""",
)
@click.option(
    "--from-role",
    required=True,
    help="The role to revoke the permission from.",
)
@click.pass_context
def revoke_permission_cli(
    ctx: click.Context, permission: tuple[str], from_role: str
) -> None:
    """Revoke a permission from a role."""
    client = None
    try:
        client = get_client_from_context(ctx)
        role_manager = RoleManager(client)
        role_manager.revoke_permission(permission=permission, from_role=from_role)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
