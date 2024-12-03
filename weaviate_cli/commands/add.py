import click
import sys
from weaviate_cli.managers.user_manager import UserManager
from weaviate_cli.managers.role_manager import RoleManager
from weaviate_cli.utils import get_client_from_context


@click.group()
def add() -> None:
    """Add resources to other resources in Weaviate."""
    pass


@add.command("role")
@click.option(
    "--role",
    multiple=True,
    required=True,
    help="The name of the role to add. Can be specified multiple times. Example: --role MoviesAdmin --role TenantAdmin",
)
@click.option(
    "--to-user",
    required=True,
    help="The user to add the role to.",
)
@click.pass_context
def add_role_cli(ctx: click.Context, role: tuple[str], to_user: str) -> None:
    """Add a role to a user."""
    client = None
    try:
        client = get_client_from_context(ctx)
        user_manager = UserManager(client)
        user_manager.add_role(role=role, to_user=to_user)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@add.command("permission")
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
    "--to-role",
    required=True,
    help="The name of the role to add the permission to.",
)
@click.pass_context
def add_permission_cli(
    ctx: click.Context, permission: tuple[str], to_role: str
) -> None:
    """Add a permission to a role."""

    client = None
    try:
        client = get_client_from_context(ctx)
        role_manager = RoleManager(client)
        role_manager.add_permission(permission=permission, to_role=to_role)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
