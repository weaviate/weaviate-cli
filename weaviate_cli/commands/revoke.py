import click
import sys

from weaviate_cli.completion.complete import role_name_complete
from weaviate_cli.managers.user_manager import UserManager
from weaviate_cli.managers.role_manager import RoleManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.defaults import PERMISSION_HELP_STRING


@click.group()
def revoke() -> None:
    """Revoke resources from other resources in Weaviate."""
    pass


@revoke.command("role")
@click.option(
    "--role_name",
    multiple=True,
    required=True,
    help="The name of the role to revoke. Can be specified multiple times. Example: --role MoviesAdmin --role TenantAdmin",
    shell_complete=role_name_complete,
)
@click.option(
    "--user_name",
    required=True,
    help="The user to revoke the role from.",
)
@click.pass_context
def revoke_role_cli(ctx: click.Context, role_name: tuple[str], user_name: str) -> None:
    """Revoke a role from a user."""
    client = None
    try:
        client = get_client_from_context(ctx)
        user_manager = UserManager(client)
        user_manager.revoke_role(role_name=role_name, user_name=user_name)
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
    help=PERMISSION_HELP_STRING,
)
@click.option(
    "--role_name",
    required=True,
    help="The role to revoke the permission from.",
)
@click.pass_context
def revoke_permission_cli(
    ctx: click.Context, permission: tuple[str], role_name: str
) -> None:
    """Revoke a permission from a role."""
    client = None
    try:
        client = get_client_from_context(ctx)
        role_manager = RoleManager(client)
        role_manager.revoke_permission(permission=permission, role_name=role_name)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
