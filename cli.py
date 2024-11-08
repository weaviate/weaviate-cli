import click
import sys
from weaviate_cli.managers.config_manager import ConfigManager
from weaviate_cli.commands.create import create
from weaviate_cli.commands.delete import delete
from weaviate_cli.commands.get import get
from weaviate_cli.commands.update import update
from weaviate_cli.commands.query import query
from weaviate_cli.commands.restore import restore
from weaviate_cli.commands.cancel import cancel
from weaviate_cli import __version__


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"Weaviate CLI version {__version__}")
    ctx.exit()


@click.group()
@click.option(
    "--config-file",
    required=False,
    type=str,
    is_flag=False,
    help="If specified cli uses the config specified with this path.",
)
@click.option(
    "--version",
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Prints the version of the CLI.",
)
@click.pass_context
def main(ctx: click.Context, config_file):
    """Weaviate CLI tool"""
    try:
        ctx.obj = {"config": ConfigManager(config_file)}
    except Exception as e:
        click.echo(f"Fatal Error: {e}")
        sys.exit(1)


main.add_command(create)
main.add_command(delete)
main.add_command(get)
main.add_command(update)
main.add_command(restore)
main.add_command(query)
main.add_command(cancel)

if __name__ == "__main__":
    main()
