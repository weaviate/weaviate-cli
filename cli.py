import click
from weaviate_cli.managers.config_manager import ConfigManager
from weaviate_cli.commands.create import create
from weaviate_cli.commands.delete import delete
from weaviate_cli.commands.get import get
from weaviate_cli.commands.update import update
from weaviate_cli.commands.query import query
from weaviate_cli.commands.restore import restore


@click.group()
@click.pass_context
@click.option('--config-file', required=False, default=None, type=str, is_flag=False,
              help="If specified cli uses the config specified with this path.")
def main(ctx: click.Context, config_file):
    ctx.obj = {
        "config": ConfigManager(config_file)
    }


main.add_command(create)
main.add_command(delete)
main.add_command(get)
main.add_command(update)
if __name__ == "__main__":
    main()
