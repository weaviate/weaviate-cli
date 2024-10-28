import click
from lib.managers.config_manager import ConfigManager
from lib.commands.create import create
from lib.commands.delete import delete
from lib.commands.get import get
from lib.commands.update import update
from lib.commands.query import query
from lib.commands.restore import restore


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
