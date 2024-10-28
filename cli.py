import click
from lib.managers.config_manager import ConfigManager
from lib.commands.create import create



@click.group()
@click.pass_context
@click.option('--config-file', required=False, default=None, type=str, is_flag=False,
              help="If specified cli uses the config specified with this path.")
def main(ctx: click.Context, config_file):
    ctx.obj = {
        "config": ConfigManager(config_file)
    }


main.add_command(create)



if __name__ == "__main__":
    main()
