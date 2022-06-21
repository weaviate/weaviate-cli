import click
from semi.config.commands import config_group
from semi.cloud.commands import cloud_group
from semi.data.commands import data_group
from semi.schema.commands import schema_group
from semi.misc import main_init, main_ping, main_version


@click.group()
@click.pass_context
@click.option('--config-file', required=False, default=None, type=str, is_flag=False,
              help="If specified cli uses the config specified with this path.")
def main(ctx: click.Context, config_file):
    ctx.obj = {
        "config-file": config_file
    }


main.add_command(config_group)
main.add_command(cloud_group)
main.add_command(data_group)
main.add_command(schema_group)
main.add_command(main_ping)
main.add_command(main_version)
main.add_command(main_init)


if __name__ == "__main__":
    main()
