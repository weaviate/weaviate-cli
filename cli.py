import click
from semi.config.configuration import Configuration
from semi.commands.schema import import_schema, export_schema, truncate_schema
from semi.commands.ping import ping


@click.group()
@click.pass_context
def main(ctx):
    ctx.obj = {
        "config": Configuration()
    }

# First order commands
@main.group("schema")
def schema_group():
    pass

@main.group("config")
def config_group():
    pass

# TODO allow both concept and concepts
@main.group("concept")
def concept_group():
    pass

@main.group("cloud")
def cloud_group():
    pass


@main.command("ping")
@click.pass_context
def main_ping(ctx):
    ping(_get_config_from_context(ctx))

# schema
@schema_group.command("import")
@click.pass_context
@click.argument('filename')
def schema_import(ctx, filename):
    import_schema(_get_config_from_context(ctx), filename)

@schema_group.command("export")
@click.pass_context
@click.argument('filename')
def schema_export(ctx, filename):
    export_schema(_get_config_from_context(ctx), filename)

@schema_group.command("truncate")
@click.pass_context
def schema_truncate(ctx):
    truncate_schema(_get_config_from_context(ctx))


# config
@config_group.command("view")
@click.pass_context
def config_view(ctx):
    print(ctx.obj["config"])

@config_group.command("set")
def config_set():
    click.echo("TODO impl")

# concept
# TODO decide if it should be called concept or entity
@concept_group.command("import")
def concept_import():
    click.echo("TODO impl")

@concept_group.command("empty")
def concept_empty():
    click.echo("TODO impl")

@cloud_group.command("create")
def cloud_create():
    click.echo("TODO impl")

@cloud_group.command("delete")
def cloud_delete():
    click.echo("TODO impl")


def _get_config_from_context(ctx):
    """

    :param ctx:
    :return:
    :rtype: semi.config.configuration.Configuration
    """
    return ctx.obj["config"]

if __name__ == "__main__":
    main()