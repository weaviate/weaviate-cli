import click


@click.group()
@click.pass_context
def main(ctx):
    pass


@main.group("schema")
def schema_group():
    click.echo("schema_group group")

@main.group("config")
def config_group():
    click.echo("config group")


@schema_group.command("import")
def import_schema():
    click.echo("import (schema)")

@schema_group.command()
def export():
    click.echo("export (schema)")


@config_group.command()
def describe():
    click.echo("Describe config")


if __name__ == "__main__":
    print("main")
    main()