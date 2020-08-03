#!/usr/bin/env python3
##                          _       _
##__      _____  __ ___   ___  __ _| |_ ___
##\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
## \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
##  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
##
## Copyright © 2016 - 2020 Weaviate. All rights reserved.
## LICENSE: https://github.com/semi-technologies/weaviate/blob/master/LICENSE
## AUTHOR: Bob van Luijt (bob@semi.technology)
##

import click


@click.group()
@click.pass_context
def main(ctx):
    pass

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
def main_ping():
    click.echo("TODO impl")


# schema
@schema_group.command("import")
def schema_import():
    click.echo("TODO impl")

@schema_group.command("export")
def schema_export():
    click.echo("TODO impl")

@schema_group.command("truncate")
def schema_truncate():
    click.echo("TODO impl")


# config
@config_group.command("view")
def config_view():
    click.echo("TODO impl")

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


if __name__ == "__main__":
    print("main")
    main()