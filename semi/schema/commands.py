"""
Weaviate CLI schema group functions.
"""

import sys
import json
import click

from semi.prompt import is_question_answer_yes
from semi.utils import get_client_from_context
from weaviate import Client


@click.group("schema", help="Importing and exporting schema files.")
def schema_group():
    pass


@schema_group.command("import")
@click.pass_context
@click.argument('filename')
@click.option('--force', required=False, default=False, is_flag=True)
def schema_import(ctx, filename, force):
    """
        Import a weaviate schema from a file.
    """
    import_schema(get_client_from_context(ctx), filename, force)


@schema_group.command("export")
@click.pass_context
@click.argument('filename')
def schema_export(ctx, filename):
    """
        Export Weaviate schema to a file.
    """
    export_schema(get_client_from_context(ctx), filename)


@schema_group.command("delete")
@click.pass_context
@click.option('--force', required=False, default=False, is_flag=True)
def schema_truncate(ctx: click.Context, force):
    """
        Delete entire schema and data associated with it.
    """
    delete_schema(get_client_from_context(ctx), force)



####################################################################################################
# Helper functions
####################################################################################################


def import_schema(client: Client, file_name: str, force: bool) -> None:
    """
    Import schema into weaviate.

    Parameters
    ----------
    cfg : Configuration
        The CLI configuration.
    file_name : str
        The path to a schema file or URL.
    force : bool
        If True replaces the the schema from weaviate (if one present), if False imports the schema
        only if no schema is present in weaviate.
    """

    if client.schema.contains(file_name):
        if not force:
            print("The schema or part of it is already present! Use --force to force replace it.")
            sys.exit(1)
        client.schema.delete_all()
    print("Importing file: ", file_name)
    client.schema.create(file_name)


def export_schema(client: Client, file_name: str) -> None:
    """
    Export schema from weaviate to a file.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    file_name : str
        A file name where to export the schema.
    """

    print("Exporting to file: ", file_name)
    schema = client.schema.get()
    with open(file_name, 'w', encoding="utf-8") as output_file:
        json.dump(schema, output_file, indent=4)


def delete_schema(client: Client, force: bool) -> None:
    """
    Delete the weaviate schema.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    force : bool
        If True deletes all objects and schema from weaviate, if False deletes schema only if
        weaviate does not have any objects or asks permission to delete all the objects and schema.
    """

    data = client.data_object.get()
    if len(data) == 0 or force:
        client.schema.delete_all()
        sys.exit(0)
    if not is_question_answer_yes("Weaviate contains data, deleting the schema will delete all data do you want to continue?"):
        sys.exit(1)
    client.schema.delete_all()
