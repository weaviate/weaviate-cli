import json
import sys
from semi.config.configuration import Configuration
from semi.prompt import is_question_answer_yes


def import_schema(cfg: Configuration, file_name: str, force: bool) -> None:
    """
    Import schema into weaviate.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    file_name : str
        Path to a schema file or URL.
    force : bool
        If True replaces the the schema from weaviate (if one present),
        if False imports the schema only if no schema is present in weaviate.
    """

    if cfg.client.schema.contains(file_name):
        if not force:
            print("The schema or part of it is already present! Use --force to force replace it.")
            sys.exit(1)
        cfg.client.schema.delete_all()
    print("Importing file: ", file_name)
    cfg.client.schema.create(file_name)


def export_schema(cfg: Configuration, file_name: str) -> None:
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
    schema = cfg.client.schema.get()
    with open(file_name, 'w') as output_file:
        json.dump(schema, output_file, indent=4)


def delete_schema(cfg: Configuration, force: bool) -> None:
    """
    Delete the weaviate schema.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    force : bool
        If True deletes all objects and schema from weaviate,
        if False deletes schema only if weaviate does not have any objects or
        asks permision to delete all the objects and schema.
    """

    data = cfg.client.data_object.get()
    if len(data) == 0 or force:
        cfg.client.schema.delete_all()
        sys.exit(0)
    if not is_question_answer_yes("Weaviate contains data, deleting the schema will delete all data do you want to continue?"):
        sys.exit(1)
    cfg.client.schema.delete_all()
