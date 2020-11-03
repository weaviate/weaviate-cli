import json
from semi.config.configuration import Configuration
from semi.prompt import is_question_answer_yes


def import_schema(cfg:Configuration, file_name: str, force: bool):
    if cfg.client.schema.contains(file_name):
        if not force:
            print("The schema or part of it is already present! Use --force to force replace it.")
            exit(1)
        cfg.client.schema.delete_all()

    print("Importing file: ", file_name)
    cfg.client.schema.create(file_name)


def export_schema(cfg:Configuration, file_name:str):
    print("Exporting to file: ", file_name)
    schema = cfg.client.schema.get()
    with open(file_name, 'w') as output_file:
        json.dump(schema, output_file, indent=4)


def truncate_schema(cfg:Configuration, force:bool):
    data = cfg.client.data_object.get()
    if len(data) == 0 or force:
        cfg.client.schema.delete_all()
        exit(0)
    if not is_question_answer_yes("Weaviate contains data, truncating the schema will delete all data do you want to continue?"):
        exit(1)
    cfg.client.schema.delete_all()

