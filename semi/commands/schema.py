import json
from semi.config.configuration import Configuration
from semi.prompt import is_question_answer_yes


def import_schema(cfg:Configuration, file_name:str):
    print("Importing file: ", file_name)
    cfg.client.schema.create(file_name)


def export_schema(cfg:Configuration, file_name:str):
    print("Exporting to file: ", file_name)
    schema = cfg.client.schema.get()
    with open(file_name, 'w') as output_file:
        json.dump(schema, output_file, indent=4)


def truncate_schema(cfg:Configuration):
    data = cfg.client.data_object.get()
    if len(data) > 0:
        if not is_question_answer_yes("Weaviate contains data, truncating the schema will delete all data do you want to continue? "):
            exit()
    cfg.client.schema.delete_all()