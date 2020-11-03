from semi.config.configuration import Configuration
from semi.prompt import is_question_answer_yes


def delete_all_data(cfg:Configuration, force):
    if force:
        _delete_all(cfg.client)
        exit()
    if not is_question_answer_yes("Do you really want to delete all data?"):
        exit()
    _delete_all(cfg.client)


def _delete_all(client):
    schema = client.schema.get()
    client.schema.delete_all()
    client.schema.create(schema)