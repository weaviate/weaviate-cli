from semi.config.configuration import Configuration
from semi.prompt import is_question_answer_yes


def delete_all_data(cfg:Configuration):
    if not is_question_answer_yes("Do you really want to delete all data? "):
        exit()
    schema = cfg.client.schema.get()
    cfg.client.schema.delete_all()
    cfg.client.schema.create(schema)
