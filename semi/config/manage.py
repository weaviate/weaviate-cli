from semi.prompt import is_question_answer_yes, let_user_pick
from semi.config.configuration import *


def create_new_config():
    """

    :return: config as prompted from the user
    :rtype: json
    """

    config = {
        "url": input("Please give a weaviate url: "),
        "auth": _get_authentication_config()
    }

    print("Config creation complete\n\n")

    return config


def _get_authentication_config():
    auth_options = ["No authentication", "Client secret", "Username and password"]
    selection_index = let_user_pick(auth_options)
    if selection_index == 1:
        return {
            "type": config_value_auth_type_client_secret,
            "secret": input("Please specify the client secret: ")
        }
    if selection_index == 2:
        return {
            "type": config_value_auth_type_username_pass,
            "user": input("Please specify the user name: "),
            "pass": input("Please specify the user password: ")
        }
    return None
