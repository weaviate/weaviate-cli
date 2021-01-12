from getpass import getpass
from typing import Optional
from semi.prompt import let_user_pick
import semi.config.config_values as cfg_vals


def create_new_config() -> dict:
    """
    Create a new weaviate authentication configuration.

    Returns
    -------
    dict
        The config as prompted from the user
    """

    config = {
        "url": input("Please give a weaviate url: "),
        "auth": _get_authentication_config()
    }

    print("Config creation complete\n\n")

    return config


def _get_authentication_config() -> Optional[dict]:
    """
    Get Authentication config from user input.

    Returns
    -------
    dict or None
        The authentication configuration.
    """

    auth_options = ["No authentication", "Client secret", "Username and password"]
    selection_index = let_user_pick(auth_options)
    if selection_index == 1:
        return {
            "type": cfg_vals.config_value_auth_type_client_secret,
            "secret": getpass("Please specify the client secret: ") # Hide Secret
        }
    if selection_index == 2:
        return {
            "type": cfg_vals.config_value_auth_type_username_pass,
            "user": input("Please specify the user name: "),
            "pass": getpass("Please specify the user password: ") # Hide Password
        }
    return None
