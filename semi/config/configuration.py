import os.path
import json
import sys
from typing import Optional
import weaviate
from semi.config.manage import create_new_config
import semi.config.config_values as cfg_vals

_cli_config_sub_path = ".config/semi_technologies/"
_cli_config_file_name = "configs.json"



class Configuration:

    def __init__(self, user_specified_config_file: Optional[str]):
        """
        Initialize a Configuration class instance.

        Parameters
        ----------
        user_specified_config_file : Optional[str]
            Path to a config file, if None then one is created in $HOME folder.
        """

        home = os.getenv("HOME")
        self._config_folder = os.path.join(home, _cli_config_sub_path)
        self._config_path = os.path.join(self._config_folder, _cli_config_file_name)

        if user_specified_config_file is not None:
            self._config_path = user_specified_config_file

        if not os.path.isfile(self._config_path):
            print("No config was found, creating a new one.")
            self.init()

        with open(self._config_path, 'r') as user_specified_config_file:
            self.config = json.load(user_specified_config_file)

        self.client = _creat_client_from_config(self.config)

    def init(self):
        """
        Create the config folder and prompt user for an inital config.
        """

        try:
            os.makedirs(self._config_folder)
        except:
            pass  # Folders already exist

        cfg = create_new_config()
        with open(self._config_path, 'w') as new_config_file:
            json.dump(cfg, new_config_file)

    def __str__(self):
        return str(json.dumps(self.config, indent=4))



def _creat_client_from_config(config: dict) -> weaviate.Client:
    """
    Create weaviate client from a config.

    Parameters
    ----------
    config : dict
        The configuration as a dict.

    Returns
    -------
    weaviate.Client
        The configured weaviate client.
    """

    if config["auth"] is None:
        return weaviate.Client(config["url"])
    if config["auth"]["type"] == cfg_vals.config_value_auth_type_client_secret:
        cred = weaviate.AuthClientCredentials(config["auth"]["secret"])
        return weaviate.Client(config["url"], cred)
    if config["auth"]["type"] == cfg_vals.config_value_auth_type_username_pass:
        cred = weaviate.AuthClientPassword(config["auth"]["user"], config["auth"]["pass"])
        return weaviate.Client(config["url"], cred)

    print("Fatal error unknown authentication type in config!")
    sys.exit(1)
