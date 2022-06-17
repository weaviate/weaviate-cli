import os.path
import json
import sys
from typing import Optional
import click
import weaviate
from semi.config.manage import create_new_config
import semi.config.config_values as cfg_vals

_cli_config_sub_path = ".config/semi_technologies/"
_cli_config_file_name = "configs.json"



class Configuration:

    def __init__(self, url: Optional[str] = None, user: Optional[str] = None, 
                 password: Optional[str] = None, client_secret: Optional[str] = None, 
                 user_specified_config_file: Optional[str] = None):
        """
        Initialize a Configuration class instance.

        Parameters
        ----------
        user_specified_config_file : Optional[str]
            Path to a config file, if None then one is created in $HOME folder.
        """

        home = os.getenv("HOME")
        self._config_folder = os.path.join(home, _cli_config_sub_path)

        if user_specified_config_file is None:
            self._config_path = os.path.join(self._config_folder, _cli_config_file_name) 
        else:
            self._config_path = user_specified_config_file

        if not os.path.isfile(self._config_path):
            try:
                os.makedirs(self._config_folder)
            except FileExistsError:
                pass  # Folders already exist

            if url is not None:
                cfg = { "url": url }
                if user is not None:
                    cfg["auth"] = {
                        "type": cfg_vals.config_value_auth_type_username_pass,
                        "user": user,
                        "pass": password
                    }
                elif client_secret is not None:
                    cfg["auth"] = {
                        "type": cfg_vals.config_value_auth_type_client_secret,
                        "secret": client_secret
                    }
                else:
                    cfg["auth"] = None
            else:
                click.echo("No config file found, creating a new one.")
                cfg = create_new_config()

            with open(self._config_path, 'w') as new_config_file:
                json.dump(cfg, new_config_file)

        with open(self._config_path, 'r') as user_specified_config_file:
            self.config = json.load(user_specified_config_file)

        self.client = _create_client_from_config(self.config)        

    def __str__(self):
        return str(json.dumps(self.config, indent=4))



def _create_client_from_config(config: dict) -> weaviate.Client:
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
