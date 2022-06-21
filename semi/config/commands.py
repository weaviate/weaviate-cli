"""
Weaviate CLI config group functions.
"""

import os
import sys
import json
import click
import requests
import weaviate
from typing import Optional
from getpass import getpass

from semi.prompt import let_user_pick
import semi.config.config_values as cfg_vals


@click.group("config", help="Configuration of the CLI.")
def config_group():
    pass


@config_group.command("view", help="Print the current CLI configuration.")
@click.pass_context
def config_view(ctx):
    print(Configuration(ctx.obj['config-file']))


@config_group.command("set", help="Set a new CLI configuration.")
def config_set():
    Configuration().create_new_config()


########################################################################################################################
# Helper class
########################################################################################################################


_cli_config_sub_path = ".config/semi_technologies/"
_cli_config_file_name = "configs.json"


class Configuration:

    def __init__(self, config_file: Optional[str] = None):
        self._config_folder = os.path.join(os.getenv("HOME"), _cli_config_sub_path)
        if config_file is None:
            self._config_path = os.path.join(self._config_folder, _cli_config_file_name) 
        else:
            self._config_path = config_file
        if not os.path.isfile(self._config_path):
            try:
                os.makedirs(self._config_folder)
            except FileExistsError:
                pass  # Folders already exist
            self._config_path = os.path.join(self._config_folder, _cli_config_file_name)
            self.config = self.create_new_config()
        else:
            with open(self._config_path, 'r') as user_specified_config_file:
                self.config = json.load(user_specified_config_file)

        try:
            self.client = self.get_client()
        except requests.exceptions.ConnectionError:
            click.echo("Fatal error: Connection to the specified weaviate url failed!")
            sys.exit(1)


    def get_client(self) -> weaviate.Client:

        if self.config["auth"] is None:
            return weaviate.Client(self.config["url"])
        if self.config["auth"]["type"] == cfg_vals.config_value_auth_type_client_secret:
            cred = weaviate.AuthClientCredentials(self.config["auth"]["secret"])
            return weaviate.Client(self.config["url"], cred)
        if self.config["auth"]["type"] == cfg_vals.config_value_auth_type_username_pass:
            cred = weaviate.AuthClientPassword(self.config["auth"]["user"], self.config["auth"]["pass"])
            return weaviate.Client(self.config["url"], cred)

        print("Fatal error unknown authentication type in config!")
        sys.exit(1)


    def create_new_config(self) -> dict:

        config = {
            "url": input("Please give a weaviate url: "),
            "auth": self.get_authentication_config()
        }

        with open(self._config_path, 'w') as new_config_file:
                    json.dump(config, new_config_file)

        print("Config creation complete\n\n")

        return config


    def get_authentication_config(self) -> Optional[dict]:

        auth_options = ["No authentication", "Client secret", "Username and password"]
        selection_index = let_user_pick(auth_options, "Please select the authentication method:")
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

    def __str__(self):
        return str(json.dumps(self.config, indent=4))