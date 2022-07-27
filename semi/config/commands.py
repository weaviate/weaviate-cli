"""
Weaviate CLI config group functions.
"""

from genericpath import exists
import os
from pathlib import Path
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
    print(ctx.obj["config"])


@config_group.command("set", help="Set a new CLI configuration.")
@click.pass_context
def config_set(ctx):
    ctx.obj["config"].create_new_config()


########################################################################################################################
# Helper class
########################################################################################################################


class Configuration:
    default_file_path = ".config/semi_technologies/"
    default_file_name = "configs.json"

    def __init__(self, config_file: Optional[str] = None):
        if config_file:
            assert os.path.isfile(config_file), "Config file does not exist!"
            self.config_path = config_file
            with open(self.config_path, 'r') as config_file:
                try:
                    self.config = json.load(config_file)
                except:
                    click.echo("Fatal Error: Config file is not valid JSON!")
                    sys.exit(1)

        else:
            self.config_path = Path(os.path.join(os.getenv("HOME"),
                                            self.default_file_path,
                                            self.default_file_name))
            self.config_path.touch(exist_ok=True)
            with open(self.config_path, 'r') as config_file:
                try:
                    self.config = json.load(config_file)
                except:
                    click.echo("No existing configuration found, creating new one.")
                    self.create_new_config()


    def get_client(self) -> weaviate.Client:

        if self.config["auth"] is None:
            return weaviate.Client(self.config["url"])
        if self.config["auth"]["type"] == cfg_vals.config_value_auth_type_client_secret:
            cred = weaviate.AuthClientCredentials(self.config["auth"]["secret"])
            return weaviate.Client(self.config["url"], cred)
        if self.config["auth"]["type"] == cfg_vals.config_value_auth_type_username_pass:
            cred = weaviate.AuthClientPassword(self.config["auth"]["user"], self.config["auth"]["pass"])
            return weaviate.Client(self.config["url"], cred)

        click.echo("Fatal Error: Unknown authentication type in config!")
        sys.exit(1)


    def create_new_config(self) -> dict:
        self.config = {
            "url": input("Please give a weaviate url: "),
            "auth": self.get_authentication_config()
        }

        with open(self.config_path, 'w') as new_config_file:
                    json.dump(self.config, new_config_file)

        print("Config creation complete\n")


    def get_authentication_config(self) -> Optional[dict]:

        auth_options = ["No authentication", "Client secret", "Username and password"]
        selection_index = let_user_pick(auth_options, "Please select the authentication method:")
        if selection_index == 1:
            return {
                "type": cfg_vals.config_value_auth_type_client_secret,
                "secret": getpass("Please specify the client secret: ")
            }
        if selection_index == 2:
            return {
                "type": cfg_vals.config_value_auth_type_username_pass,
                "user": input("Please specify the user name: "),
                "pass": getpass("Please specify the user password: ")
            }
        return None

    def __str__(self):
        return str(json.dumps(self.config, indent=4))