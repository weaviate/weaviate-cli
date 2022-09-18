"""
Weaviate CLI config group functions.
"""

import os
import sys
import json
from pathlib import Path
from typing import Optional
from getpass import getpass

import click
import requests
import weaviate
from semi.prompt import let_user_pick
import semi.config.config_values as cfg_vals


@click.group("config")
def config_group():
    """
    Configuration of the CLI.
    """
    pass


@config_group.command("view")
@click.pass_context
def config_view(ctx):
    """
    Print the current CLI configuration.
    """

    print(ctx.obj["config"])


@config_group.command("set")
@click.pass_context
def config_set(ctx):
    """
    Set a new CLI configuration.
    """

    ctx.obj["config"].create_new_config()


####################################################################################################
# Helper class
####################################################################################################


class Configuration:
    default_file_path = ".config/semi_technologies/"
    default_file_name = "configs.json"

    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize a configuration instance.

        Parameters
        ----------
        config_file : str or None
            The path to the user specified configuration file.
        """
        if config_file:
            assert os.path.isfile(config_file), "Config file does not exist!"
            self.config_path = config_file
            with open(self.config_path, 'r', encoding="utf-8") as config_data:
                try:
                    self.config = json.load(config_data)
                except:
                    click.echo("Fatal Error: Config file is not valid JSON!")
                    sys.exit(1)

        else:
            self.config_path = Path(os.path.join(os.getenv("HOME"),
                                            self.default_file_path,
                                            self.default_file_name))
            self.config_path.touch(exist_ok=True)
            with open(self.config_path, 'r', encoding="utf-8") as configuration_file:
                try:
                    self.config = json.load(configuration_file)
                except:
                    click.echo("No existing configuration found, creating new one.")
                    self.create_new_config()


    def get_client(self) -> weaviate.Client:
        """
        Get a weaviate client from the configuration.

        Returns
        -------
        weaviate.Client
            A weaviate client.
        """

        try:
            if self.config["auth"] is None:
                return weaviate.Client(self.config["url"])
            if self.config["auth"]["type"] == cfg_vals.config_value_auth_type_client_secret:
                cred = weaviate.AuthClientCredentials(self.config["auth"]["secret"])
                return weaviate.Client(self.config["url"], cred)
            if self.config["auth"]["type"] == cfg_vals.config_value_auth_type_username_pass:
                cred = weaviate.AuthClientPassword(self.config["auth"]["user"], self.config["auth"]["pass"])
                return weaviate.Client(self.config["url"], cred)
        except requests.exceptions.ConnectionError:
            click.echo("Fatal Error: Could not connect to the weaviate instance!")
            sys.exit(1)
        except KeyError:
            click.echo("Fatal Error: Unknown authentication type in config!")
            sys.exit(1)


    def create_new_config(self):
        """
        Create a new configuration (interacitve).
        """

        self.config = {
            "url": input("Please give a weaviate url: "),
            "auth": self.get_authentication_config()
        }

        self.write_config(self.config)

        print("Config creation complete\n")


    @classmethod
    def write_config(cls, configuration: dict):
        """
        Write a configuration to the config file.

        Parameters
        ----------
        configuration : dict
            The configuration to write.
        """

        config_path = Path(os.path.join(os.getenv("HOME"),
                                            cls.default_file_path,
                                            cls.default_file_name))

        with open(config_path, 'w', encoding="utf-8") as configuration_file:
            json.dump(configuration, configuration_file)


    @classmethod
    def create_user_specified_config(cls, url: str, user: str, password: str, client_secret: str):
        """
        Create a configuration from user specified values.

        Parameters
        ----------
        url : str
            The weaviate url.
        user : str
            The user name.
        password : str
            The user password.
        client_secret : str
            The client secret.
        """

        if user and password:
            auth = {
                "type": cfg_vals.config_value_auth_type_username_pass,
                "user": user,
                "pass": password
            }
        elif client_secret:
            auth = {
                "type": cfg_vals.config_value_auth_type_client_secret,
                "secret": client_secret
            }
        else:
            auth = None

        config = {
            "url": url,
            "auth": auth
        }

        cls.write_config(config)


    def get_authentication_config(self) -> Optional[dict]:
        """
        Get the authentication configuration.

        Returns
        -------
        dict
            The authentication configuration.
        """

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
