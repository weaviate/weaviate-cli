"""
Miscellaneous helper function/command/classes.
"""

import os
import json
import click
from click_params import URL

from semi.version import __version__
import semi.config.config_values as cfg_vals
from semi.config.commands import Configuration
from semi.utils import get_client_from_context, Mutex


@click.command("ping", help="Check if the configured weaviate is reachable.")
@click.pass_context
def main_ping(ctx):
    """
    Ping the active configuration.
    """

    ping(get_client_from_context(ctx))


@click.command("version", help="Version of the CLI")
def main_version():
    """
    Get Version of Weaviate CLI.
    """
    version()


@click.command("init", help="Initialize a new CLI configuration.")
@click.option('--url', required=True, default=None, type=URL, is_flag=False,)
@click.option('--user', required=False, default=None, type=str, is_flag=False,
                cls=Mutex, not_required_if=['client_secret'])
@click.option('--password', required=False, default=None, type=str, is_flag=False,
                cls=Mutex, not_required_if=['client_secret'])
@click.option('--client-secret', required=False, default=None, type=str, is_flag=False,
                cls=Mutex, not_required_if=['user', 'password'])
def main_init(url, user, password, client_secret):
    """
    Create a new user configuration using CLI command options.
    """
    UserConfiguration(url, user, password, client_secret)


def ping(client) -> None:
    """
    Get weaviate ping status.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    """

    if client.is_ready():
        print("Weaviate is reachable!")
    else:
        print("Weaviate not reachable!")


def version() -> None:
    """
    Print weaviate CLI version.
    """

    print(__version__)



_cli_config_sub_path = ".config/semi_technologies/"
_cli_config_file_name = "configs.json"


class UserConfiguration(Configuration):
    def __init__(self, url: str, user: str, password: str, client_secret: str):
        self._config_folder = os.path.join(os.getenv("HOME"), _cli_config_sub_path)
        self._config_path = os.path.join(self._config_folder, _cli_config_file_name)
        if not os.path.isfile(self._config_path):
            try:
                os.makedirs(self._config_folder)
            except FileExistsError:
                pass  # Folders already exist

        self.config = {"url": url}

        if user:
            self.config["auth"] = {
                    "type": cfg_vals.config_value_auth_type_username_pass,
                    "user": user,
                    "pass": password
                }
        elif client_secret:
            self.config["auth"] = {
                    "type": cfg_vals.config_value_auth_type_client_secret,
                    "secret": client_secret
                }
        else:
            self.config["auth"] = None

        with open(self._config_path, 'w') as new_config_file:
            json.dump(self.config, new_config_file)

        print("Config creation complete\n\n")

        super().__init__()
