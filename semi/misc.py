"""
Miscellaneous helper function/command/classes.
"""

import click
from click_params import URL
import weaviate

from semi.version import __version__
from semi.config.commands import Configuration
from semi.utils import get_client_from_context, Mutex


@click.command("ping")
@click.pass_context
def main_ping(ctx):
    """
    Ping the active configuration.
    """

    ping(get_client_from_context(ctx))


@click.command("version")
def main_version():
    """
    Get Version of Weaviate CLI.
    """
    version()


@click.command("init")
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
    Configuration.create_user_specified_config(url, user, password, client_secret)


def ping(client: weaviate.Client):
    """
    Get weaviate ping status.

    Parameters
    ----------
    client : weaviate.Client
        A weaviate client.
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
