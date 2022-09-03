"""
Weaviate CLI data group functions.
"""

import os
import json
import click
from weaviate.wcs import WCS
from weaviate.auth import AuthClientCredentials, AuthClientPassword

from semi.prompt import let_user_pick


@click.group("cloud")
@click.pass_context
def cloud_group(ctx: click.Context):
    """
        Manage WCS cluster instances.
    """
    ctx.obj["cloud_client"] = get_wcs_client()


@cloud_group.command("add", help="Add a new WCS account.")
def cloud_add():
    """
        Add a new WCS account.
    """
    create_new_wcs_account()


@cloud_group.command("list", help="List all WCS accounts.")
@click.pass_context
def cloud_list(ctx):
    """
        List all WCS accounts.
    """
    all_clusters = ctx.obj["cloud_client"].get_clusters()
    if all_clusters:
        print("Available clusters:")
        for index, cluster_name in enumerate(all_clusters, start=1):
            click.echo(f"{index}. {cluster_name}")
    else:
        print("No clusters available.")


@cloud_group.command("create", help="Create a new WCS cluster.")
@click.pass_context
@click.option('--name', required=True, type=str, help="Name of the cluster.")
def cloud_create(ctx, name):
    """
        Create a new WCS cluster.
    """
    ctx.obj["cloud_client"].create(name)


@cloud_group.command("delete", help="Delete a WCS cluster.")
@click.pass_context
@click.argument('cluster_id')
def cloud_delete(ctx, cluster_id):
    """
        Delete a WCS cluster.
    """
    ctx.obj["cloud_client"].delete_cluster(cluster_id)


@cloud_group.command("status", help="Get the status of a WCS cluster.")
@click.pass_context
@click.argument('cluster_id')
def cloud_status(ctx, cluster_id):
    """
        Get the status of a WCS cluster.
    """
    if ctx.obj["cloud_client"].is_ready(cluster_id):
        print("Ready.")
    else:
        print("Not ready.")


####################################################################################################
# Helper functions
####################################################################################################

# TODO: Move all functions to a class

cli_wcs_sub_path = ".config/semi_technologies/"
cli_wcs_filename = "wcs.json"


def create_new_wcs_account():
    home = os.getenv("HOME")
    config_folder = os.path.join(home, cli_wcs_sub_path)
    wcs_path = os.path.join(config_folder, cli_wcs_filename)

    if not os.path.isfile(wcs_path):
        print("No WCS user was found, creating a new one.")
        wcs_config = create_new_user()
        with open(wcs_path, 'w') as new_config_file:
            json.dump(wcs_config, new_config_file)


def create_new_user() -> dict:
    wcs_auth_options = ["Client Secret", "Username and Password"]
    selection_index = let_user_pick(wcs_auth_options,
                                "Please select the authentication method:") + 1
    if selection_index == 1:
        return {
            "type": "client_secret",
            "secret": input("Please specify the client secret: ")
        }
    if selection_index == 2:
        return {
            "type": "username_password",
            "username": input("Please specify the user name: "),
            "password": input("Please specify the user password: ")
        }
    click.echo("Invalid selection.")
    return create_new_user()

def get_wcs_config() -> dict:
    home = os.getenv("HOME")
    config_folder = os.path.join(home, cli_wcs_sub_path)
    wcs_path = os.path.join(config_folder, cli_wcs_filename)

    if not os.path.isfile(wcs_path):
        create_new_wcs_account()

    with open(wcs_path, 'r') as config_file:
        return json.load(config_file)


def get_wcs_client() -> WCS:
    wcs_config = get_wcs_config()
    if wcs_config["type"] == "client_secret":
        return WCS(AuthClientCredentials(wcs_config["secret"]))
    if wcs_config["type"] == "username_password":
        return WCS(AuthClientPassword(wcs_config["username"], wcs_config["password"]))
