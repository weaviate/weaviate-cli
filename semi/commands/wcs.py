from weaviate.wcs import WCS
import weaviate.auth
import os.path
import json
import click
from semi.prompt import let_user_pick

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


def create_new_user():
    wcs_auth_options = ["Client Secret", "Username and Password"]
    selection_index = let_user_pick(wcs_auth_options, "Please select the authentication method:") + 1
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

def get_wcs_config():
    home = os.getenv("HOME")
    config_folder = os.path.join(home, cli_wcs_sub_path)
    wcs_path = os.path.join(config_folder, cli_wcs_filename)

    if not os.path.isfile(wcs_path):
        create_new_wcs_account()

    with open(wcs_path, 'r') as config_file:
        return json.load(config_file)


def get_wcs_client():
    wcs_config = get_wcs_config()
    if wcs_config["type"] == "client_secret":
        return WCS(weaviate.auth.AuthClientCredentials(wcs_config["secret"]))
    if wcs_config["type"] == "username_password":
        return WCS(weaviate.auth.AuthClientPassword(wcs_config["username"], wcs_config["password"]))
