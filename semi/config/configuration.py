import os.path
import json


cli_config_sub_path = ".config/semi_technologies/weaviate_cli.json"
cli_config_file_name = "weaviate_cli.json"

class Configuration:

    def __init__(self):
        home = os.getenv("HOME")
        self.config_folder = os.path.join(home, cli_config_sub_path)
        self.config_path = os.path.join(self.config_folder, cli_config_file_name)

        if not os.path.isfile(self.config_path):
            self.init()

        with open(self.config_path, 'r') as config_file:
            self.config = json.load(config_file)

    def init(self):
        """ Create the config folder and prompt user for an inital config

        :return:
        """
        try:
            os.makedirs(self.config_folder)
        except:
            pass  # Folders already exist

        cfg = create_new_config()
        with open(self.config_path, 'w') as new_config_file:
            json.dump(cfg, new_config_file)


def create_new_config():
    """

    :return: config as prompted from the user
    :rtype: json
    """
    cfg = {
        "url": input("Please give a weaviate url: ")
    }
    return cfg