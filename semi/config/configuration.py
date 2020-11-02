import os.path
import json
import weaviate


_cli_config_sub_path = ".config/semi_technologies/weaviate_cli.json"
_cli_config_file_name = "weaviate_cli.json"

class Configuration:

    def __init__(self):
        home = os.getenv("HOME")
        self._config_folder = os.path.join(home, _cli_config_sub_path)
        self._config_path = os.path.join(self._config_folder, _cli_config_file_name)

        if not os.path.isfile(self._config_path):
            self.init()

        with open(self._config_path, 'r') as config_file:
            self.config = json.load(config_file)

        self.client = weaviate.Client(self.config["url"])

    def init(self):
        """ Create the config folder and prompt user for an inital config

        :return:
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


def create_new_config():
    """

    :return: config as prompted from the user
    :rtype: json
    """
    cfg = {
        "url": input("Please give a weaviate url: ")
    }
    return cfg