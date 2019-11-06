import weaviate
from modules.config import *


class Commands:
    """ Class for encapsulating the more complex commands.
        Makes use of the config and the client
    """

    def __init__(self):
        """ Create a new commands object to handle more complex commands

        :param parsed_arguments: Arguments parsed from ArgumentParser
        """
        # Load the config
        self.config = load_config()

        # Create a client
        auth_clientsecret = ""
        if CONFIG_KEY_AUTH_CLIENTSECRET in self.config:
            auth_clientsecret = self.config[CONFIG_KEY_AUTH_CLIENTSECRET]
        self.client = weaviate.Client(self.config[CONFIG_KEY_URL], auth_clientsecret)

        # Check if weaviate is reachable
        if not self.client.is_reachable():
            Helpers(self.config).Error(Messages().Get(210))

    def schema_import(self, schema_location=None, force_replace_schema=False):
        # start the import
        self.helpers.Info(Messages().Get(113))

        schema_present = False  # is there already a schema loaded?
        existing_schema = self.client.get_schema()
        if len(existing_schema["things"]["classes"]) > 0 or len(existing_schema["actions"]["classes"]) > 0:
            schema_present = True  # Schema is already loaded

        if schema_present and not force_replace_schema:
            self.helpers.Error(Messages().Get(208))  # Exit

        if schema_present and force_replace_schema:
            pass
            # TODO check if data is in weaviate
            #  If data is in weaviate warn and exit
            #  else Delete existing schema

        # TODO load schema