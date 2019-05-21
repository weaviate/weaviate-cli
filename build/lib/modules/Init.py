##                          _       _
##__      _____  __ ___   ___  __ _| |_ ___
##\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
## \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
##  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
##
## Copyright © 2016 - 2018 Weaviate. All rights reserved.
## LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
## AUTHOR: Bob van Luijt (bob@kub.design)
## See www.creativesoftwarefdn.org for details
## Contact: @CreativeSofwFdn / bob@kub.design
##

"""This is the Init module for the Weaviate-cli tool."""
from os.path import expanduser
import validators
import yaml
from modules.Helpers import Helpers
from modules.Messages import Messages

class Init:
    """This class produces the Init functions. \
    These are needed to validate if the tool is setup correctly \
    and if a Weaviate is present."""

    def __init__(self):
        """This function inits the class"""

        self.configFile = expanduser("~") + "/weaviate.conf"

    def setConfig(self, options):
        """This function sets the config file"""

        configVars = {
            "url": "",
            "email": ""
        }

        # Validate and set URL 
        if options.init_url == None:
            options.init_url = input(Messages().Get(130) + ": ")
        if validators.url(options.init_url) != True:
            Helpers(None).Error(Messages().Get(211))

        if options.init_email == None:
            options.init_email = input(Messages().Get(131) + ": ")
        if validators.email(options.init_email) != True:
            Helpers(None).Error(Messages().Get(212))

        # start creating the config file
        configVars["url"] = options.init_url
        configVars["email"] = options.init_email

        # write to file
        try:
            with open(self.configFile, 'w') as configFile:
                yaml.dump(configVars, configFile, default_flow_style=False)
            Helpers(None).Info(Messages().Get(119))
        except IOError:
            Helpers(None).Error(Messages().Get(205) + self.configFile)

    def loadConfig(self):
        """This function loads the config file or errors if it is not available"""

        # passed this point a valid config file should be available
        try:
            configYaml = yaml.load(open(self.configFile))
            Helpers(configYaml).Info(Messages().Get(120))
            return configYaml
        except IOError:
            Helpers(None).Error(Messages().Get(206))
