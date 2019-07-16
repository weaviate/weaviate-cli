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
import datetime, time

class Init:
    """This class produces the Init functions. \
    These are needed to validate if the tool is setup correctly \
    and if a Weaviate is present."""

    def __init__(self):
        """This function inits the class"""

        self.configFile = expanduser("~") + "/weaviate.conf"

    def UpdateConfigFile(self, key, value):
        """Update a single key in the config file"""
    
        # Load and update config YAML
        currentYaml = yaml.load(open(self.configFile), Loader=yaml.FullLoader)
        currentYaml[key] = value

        # Store config YAML
        try:
            with open(self.configFile, 'w') as configFile:
                yaml.dump(currentYaml, configFile, default_flow_style=False)
            Helpers(None).Info(Messages().Get(119))
        except IOError:
            Helpers(None).Error(Messages().Get(205) + self.configFile)

    def setConfig(self, options):
        """This function sets the config file"""

        configVars = {
            "url": "",
            "email": "",
            "auth": None
        }

        # Validate and set URL 
        if options.url == None:
            options.url = input(Messages().Get(130) + ": ")
        if validators.url(options.url) != True:
            Helpers(None).Error(Messages().Get(211))

        # Validate and set email 
        if options.email == None:
            options.email = input(Messages().Get(131) + ": ")
        if validators.email(options.email) != True:
            Helpers(None).Error(Messages().Get(212))

        # Validate and set auth 
        if options.auth == None:
            options.auth = input(Messages().Get(134) + ": ")

        # No auth is selected
        if options.auth == "1":
            configVars['auth'] = None
        # OAuth selected, ask followup questions
        elif options.auth == "2":
            # Fixed OAuth variables
            configVars["auth_bearer"] = None
            configVars["auth_expires"] = 0
            # Request variables
            if options.auth_url == None:
                options.auth_url = input(Messages().Get(139) + ": ")
                configVars["auth_url"] = options.auth_url
            if options.auth_clientid == None:
                options.auth_clientid = input(Messages().Get(135) + ": ")
                configVars["auth_clientid"] = options.auth_clientid
            if options.auth_granttype == None:
                options.auth_granttype = input(Messages().Get(136) + ": ")
                configVars["auth_granttype"] = options.auth_granttype
            if options.auth_clientsecret == None:
                options.auth_clientsecret = input(Messages().Get(137) + ": ")
                configVars["auth_clientsecret"] = options.auth_clientsecret
            if options.auth_realmid == None:
                options.auth_realmid = input(Messages().Get(138) + ": ")
                configVars["auth_realmid"] = options.auth_realmid
        else:
            Helpers(None).Error(Messages().Get(215))

        # start creating the config file
        configVars["url"] = options.url
        configVars["email"] = options.email
        configVars["auth"] = int(options.auth)

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
            configYaml = yaml.load(open(self.configFile), Loader=yaml.FullLoader)
            Helpers(configYaml).Info(Messages().Get(120))
            return configYaml
        except IOError:
            Helpers(None).Error(Messages().Get(206))
