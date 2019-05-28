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

    def UpdateConfigFile(self, key, value):
        """Update a single key in the config file"""
    
        # Load and update config YAML
        currentYaml = yaml.load(open(self.configFile))
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
        if options.init_url == None:
            options.init_url = input(Messages().Get(130) + ": ")
        if validators.url(options.init_url) != True:
            Helpers(None).Error(Messages().Get(211))

        # Validate and set email 
        if options.init_email == None:
            options.init_email = input(Messages().Get(131) + ": ")
        if validators.email(options.init_email) != True:
            Helpers(None).Error(Messages().Get(212))

        # Validate and set auth 
        if options.init_auth == None:
            options.init_auth = input(Messages().Get(134) + ": ")

        # No auth is selected
        if options.init_auth == "1":
            configVars['auth'] = None
        # OAuth selected, ask followup questions
        if options.init_auth == "2":
            # Fixed OAuth variables
            configVars["auth_bearer"] = None
            configVars["auth_expires"] = 0
            # Request variables
            if options.init_auth_url == None:
                options.init_auth_url = input(Messages().Get(139) + ": ")
                configVars["auth_url"] = options.init_auth_url
            if options.init_auth_clientid == None:
                options.init_auth_clientid = input(Messages().Get(135) + ": ")
                configVars["auth_clientid"] = options.init_auth_clientid
            if options.init_auth_granttype == None:
                options.init_auth_granttype = input(Messages().Get(136) + ": ")
                configVars["auth_granttype"] = options.init_auth_granttype
            if options.init_auth_clientsecret == None:
                options.init_auth_clientsecret = input(Messages().Get(137) + ": ")
                configVars["auth_clientsecret"] = options.init_auth_clientsecret
            if options.init_auth_realmid == None:
                options.init_auth_realmid = input(Messages().Get(138) + ": ")
                configVars["auth_realmid"] = options.init_auth_realmid
        else:
            Helpers(None).Error(Messages().Get(215))

        # start creating the config file
        configVars["url"] = options.init_url
        configVars["email"] = options.init_email
        configVars["auth"] = int(options.init_auth)

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
