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

"""This module handles the communication with Weaviate."""
import urllib
import json
import requests
from modules.Messages import Messages

class Weaviate:
    """This class handles the communication with Weaviate."""

    def __init__(self, c):
        """This function inits the class."""

        # make the config available in this class
        from modules.Helpers import Helpers
        self.config = c
        self.helpers = Helpers

    def Ping(self):
        """This function pings a Weaviate to see if it is online."""

        self.helpers(self.config).Info("Ping Weaviate...")
        # get the meta endpoint
        try:
            status, _ = self.Get("/meta")
        except:
            self.helpers(self.config).Error(Messages().Get(210))
        # throw error if failed
        if status != 200 or status == None:
            self.helpers(self.config).Error(Messages().Get(210))
        # would fail is not available.
        self.helpers(self.config).Info("Pong from Weaviate...")

    def Delete(self, path):
        """This function deletes from a Weaviate."""

        # try to request
        try:
            request = requests.delete(self.config["url"] + "/weaviate/v1" + path)
        except urllib.error.HTTPError as error:
            return None

        return request.status_code

    def Post(self, path, body):
        """This function posts to a Weaviate."""

        # try to request
        try:
            request = requests.post(self.config["url"] + "/weaviate/v1" + path, json.dumps(body), headers={"content-type": "application/json"})
        except urllib.error.HTTPError as error:
            return 0, json.loads(error.read().decode('utf-8'))

        # return the values
        if len(request.json()) == 0:
            return request.status_code, {}
        else: 
            return request.status_code, request.json()

    def Get(self, path):
        """This function GETS from a Weaviate Weaviate."""

        # try to request
        try:
            request = requests.get(self.config["url"] + "/weaviate/v1" + path)
        except urllib.error.HTTPError as error:
            return None, json.loads(error.read().decode('utf-8'))

        return request.status_code, request.json()