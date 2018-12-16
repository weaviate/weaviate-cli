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
from urllib.request import Request, urlopen  # Python 3
import json

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
        _, _ = self.Get("/meta")
        # would fail is not available.
        self.helpers(self.config).Info("Pong from Weaviate...")

    def Delete(self, path):
        """This function deletes from a Weaviate."""

        query = Request(self.config["url"] + "/weaviate/v1" + path, method='DELETE')
        query.add_header('X-API-KEY', self.config["key"])
        query.add_header('X-API-TOKEN', self.config["token"])

        # try to request
        try:
            request = urlopen(query)
        except urllib.error.HTTPError as error:
            return 0, json.loads(error.read().decode('utf-8'))

        # return the values
        if len(request.read().decode('utf-8')) == 0:
            return request.status, {}
        else:
            return request.status, json.loads(request.read().decode('utf-8'))

    def Post(self, path, body):
        """This function posts to a Weaviate."""

        query = Request(self.config["url"] + "/weaviate/v1" + path)
        query.add_header('X-API-KEY', self.config["key"])
        query.add_header('X-API-TOKEN', self.config["token"])
        query.add_header('Content-Type', 'application/json; charset=utf-8')

        # format the body
        jsondata = json.dumps(body)
        jsondataasbytes = jsondata.encode('utf-8') # needs to be bytes
        query.add_header('Content-Length', len(jsondataasbytes))

        # try to request
        try:
            request = urlopen(query, jsondataasbytes)
        except urllib.error.HTTPError as error:
            return 0, json.loads(error.read().decode('utf-8'))

        # return the values
        if len(request.read().decode('utf-8')) == 0:
            return request.status, {}
        else:
            return request.status, json.loads(request.read().decode('utf-8'))

    def Get(self, path):
        """This function GETS from a Weaviate Weaviate."""
        query = Request(self.config["url"] + "/weaviate/v1" + path)
        query.add_header('X-API-KEY', self.config["key"])
        query.add_header('X-API-TOKEN', self.config["token"])

        # try to request
        try:
            request = urlopen(query)
        except urllib.error.HTTPError as error:
            return None, json.loads(error.read().decode('utf-8'))

        return request.status, json.loads(request.read().decode('utf-8'))
