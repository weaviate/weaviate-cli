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
from modules.Messages import Messages
from modules.Init import Init
import requests
import urllib
import json
import time
import math

"""This module handles creation of Weaviate Sandboxes."""

class Sandbox:
    """This class handles the export of a Weaviate schema."""

    def __init__(self, c):
        """This function inits the export of schema module."""
        from modules.Helpers import Helpers
        from modules.Weaviate import Weaviate
        self.config = c
        self.helpers = Helpers(c)
        self.weaviate = Weaviate(c)

    # run the sandbox service
    def Run(self, create, remove, setNotAsDefault, runAsync, replace, all, force):

        # can't both create and remove sandboxes. Error...
        if create == True and remove == True:
            self.helpers.Error(Messages().Get(217))

        # If create nor remove is set. Error...
        if create == False and remove == False:
            self.helpers.Error(Messages().Get(221))

        # validate if sandboxes should be listed or created
        if create == True:

            # check if there already is a sandbox set. You can't have 2
            if 'sandbox' in self.config:
                if self.__status(self.config['sandbox']) == 200:
                    if replace != True:
                        shouldReplace = input(Messages().Get(150))
                        if shouldReplace != 'y':
                            return
                        else:
                            self.__delete(self.config['sandbox'])
                            Init().UpdateConfigFile('sandbox', '')

            sandboxId = self.__create(setNotAsDefault, runAsync)
            self.helpers.Info("Sandbox will be available on: https://" + str(sandboxId) + ".semi.network")

            # Set not as default url
            if setNotAsDefault == True:
                self.helpers.Info(Messages().Get(147))
            else:
                self.helpers.Info(Messages().Get(148))
                Init().UpdateConfigFile('url', 'https://' + str(sandboxId) + '.semi.network')
                Init().UpdateConfigFile('sandbox', str(sandboxId))

            # run async service
            if runAsync == False:
                isSandboxDone = False
                previousState = ''
                while isSandboxDone == False:
                    state = self.__info(sandboxId)['status']['state']

                    if 'percentage' not in state:
                        state['percentage'] = 0

                    if state != previousState:
                        self.helpers.Info(str(math.ceil(state['percentage'])) + '% | ' + state['message'])
                    if state['percentage'] == 100:
                        self.helpers.Info("Sandbox is available on: https://" + str(sandboxId) + ".semi.network")
                        isSandboxDone = True
                    time.sleep(2)
                    previousState = state
            return
        elif remove == True:
            if not (force == True):
                var = input(Messages().Get(155))
                if not (var == "y" or var == "Y"):
                    exit(0)
            if all:
                sandboxes = self.__list()
                for box in sandboxes:
                    self.helpers.Info("Delete sandbox: " + box)
                    self.__delete(box)
            else:
                if 'sandbox' in self.config:
                    self.helpers.Info("Delete sandbox: " + str(self.config['sandbox']))
                    self.__delete(self.config['sandbox'])
        else:
            self.helpers.Info(self.__info(sandboxId)['status']['state'])

        # done
        exit(0)

    def ListSandboxes(self):
        self.config
        listOfSandboxes = self.__list()
        if len(listOfSandboxes) == 0:
            self.helpers.Info(Messages().Get(152))
        else:
            self.helpers.Info(Messages().Get(153))
            for i, id in enumerate(listOfSandboxes):
                print(str(i+1) + ":\t" + id)


    # Run API request
    def __APIcall(self, action, bodyOrQuery, runAsync):
        """Runs the API call, accepts: 'create', 'delete', 'info' and 'status'"""
        """Returns the ID"""
        if action == 'create':
            try:
                request = requests.post('http://sandbox.api.semi.technology/v1/sandboxes', json.dumps(bodyOrQuery), headers={"content-type": "application/json"})
                return request.json()
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        elif action == 'delete':
            try:
                request = requests.delete('http://sandbox.api.semi.technology/v1/sandboxes/' + bodyOrQuery)
                return None
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        elif action == 'status':
            try:
                request = requests.get('http://sandbox.api.semi.technology/v1/sandboxes/' + bodyOrQuery)
                return request.status_code
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        elif action == 'list':
            try:
                request = requests.get('http://sandbox.api.semi.technology/v1/sandboxes/' + bodyOrQuery)
                return request.json()
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        else: # assume info
            try:
                request = requests.get('http://sandbox.api.semi.technology/v1/sandboxes/' + bodyOrQuery)
                return request.json()
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        return None

    # create a sandbox
    def __create(self, setNotAsDefault, runAsync):
        """This module handles the import of a Weaviate instance."""
        return self.__APIcall('create', {
            "email": self.config['email']
        }, runAsync)['id']

    # remove a sandbox
    def __delete(self, id):
        """Function to delete a sandbox"""
        return self.__APIcall('delete', id, None)

    # list semi sandboxes, return a list of all sandboxIDs for that email
    def __list(self):
        path = 'list?email=' + self.config['email']
        result = self.__APIcall('list', path, None)
        try:
            IDs = result['sandboxIDs']
            if IDs is None:
                return []
            return IDs
        except KeyError as e:
            Messages().Get(Messages().Get(222))
            return []


    # list info of a sandbox
    def __info(self, id):
        """Function to show info of a sandbox"""
        return self.__APIcall('info', id, None)

    def __status(self, id):
        """Get status ID (http code) of a sandbox"""
        return self.__APIcall('status', id, None)