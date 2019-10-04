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

"""This module handles creation of Weaviate Clusters."""

class Cluster:
    """This class handles the export of a Weaviate schema."""

    def __init__(self, c):
        """This function inits the export of schema module."""
        from modules.Helpers import Helpers
        from modules.Weaviate import Weaviate
        self.config = c
        self.helpers = Helpers(c)
        self.weaviate = Weaviate(c)
        try:
            if self.config['developer']:
                self.clusterProtocoll = "http://"
                self.clusterAPI = self.clusterProtocoll + "dev.wcs.api.semi.technology/v1/clusters/"
                self.clusterInstanceDomain = ".dev.semi.network"
            else:
                self.clusterProtocoll = "https://"
                self.clusterAPI = self.clusterProtocoll + "wcs.api.semi.technology/v1/clusters/"
                self.clusterInstanceDomain = ".semi.network"
        except KeyError:
            self.clusterProtocoll = "https://"
            self.clusterAPI = self.clusterProtocoll + "wcs.api.semi.technology/v1/clusters/"
            self.clusterInstanceDomain = ".semi.network"


    # run the cluster service
    def Run(self, create, remove, setNotAsDefault, runAsync, replace, all, force):

        # can't both create and remove clusters. Error...
        if create == True and remove == True:
            self.helpers.Error(Messages().Get(217))

        # If create nor remove is set. Error...
        if create == False and remove == False:
            self.helpers.Error(Messages().Get(221))

        # validate if clusters should be listed or created
        if create == True:

            # check if there already is a cluster set. You can't have 2
            if 'cluster' in self.config:
                if self.__status(self.config['cluster']) == 200:
                    if replace != True:
                        shouldReplace = input(Messages().Get(150))
                        if shouldReplace != 'y':
                            return
                        else:
                            self.__delete(self.config['cluster'])
                            Init().UpdateConfigFile('cluster', '')

            clusterId = self.__create(setNotAsDefault, runAsync)
            self.helpers.Info("Cluster will be available on: " + self.clusterProtocoll + str(clusterId) + self.clusterInstanceDomain)

            # Set not as default url
            if setNotAsDefault == True:
                self.helpers.Info(Messages().Get(147))
            else:
                self.helpers.Info(Messages().Get(148))
                Init().UpdateConfigFile('url', self.clusterProtocoll + str(clusterId) + self.clusterInstanceDomain)
                Init().UpdateConfigFile('cluster', str(clusterId))

            # run async service
            if runAsync == False:
                isClusterDone = False
                previousState = ''
                cluster_ready_components_percentage = 0
                while isClusterDone == False:
                    state = self.__info(clusterId)['status']['state']

                    if 'percentage' not in state:
                        state['percentage'] = 0

                    if state != previousState:
                        if math.ceil(state['percentage']) > cluster_ready_components_percentage:
                            # Only show when cluster has gotten bigger
                            cluster_ready_components_percentage = math.ceil(state['percentage'])
                        self.helpers.Info(str(cluster_ready_components_percentage) + '% | ' + state['message'])
                    if state['percentage'] == 100:
                        self.helpers.Info("Cluster is available on: " + self.clusterProtocoll + str(clusterId) + self.clusterInstanceDomain)
                        isClusterDone = True
                    time.sleep(2)
                    previousState = state
            return
        elif remove == True:
            if not (force == True):
                var = input(Messages().Get(155))
                if not (var == "y" or var == "Y"):
                    exit(0)
            if all:
                clusters = self.__list()
                for box in clusters:
                    self.helpers.Info("Delete cluster: " + box)
                    self.__delete(box)
            else:
                if 'cluster' in self.config:
                    self.helpers.Info("Delete cluster: " + str(self.config['cluster']))
                    self.__delete(self.config['cluster'])

        # done
        exit(0)

    def ListClusters(self):
        self.config
        listOfClusters = self.__list()
        if len(listOfClusters) == 0:
            self.helpers.Info(Messages().Get(152))
        else:
            self.helpers.Info(Messages().Get(153))
            for i, id in enumerate(listOfClusters):
                print(str(i+1) + ":\t" + id)


    # Run API request
    def __APIcall(self, action, bodyOrQuery, runAsync):
        """Runs the API call, accepts: 'create', 'delete', 'info' and 'status'"""
        """Returns the ID"""
        if action == 'create':
            try:
                request = requests.post(self.clusterAPI, json.dumps(bodyOrQuery), headers={"content-type": "application/json"})
                return request.json()
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        elif action == 'delete':
            try:
                request = requests.delete(self.clusterAPI + bodyOrQuery)
                return None
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        elif action == 'status':
            try:
                request = requests.get(self.clusterAPI + bodyOrQuery)
                return request.status_code
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        elif action == 'list':
            try:
                request = requests.get(self.clusterAPI + bodyOrQuery)
                return request.json()
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        else: # assume info
            try:
                request = requests.get(self.clusterAPI + bodyOrQuery)
                return request.json()
            except urllib.error.HTTPError as _:
                Messages().Get(Messages().Get(218))
        return None

    # create a cluster
    def __create(self, setNotAsDefault, runAsync):
        """This module handles the import of a Weaviate instance."""
        return self.__APIcall('create', {
            "email": self.config['email']
        }, runAsync)['id']

    # remove a cluster
    def __delete(self, id):
        """Function to delete a cluster"""
        return self.__APIcall('delete', id, None)

    # list semi clusters, return a list of all clusterIDs for that email
    def __list(self):
        path = 'list?email=' + self.config['email']
        result = self.__APIcall('list', path, None)
        try:
            IDs = result['clusterIDs']
            if IDs is None:
                return []
            return IDs
        except KeyError as e:
            Messages().Get(Messages().Get(222))
            return []


    # list info of a cluster
    def __info(self, id):
        """Function to show info of a cluster"""
        return self.__APIcall('info', id, None)

    def __status(self, id):
        """Get status ID (http code) of a cluster"""
        return self.__APIcall('status', id, None)