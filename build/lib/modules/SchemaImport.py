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

"""This module handles the import of a Weaviate instance."""
import json
import validators
import urllib
from modules.Messages import Messages

class SchemaImport:
    """This class handles the import of a schema."""

    def __init__(self, c):
        """This function inits the import module and sets the config."""
        from modules.Helpers import Helpers
        from modules.Weaviate import Weaviate
        self.config = c
        self.helpers = Helpers(c)
        self.weaviate = Weaviate(c)

    def checkIfThereIsData(self, i):
        """This functions checks if there is data, returns True if there is."""
        _, amountOfThings = self.weaviate.Get("/" + i)
        if len(amountOfThings[i]) == 0:
            return False
        else:
            return True

    def downloadSchemaFiles(self, outputFile, url):
        """This functions downloads a schema file."""
        thingsFileFromUrl = urllib.request.urlopen(url)
        data = thingsFileFromUrl.read()
        with open(outputFile, 'w+') as output:
            output.write(data.decode('utf-8'))
        return outputFile

    def Run(self, ontologyFile, deleteIfFound):
        """This functions runs the import module."""

        # start the import
        self.helpers.Info(Messages().Get(113))

        # Check if the schema is empty
        if deleteIfFound == False:
            schemaThingsCount, schemaActionsCount = self.helpers.SchemaCount()
            if schemaThingsCount != 0 or schemaActionsCount != 0:
                self.helpers.Error(Messages().Get(208))

        # check if things files is url
        if ontologyFile == None:
            ontologyFile = input(Messages().Get(132) + ": ")

        if validators.url(ontologyFile) is True:
            ontologyFile = self.downloadSchemaFiles('./ontology.json', ontologyFile)

        # open the thingsfile
        try:
            with open(ontologyFile, 'r') as file:
                ontology = json.load(file)
        except IOError:
            self.helpers.Error(Messages().Get(201) + ontologyFile)

        # Set things and actions from ontology file
        if "actions" not in ontology or "classes" not in ontology["actions"]:
            self.helpers.Error(Messages().Get(209) + "actions")
        elif "things" not in ontology or "classes" not in ontology["things"]:
            self.helpers.Error(Messages().Get(209) + "things")

        actions = ontology["actions"]
        things = ontology["things"]

        # Validate if delete function would work
        if deleteIfFound is True:
            self.helpers.Info(Messages().Get(114))

            # check if there is data
            if self.checkIfThereIsData("things") is True \
            or self.checkIfThereIsData("actions") is True:
                self.helpers.Error(Messages().Get(203))

        # Render and create things
        self.helpers.Info(Messages().Get(115) + "things")
        self.helpers.CreateConceptClasses("things", things["classes"], deleteIfFound)

        # Render and create actions
        self.helpers.Info(Messages().Get(115) + "actions")
        self.helpers.CreateConceptClasses("actions", actions["classes"], deleteIfFound)

        # Add properties to things (needs to run after CreateConceptClasses()!)
        self.helpers.Info(Messages().Get(116) + "things")
        self.helpers.AddPropsToConceptClasses("things", things["classes"], deleteIfFound)

        # Add properties to actions (needs to run after CreateConceptClasses()!)
        self.helpers.Info(Messages().Get(116) + "actions")
        self.helpers.AddPropsToConceptClasses("actions", actions["classes"], deleteIfFound)

        # Validate Things & Actions
        self.helpers.Info(Messages().Get(117))
        if self.helpers.ValidateConceptClasses(things["classes"], actions["classes"]) is True:
            self.helpers.Info(Messages().Get(118))
            exit(0)
        else:
            self.helpers.Error(Messages().Get(204))
