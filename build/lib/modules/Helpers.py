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

"""This is the Helpers module for the Weaviate-cli tool."""
import datetime

class Helpers:
    """This class produces the Helpers."""

    def __init__(self, c):
        """This function inits the Helpers and sets the config."""

        # make the config available in this class
        from modules.Weaviate import Weaviate
        self.config = c
        self.weaviate = Weaviate(c)

    def ValidateAndGet(self, thing, value, context):
        """This function validates if a key is available and throws an error if not."""

        # Only if cardinality is not a Class
        if value == "cardinality":
            if "cardinality" in thing:
                if thing["cardinality"][0].isupper() is True:
                    return "atMostOne"
            else:
                return "atMostOne"

        # check if thing (t) has this value (v)
        if value in thing:
            return thing[value]
        else:
            if "name" in thing:
                self.Error("Can't locate: " + value + \
                    " in the context of: [" + thing["name"] + "].\
                    Are your files correctly formated?  \
                    Hint: There might be a cross-ref without a cardinality in your schema.")
            else:
                self.Error("Can't locate: " + value + \
                    " in the context of: [" + context + "]. Are your files correctly formated? " \
                    "Hint: There might be a cross-ref without a cardinality in your schema.")

    def CreateConceptClasses(self, name, concepts, deleteIfFound):
        """This function creates a concept in Weaviate style"""

        # Loop over all concepts and add them without properties
        for concept in concepts:

            sendObject = {
                "class": self.ValidateAndGet(concept, "class", "classname of " + name),
                "description": self.ValidateAndGet(concept, "description", \
                "description of " + name),
                "properties": [],
                "keywords": []
            }

            # Create empty schema first, this is done to avoid loops in the schema.
            noPropertySendObject = sendObject
            noPropertySendObject["properties"] = []

            code, _ = self.weaviate.Post("/schema/" + name, noPropertySendObject)

            if code is 0 and deleteIfFound is True:
                self.weaviate.Delete("/schema/" + name + noPropertySendObject["class"])

            # Add the item
            self.weaviate.Post("/schema/" + name, sendObject)

    def AddPropsToConceptClasses(self, name, concepts, deleteIfFound):
        """This function adds properties to a concept object. \
        needs to run after CreateConceptClasses()."""

        # Loop over all concepts and add the properties
        for concept in concepts:

            # loop over the properties
            self.ValidateAndGet(concept, "properties", "properties of root " + name)

            for prperty in concept["properties"]:

                # create the property object
                propertyObject = {
                    "dataType": [],
                    "cardinality": self.ValidateAndGet(prperty, "cardinality", "cardinality of " + name),
                    "description": self.ValidateAndGet(prperty, "description", "description of " + name),
                    "name": self.ValidateAndGet(prperty, "name", "name of " + name)
                }

                # validate if dataType is formatted correctly.
                self.ValidateAndGet(prperty, "dataType", "dataType of" + name)

                if len(prperty["dataType"]) == 0:
                    self.Error("There is no dataType for the Thing with class: " \
                    + self.ValidateAndGet(prperty, "name", "root: " + name))

                # check if the dataTypes are set correctly (with multiple crefs, only crefs)
                if len(prperty["dataType"]) > 1:
                    # check if they are all crefs
                    correctlyFormatted = True
                    for datatype in prperty["dataType"]:
                        if datatype[0] != datatype[0].capitalize():
                            correctlyFormatted = False
                    if correctlyFormatted is False:
                        self.Error("There is an incorrect dataType for the Thing with class: " + \
                        self.ValidateAndGet(prperty, "name", "root dataType: " + name))

                # add the dataType(s)
                for datatype in prperty["dataType"]:
                    propertyObject["dataType"].append(datatype)

                # add the Keywords
                if "keywords" in propertyObject:
                    self.ValidateAndGet(prperty, "keywords", "keywords of the root " \
                    + name + " => " + prperty["name"])
                    for keyword in prperty["keywords"]:
                        propertyObject["keywords"].append({
                            "keyword": self.ValidateAndGet(keyword, "keyword", "keyword" + name),
                            "weight": self.ValidateAndGet(keyword, "weight", "weight: " + name)
                        })

                # Delete if deleteIfFound is set
                if deleteIfFound == True:
                    self.Info("Delete: " + self.ValidateAndGet(prperty, "name", "name of " + name))
                    self.weaviate.Delete("/schema/" + name + "/" + \
                        self.ValidateAndGet(concept, "class", "classname of " + name) + \
                        "/properties/" + \
                        self.ValidateAndGet(prperty, "name", "name of " + name))

                # Update the class with the schema
                status, result = self.weaviate.Post("/schema/" + name + "/" + \
                self.ValidateAndGet(concept, "class", "classname of " + name) + \
                "/properties", propertyObject)

                if status != 200:
                    self.Error(str(result))

    def compareJSON(self, className, objectNeedle, objectStack):
        """This function compares the JSON of a remote Weaviate \
        agains local Things and Action files."""

        ##
        # https://github.com/semi-technologies/weaviate-cli/issues/9
        ##

        # loop over stack
        # for concept in objectStack:
        #     if concept["class"] == className:
        #         print(concept)
        #         print(objectNeedle)
        #         exit(0)

        return True

    def ValidateConceptClasses(self, things, actions):
        """This function validates if a concept is added correctly."""

        # On success return True
        success = True

        # Get the meta tags
        status, results = self.weaviate.Get("/meta")

        if status != 200:
            self.Error("Connection to Weaviate is lost.")

        # Loop over the results for things
        for remote in results["thingsSchema"]["classes"]:
            success = self.compareJSON(remote["class"], remote["properties"], things)

        # Loop over the results for actions
        for remote in results["actionsSchema"]["classes"]:
            success = self.compareJSON(remote["class"], remote["properties"], actions)

        #
        # COMPARRE result TO concepts
        #

        return success

    def SchemaCount(self):
        """Counts the things and actions in the schema"""
        _, conceptCount = self.weaviate.Get("/schema")

        return len(conceptCount["things"]["classes"]), len(conceptCount["actions"]["classes"])

    def Error(self, i):
        """This function produces an error and throws an exit 1."""

        print(datetime.datetime.now().isoformat() + " ERROR: " + i)
        exit(1)

    def Info(self, i):
        """This function produces an INFO statement."""

        print(datetime.datetime.now().isoformat() + " INFO: " + i)
