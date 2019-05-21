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

"""This module empties a Weaviate."""
from modules.Messages import Messages

class Truncate:
    """This module processes the data import."""

    def __init__(self, c):
        """This function inits the module and sets the config."""

        # make the config available in this class
        from modules.Helpers import Helpers
        from modules.Weaviate import Weaviate
        self.config = c
        self.helpers = Helpers(c)
        self.weaviate = Weaviate(c)

    def truncatePropsAndClassses(self, propType):
        _, schema = self.weaviate.Get("/schema")
        self.helpers.Info("Deleting properties and " + propType)
        for singletonClass in schema[propType]["classes"]:
            statusCode = self.weaviate.Delete("/schema/" + propType + "/" + singletonClass["class"])
            if statusCode != 200:
                self.helpers.Error("Error while deleting property `" + singletonClass["class"] + "`. Error code: " + str(statusCode))

    def truncateSchema(self):

        # check if there is a schema
        thingCount, actionsCount = self.helpers.SchemaCount()
        if thingCount == 0 and actionsCount == 0:
            self.helpers.Info("No schema found, done")
            return

        # Delete properties
        self.truncatePropsAndClassses("actions")
        self.truncatePropsAndClassses("things")

    def Run(self, force):
        """This public function is the main Run command which runs this module."""
        if force == True:
            self.truncateSchema()
        else:
            var = input(Messages().Get(128))
            if var == "y" or var == "Y":
                self.truncateSchema()
            else:
                self.helpers.Info(Messages().Get(129))