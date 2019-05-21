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

class Empty:
    """This module processes the data import."""

    def __init__(self, c):
        """This function inits the module and sets the config."""

        # make the config available in this class
        from modules.Helpers import Helpers
        from modules.Weaviate import Weaviate
        self.config = c
        self.helpers = Helpers(c)
        self.weaviate = Weaviate(c)

    def emptyWeaviate(self, conceptType):
        
        counter = 0
        # Get all concepts
        _, result = self.weaviate.Get("/" + conceptType)

        # check if there are concepts
        if "totalResults" in result:

            # loop and delete
            for concept in result[conceptType]:

                # delete the concept
                statusCode = self.weaviate.Delete("/" + conceptType + "/" + concept["id"])
                # validate if valid
                if statusCode != 204:
                    self.helpers.Error(Messages().Get(207))
                else:
                    self.helpers.Info(Messages().Get(125) + concept["id"])

            # restart the function
            self.emptyWeaviate(conceptType)

    def Run(self, force):
        """This public function is the main Run command which runs this module."""
        if force == True:
            self.emptyWeaviate("things")
            self.emptyWeaviate("actions")
        else:
            var = input(Messages().Get(124))
            if var == "y" or var == "Y":
                self.emptyWeaviate("things")
                self.emptyWeaviate("actions")
            else:
                self.helpers.Info(Messages().Get(123))