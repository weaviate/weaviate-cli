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

"""This module handles the export of a Weaviate schema."""
import os
import json


class SchemaExport:
    """This class handles the export of a Weaviate schema."""

    def __init__(self, c):
        """This function inits the export of schema module."""

        from modules.Helpers import Helpers
        from modules.Weaviate import Weaviate
        self.config = c
        self.helpers = Helpers(c)
        self.weaviate = Weaviate(c)

    # run schema export
    def Run(self):
        """This module handles the import of a Weaviate instance."""

        status_code, schema = self.weaviate.Get("/schema")

        if status_code != 200:
            print("Schema loading did not succeed")
            print(schema)

        file_name = 'weaviate_schema_export'
        schema_file = file_name + '.json'
        i = 1

        while os.path.isfile(schema_file):
            schema_file = file_name+str(i)+'.json'
            i += 1

        json.dump(schema, open(schema_file, "w"))

        print("Schema succesfully exported to " + schema_file)
        exit(0)
