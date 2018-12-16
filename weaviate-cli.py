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

"""This is the main module for the Weaviate-cli tool."""
import argparse
from modules.Init import Init
from modules.Weaviate import Weaviate
from modules.Messages import Messages

def main():
    """This class reads the command line arguments and loads the correct modules."""

    # Get parsed arguments
    args = argparse.ArgumentParser(description=Messages().Get(112))

    # Get the arguments for sinit
    args.add_argument('--init', help=Messages().Get(100), action="store_true")
    args.add_argument('--init-url', default="http://localhost", help=Messages().Get(101))
    args.add_argument('--init-key', default="UNSET", help=Messages().Get(102))
    args.add_argument('--init-token', default="UNSET", help=Messages().Get(103))

    # Get the arguments for schema import
    args.add_argument('--schema-import', help=Messages().Get(104), action="store_true")
    args.add_argument('--schema-import-things', default="./things.json", help=Messages().Get(105))
    args.add_argument('--schema-import-actions', default="/actions.json", help=Messages().Get(106))
    args.add_argument('--schema-import-delete', help=Messages().Get(107), action="store_true")

    # Get the arguments for schema export
    args.add_argument('--schema-export', help=Messages().Get(108), action="store_true")
    args.add_argument('--schema-export-things', default="./things.json", help=Messages().Get(109))
    args.add_argument('--schema-export-actions', default="/actions.json", help=Messages().Get(110))

    # Get the arguments for bulk import
    args.add_argument('--data-import', default="UNSET", help=Messages().Get(111))

    options = args.parse_args()

    # Check init and validate if set
    if options.init is True:
        Init().setConfig(options)

    # Set the config
    config = Init().loadConfig()

    # Ping Weaviate to validate the connection
    Weaviate(config).Ping()

    # Check which items to load
    if options.schema_import is True:
        from modules.SchemaImport import SchemaImport
        SchemaImport(config).Run(options.schema_import_things, \
                                  options.schema_import_actions, \
                                  options.schema_import_delete)
    elif options.schema_export is True:
        from modules.SchemaExport import SchemaExport
        SchemaExport(config).Run()
    elif options.data_import is True:
        from modules.DataImport import DataImport
        DataImport(config).Run()
    else:
        exit(0)

if __name__ == '__main__':
    main()
 