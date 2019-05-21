#!python
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
from modules.Helpers import Helpers

def main():
    """This class reads the command line arguments and loads the correct modules."""

    # Get parsed arguments
    args = argparse.ArgumentParser(description=Messages().Get(112))

    # Get the arguments for sinit
    args.add_argument('--init', help=Messages().Get(100), action="store_true")
    args.add_argument('--init-url', default=None, help=Messages().Get(101))
    args.add_argument('--init-email', default=None, help=Messages().Get(101))

    # Get the arguments for schema import
    args.add_argument('--schema-import', help=Messages().Get(104), action="store_true")
    args.add_argument('--schema-import-ontology', default=None, help=Messages().Get(104))
    args.add_argument('--schema-import-overwrite', help=Messages().Get(107), action="store_true")

    # Get the arguments for schema export
    args.add_argument('--schema-export', help=Messages().Get(108), action="store_true")
    args.add_argument('--schema-export-things', default="./things.json", help=Messages().Get(109))
    args.add_argument('--schema-export-actions', default="/actions.json", help=Messages().Get(110))

    # truncate the schema
    args.add_argument('--schema-truncate', help=Messages().Get(126), action="store_true")
    args.add_argument('--schema-truncate-force', help=Messages().Get(127), action="store_true")

    # Empty a weaviate
    args.add_argument('--empty', help=Messages().Get(121), action="store_true")
    args.add_argument('--empty-force', help=Messages().Get(122), action="store_true")

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
        SchemaImport(config).Run(options.schema_import_ontology, options.schema_import_overwrite)
    elif options.schema_export is True:
        from modules.SchemaExport import SchemaExport
        SchemaExport(config).Run()
    elif options.empty is True:
        from modules.Empty import Empty
        Empty(config).Run(options.empty_force)
    elif options.schema_truncate is True:
        from modules.Truncate import Truncate
        Truncate(config).Run(options.schema_truncate_force)
    else:
        exit(0)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        Helpers(None).Info("\n" + Messages().Get(213))