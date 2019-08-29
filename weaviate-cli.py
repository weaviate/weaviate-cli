#!/usr/bin/env python3
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
import os
from modules.Init import Init
from modules.Weaviate import Weaviate
from modules.Messages import Messages
from modules.Helpers import Helpers

def main():
    """This class reads the command line arguments and loads the correct modules."""

    # Get parsed arguments
    args = argparse.ArgumentParser(description=Messages().Get(112))

    # Get the arguments for sinit
    #args.add_argument('init', help=Messages().Get(100), action='store_true')
    subparsers = args.add_subparsers()
    parser_init = subparsers.add_parser('init', help=Messages().Get(100))
    parser_init.add_argument('init', action='store_true')
    parser_init.add_argument('--url', default=None, help=Messages().Get(101))
    parser_init.add_argument('--email', default=None, help=Messages().Get(101))
    parser_init.add_argument('--auth-clientsecret', default=None, help=Messages().Get(137))

    # Get the arguments for schema import
    parser_schema_import = subparsers.add_parser('schema-import', help=Messages().Get(104))
    parser_schema_import.add_argument('schema-import', action='store_true')
    parser_schema_import.add_argument('--location', default=None, help=Messages().Get(104))
    parser_schema_import.add_argument('--force', help=Messages().Get(107), action='store_true')

    # Get the arguments for schema export
    parser_schema_export = subparsers.add_parser('schema-export', help=Messages().Get(108))
    parser_schema_export.add_argument('schema-export', action='store_true')
    parser_schema_export.add_argument('--location', default=None, help=Messages().Get(109))

    # truncate the schema
    parser_schema_truncate = subparsers.add_parser('schema-truncate', help=Messages().Get(126))
    parser_schema_truncate.add_argument('schema-truncate', action='store_true')
    parser_schema_truncate.add_argument('--force', help=Messages().Get(127), action='store_true')

    # Empty a weaviate
    parser_empty = subparsers.add_parser('empty', help=Messages().Get(121))
    parser_empty.add_argument('empty', action='store_true')
    parser_empty.add_argument('--force', help=Messages().Get(122), action='store_true')

    # Handle Sandboxes
    parser_sandboxCreate = subparsers.add_parser('sandbox-create', help=Messages().Get(143))
    parser_sandboxCreate.add_argument('sandbox-create', action='store_true')
    parser_sandboxCreate.add_argument('--email', help=Messages().Get(144), default=None)
    parser_sandboxCreate.add_argument('--asyncr', help=Messages().Get(144), action='store_true')
    parser_sandboxCreate.add_argument('--nodefault', help=Messages().Get(145), action='store_true')
    parser_sandboxCreate.add_argument('--replace', help=Messages().Get(149), action='store_true')

    parser_sandboxRemove = subparsers.add_parser('sandbox-remove', help=Messages().Get(146))
    parser_sandboxRemove.add_argument('sandbox-remove', action='store_true')
    parser_sandboxRemove.add_argument('--asyncr', help=Messages().Get(144), action='store_true')
    parser_sandboxRemove.add_argument('--nodefault', help=Messages().Get(145), action='store_true')

    parser_sandboxList = subparsers.add_parser('sandbox-list', help=Messages().Get(146))
    parser_sandboxList.add_argument('sandbox-list', action='store_true')
    parser_sandboxList.add_argument('--email', help=Messages().Get(144), default=None)


    # Ping a Weaviate
    parser_ping = subparsers.add_parser('ping', help=Messages().Get(140))
    parser_ping.add_argument('ping', action='store_true')

    # Show version
    parser_version = subparsers.add_parser('version', help=Messages().Get(142))
    parser_version.add_argument('version', action='store_true')

    options = args.parse_args()

    # Check init and validate if set
    if 'init' in options:
        Init().setConfig(options)
        exit(0)
    elif 'version' in options:
        with open(os.path.dirname(os.path.realpath(__file__))+"/version", "r") as fh:
            print(fh.read())
            exit(0)

    # Check which items to load
    if 'schema-import' in options:
        from modules.SchemaImport import SchemaImport
        # Ping Weaviate to validate the connection
        Weaviate(Init().loadConfig(False, None)).Ping()
        SchemaImport(Init().loadConfig(False, None)).Run(options.location, options.force)
    elif 'schema-export' in options:
        from modules.SchemaExport import SchemaExport
        # Ping Weaviate to validate the connection
        Weaviate(Init().loadConfig(False, None)).Ping()
        SchemaExport(Init().loadConfig(False, None)).Run()
    elif 'schema-truncate' in options:
        from modules.Truncate import Truncate
        # Ping Weaviate to validate the connection
        Weaviate(Init().loadConfig(False, None)).Ping()
        Truncate(Init().loadConfig(False, None)).Run(options.force)
    elif 'empty' in options:
        from modules.Empty import Empty
        # Ping Weaviate to validate the connection
        Weaviate(Init().loadConfig(False, None)).Ping()
        Empty(Init().loadConfig(False, None)).Run(options.force)
    elif 'sandbox-create' in options:
        from modules.Sandbox import Sandbox
        Sandbox(Init().loadConfig(True, options.email)).Run(True, False, options.nodefault, options.asyncr, options.replace)
    elif 'sandbox-remove' in options:
        from modules.Sandbox import Sandbox
        Sandbox(Init().loadConfig(False, None)).Run(False, True, options.nodefault, options.asyncr, None)
    elif 'sandbox-list' in options:
        from modules.Sandbox import Sandbox
        Sandbox(Init().loadConfig(True, options.email)).ListSandboxes(options.email)
    elif 'ping' in options:
        Weaviate(Init().loadConfig(False, None)).Ping()
    else:
        print(options)
        args.print_help()
        exit(0)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        Helpers(None).Info("\n" + Messages().Get(213))
