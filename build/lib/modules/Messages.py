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

"""This is the module that handles all messages for the  Weaviate-cli tool."""

class Messages:
    """This class handles the Messages, both INFO and ERROR."""

    def __init__(self):
        """This function inits the message module."""
        self.messages = {}

    def infoMessage(self, no):
        """This function handles the INFO messages."""

        self.messages[100] = "Run the init function"
        self.messages[101] = "What is the path to the Weaviate?"
        self.messages[102] = "Provide a valid email address (only used when requested a sandbox)"
        self.messages[104] = "Run the schema import function. "
        self.messages[104] += "The ontology file should be in the same format as the"
        self.messages[104] += "/weaviate/v1/schema RESTful output"
        self.messages[105] = "What is the path to things?"
        self.messages[106] = "What is the path to actions?"
        self.messages[107] = "Overwrite classes and properties if found in Weaviate? Important: \
        ONLY use on a new Weaviate instance.\
        Will fail if things or actions are already in your Weaviate"
        self.messages[108] = "Run the schema export function"
        self.messages[109] = "What is the path to things?"
        self.messages[110] = "What is the path to actions?"
        self.messages[111] = "What is the path to the bulk data file?"
        self.messages[112] = "Weaviate Arguments"
        self.messages[113] = "Start ontology schema import"
        self.messages[114] = "Overwrite if found is TRUE. Checking if it is safe to proceed"
        self.messages[115] = "Create "
        self.messages[116] = "Add properties to "
        self.messages[117] = "Validate if the schemas are added correctly"
        self.messages[118] = "Succesfully imported the schema"
        self.messages[119] = "Created config file"
        self.messages[120] = "Config set"
        self.messages[121] = "Empty the Weaviate"
        self.messages[122] = "Force deletion"
        self.messages[123] = "Not emptying"
        self.messages[124] = "Are you sure? Press Y or run with --empty-force "
        self.messages[125] = "Succesfully deleted: " # followed by uuid
        self.messages[126] = "Truncate the schema, the Weaviate must be empty."
        self.messages[127] = "Force the truncation of the schema"
        self.messages[128] = "Do you want to truncate the schema? (y/n) "
        self.messages[129] = "Stopping truncating"
        self.messages[130] = "Provide a valid URL including schema and port if needed."
        self.messages[130] += "E.g. https://someurl:8080"
        self.messages[131] = "Provide a valid email address. Only used when requested a sandbox"
        self.messages[132] = "Provide a valid path or URL to the ontology schema json file"

        # Whoops, unknown message
        if no not in self.messages:
            return "UNKNOWN MESSAGE (" + str(no) + ").\
                Please report here: https://github.com/semi-technologies/weaviate-cli"

        return self.messages[no]

    def errorMessage(self, no):
        """This function handles the ERROR messages."""

        self.messages[200] = "test"
        self.messages[201] = "The things file can not be found. Check: "
        self.messages[202] = "The actions file can not be found. Check: "
        self.messages[203] = "Can't continue, Overwrite if found is TRUE but database isn't empty"
        self.messages[204] = "Schema wasn't added succesfully"
        self.messages[205] = "Can't write config file, do you have permission to write to: "
        self.messages[206] = "Can't find config file, did you create it?"
        self.messages[207] = "Can't empty the Weaviate. Unknown reason"
        self.messages[208] = "Can't create schema items because the schema is populated. "
        self.messages[208] += "Did you try --schema-truncate or --schema-import-overwrite? "
        self.messages[208] += "Use --help for more info"
        self.messages[209] = "The ontology file is not properly formatted, can't find: "
        self.messages[210] = "Cant't connect to the Weaviate."
        self.messages[210] += "Are your setting correct? Run with --init or --help to solve."
        self.messages[211] = "No valid URL is set."
        self.messages[212] = "No valid email is set."
        self.messages[213] = "Goodbye! See you @ www.semi.technology"
        self.messages[214] = "Can't run the import function! Did you point to the correct file?"
        self.messages[214] += "Is the file formatted correctly?"

        # Whoops, unknown message
        if no not in self.messages:
            return "UNKNOWN ERROR MESSAGE (" + str(no) + ").\
                Please report here: https://github.com/semi-technologies/weaviate-cli"

        return self.messages[no]

    def Get(self, no):
        """This function gets and returns a message."""

        if str(no)[:1] is "1":
            # Sending out an INFO message
            return self.infoMessage(no)
        elif str(no)[:1] is "2":
            # Sending out an INFO message
            return self.errorMessage(no)
        else:
            return "No value set for this message."
