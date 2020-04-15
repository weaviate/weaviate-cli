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

"""This module handles the import of a data file in Weaviate."""
import json
import time
import validators
import urllib
from modules.Messages import Messages
import weaviate as client

class DataImport:
    """This class handles the import of a dataset."""

    def __init__(self, c):
        """This function inits the import module and sets the config."""
        from modules.Helpers import Helpers
        from modules.Weaviate import Weaviate
        self.config = c
        self.helpers = Helpers(c)
        self.weaviate = Weaviate(c)

    def __isPropertyOfBasicType(self, refprop):
        basic = False
        basicTypes = ['string', 'int', 'boolean', 'number', 'date', 'text', 'geoCoordinates', 'phoneNumber']
        if 'dataType' in refprop and len(refprop['dataType']) > 0:
            if refprop['dataType'][0] in basicTypes:
                basic = True
        return basic

    def __findPropertyInClass(self, propname, refclass):
        result = None
        if 'properties' in refclass:
            for item in refclass['properties']:
                if 'name' in item and item['name'] == propname:
                    result = item
                    break
        return result

    def __findReferenceClassInSchema(self, thing, schema):
        result = None
        if 'class' in thing and 'things' in schema and 'classes' in schema['things']:
            for item in schema['things']['classes']:
                if 'class' in item and item['class'] == thing['class']:
                    result = item
                    break

        if result is None:
            print("Did not find class", thing[key], "in schema; ignoring")
            return None
        if 'schema' not in thing:
            print("Did not find schema items in class", thing[key], "; Ignoring")
            return None

        return result

    def __ExtractUuidFromBeacon(self, beacon):
        result = beacon.split("/")
        return result[len(result)-1]

    def __ImportThings(self, things, schema):
        """This function imports a class in Weaviate """

        crossReferences = []
        # get the weaviate client
        wClient = client.Client(self.config['url'])
        if wClient is None:
            self.helpers.Error(Messages().Get(210))

        count = 0
        for thing in things:
            thingUuid = None
            #find the appropriate class in the schema
            refclass = self.__findReferenceClassInSchema(thing, schema)
            if refclass is not None:

                if 'id' in thing:
                    thingUuid = thing['id']
                # set all the fields for the new thing
                newthing = {}
                for key in thing['schema']:
                    refprop = self.__findPropertyInClass(key, refclass)
                    if refprop is None:
                        print("Did not find property", key, "in class; ignoring")
                    else:
                        #check if this is a reference property or a basic property
                        if self.__isPropertyOfBasicType(refprop):
                            # if this is a basic property, set the correct value
                            newthing[key] = thing['schema'][key]
                        else:
                            # if this is a reference property, collect the cross references
                            if thingUuid is not None:
                                for i in range(0, len(thing['schema'][key])):
                                    if 'beacon' in thing['schema'][key][i]:
                                        refUuid = self.__ExtractUuidFromBeacon(thing['schema'][key][i]['beacon'])
                                        crossref = {}
                                        crossref['from_thing_class_name'] = thing['class']
                                        crossref['from_thing_uuid'] = thingUuid
                                        crossref['from_property_name'] = key
                                        crossref['to_thing_uuid'] = refUuid
                                        crossReferences.append(crossref)

            wClient.create_thing(newthing, thing['class'], thingUuid)

        return crossReferences

    def __CrossReferenceThings(self, crossReferences):

        # get the weaviate client
        wClient = client.Client(self.config['url'])
        if wClient is None:
            self.helpers.Error(Messages().Get(210))

        count = 0
        for crossref in crossReferences:
            wClient.add_reference_to_thing(crossref['from_thing_uuid'], crossref['from_property_name'], crossref['to_thing_uuid'])

    def Run(self, dataFile):
        """This functions runs the import module."""

        # start the import
        self.helpers.Info(Messages().Get(160))

        # check if a schema is present
        schemaThingsCount, schemaActionsCount = self.helpers.SchemaCount()
        if schemaThingsCount == 0 and schemaActionsCount == 0:
            self.helpers.Error(Messages().Get(223))

        # get the schema
        status_code, schema = self.weaviate.Get("/schema")

        if status_code != 200:
            self.helpers.Error(Messages().Get(226))

        # check if data files is passed as argument
        if dataFile is None:
            dataFile = input(Messages().Get(161) + ": ")

        # open the datafile
        try:
            with open(dataFile, 'r') as file:
                data = json.load(file)
        except IOError:
            self.helpers.Error(Messages().Get(224) + schemaFile)

        # check if the data contains things and actions
        if 'actions' not in data:
            self.helpers.Error(Messages().Get(225) + "actions")
        if 'things' not in data:
            self.helpers.Error(Messages().Get(225) + "things")

        actions = data['actions']
        things = data['things']

        crossReferences = self.__ImportThings(things, schema)
        time.sleep(1.0)
        self.__CrossReferenceThings(crossReferences)
