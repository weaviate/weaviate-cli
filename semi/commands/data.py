from semi.config.configuration import Configuration
from semi.prompt import is_question_answer_yes
import weaviate
import weaviate.tools
import json


def delete_all_data(cfg:Configuration, force):
    if force:
        _delete_all(cfg.client)
        exit()
    if not is_question_answer_yes("Do you really want to delete all data?"):
        exit()
    _delete_all(cfg.client)


def _delete_all(client):
    schema = client.schema.get()
    client.schema.delete_all()
    client.schema.create(schema)


def import_data_from_file(cfg:Configuration, file: str, fail_on_error: bool):
    importer = DataFileImporter(cfg.client, file, fail_on_error)
    importer.load()


class DataFileImporter:

    def __init__(self, client: weaviate.Client, data_path: str, fail_on_error: bool):
        self.client = client
        self.fail_on_error = fail_on_error
        # callback = None
        # if fail_on_error:
        #     callback = _exit_on_error
        self.batcher = weaviate.tools.Batcher(client, return_values_callback=self._exit_on_error)

        with open(data_path, 'r') as data_io:
            self.data = json.load(data_io)

    def _exit_on_error(self, batch_results):
        for entry in batch_results:
            result = entry.get('result', {})
            error = result.get('errors')
            if error is not None:
                print(error)
                if self.fail_on_error:
                    exit(1)

    def load(self):
        schema = self.client.schema.get()
        print("Validating data")
        vas = ValidateAndSplitData(self.data, schema)
        vas.validate_and_split()
        print("Importing data")
        for obj in vas.data_objects:
            self.batcher.add_data_object(**obj)
        for ref in vas.data_references:
            self.batcher.add_reference(**ref)
        self.batcher.close()

class ValidateAndSplitData:

    def __init__(self, data, schema):
        self.data = data
        self.schema = dissect_schema(schema)
        self.data_objects = []
        self.data_references = []

    def validate_and_split(self):
        """ Go through the entire data and validate it agains a schema
            if not valid exit with error
            if valid split it into the primitive object and the references

        :return:
        """
        for obj in self.data.get("things", []):
            self._validate_obj(obj, weaviate.SEMANTIC_TYPE_THINGS)
        for obj in self.data.get("actions", []):
            self._validate_obj(obj, weaviate.SEMANTIC_TYPE_THINGS)

    def _validate_obj(self, obj, semantic_type):
        obj_class_name = obj.get('class')
        schema_definition = self.schema.get(obj_class_name)
        id = obj.get('id')

        # Check if class exists
        if schema_definition is None:
            _exit_validation_failed(f"Class {obj_class_name} not in schema!")

        import_object_parameter = {
            'class_name': obj_class_name,
            'uuid': id,
            'data_object': {},
            'semantic_type': semantic_type,
        }

        for obj_property_name, obj_property_val in obj.get('schema', {}).items():
            if obj_property_name in schema_definition['primitive']:
                # property is primitive -> add to data import list
                import_object_parameter['data_object'][obj_property_name] = obj_property_val
            elif obj_property_name in schema_definition['ref']:
                # property is reference to a different object
                # convert property into batch request parameters
                ref_parameters = dissect_reference(obj_property_val, semantic_type, obj_class_name, id, obj_property_name)
                self.data_references += ref_parameters
            else:
                _exit_validation_failed(f"Property {obj_property_name} of class {obj_class_name} not in schema!")
        self.data_objects.append(import_object_parameter)

def _exit_validation_failed(reason):
    print("Error validation failed:", reason)
    exit(1)


def dissect_reference(refs: list, from_type, from_class, from_id, from_prop):
    """ Disect a reference list into the parametes required for a batch request

    :param refs:
    :param from_type:
    :param from_class:
    :param from_id:
    :param from_prop:
    :return:
    """
    result = []
    for ref in refs:
        beacon_split = ref.get('beacon', '/e/e/e/e').split('/')
        ref_batch_parameters = {
            "from_semantic_type": from_type,
            "from_thing_class_name": from_class,
            "from_thing_uuid": from_id,
            "from_property_name": from_prop,
            "to_semantic_type": beacon_split[-2],
            "to_thing_uuid": beacon_split[-1]
        }
        result.append(ref_batch_parameters)
    return result

def dissect_schema(schema):
    """ Dissect the schema into a dict listing all classes with their name as key to have faster validation access

    :param schema: the schema as exported from weaviate
    :return:
    """
    dissected = {
    }
    for c in schema.get('things', {}).get('classes', []):
        prim, ref = _get_schema_properties(c['properties'])
        dissected[c['class']] = {
            'type': weaviate.SEMANTIC_TYPE_THINGS,
            'primitive': prim,
            'ref': ref,
        }
    for c in schema.get('actions', {}).get('classes', []):
        prim, ref = _get_schema_properties(c['properties'])
        dissected[c['class']] = {
            'type': weaviate.SEMANTIC_TYPE_ACTIONS,
            'primitive': prim,
            'ref': ref,
        }
    return dissected


def _get_schema_properties(properties: list):
    """ Split properties into references and primmitive types

    :param properties:
    :return: two list of property names one for primitives one for references
    """
    properties_primitive = []
    properties_reference = []
    for property in properties:
        if is_primitive_prop(property["dataType"][0]):
            properties_primitive.append(property['name'])
        else:
            properties_reference.append(property['name'])
    return (properties_primitive, properties_reference)


def is_primitive_prop(data_type:str):
    return data_type in ['text', 'string', 'int', 'boolean', 'number', 'date', 'geoCoordinates', 'phoneNumber']
