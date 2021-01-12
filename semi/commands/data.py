import json
import sys
import weaviate
import weaviate.tools
from semi.config.configuration import Configuration
from semi.prompt import is_question_answer_yes


def delete_all_data(cfg: Configuration, force: bool) -> None:
    """
    Delete all weaviate objects.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    force : bool
        If True force delete all objects, if False ask for permision.
    """

    if force:
        _delete_all(cfg.client)
        sys.exit()
    if not is_question_answer_yes("Do you really want to delete all data?"):
        sys.exit()
    _delete_all(cfg.client)


def _delete_all(client: weaviate.Client):
    """
    Delete all weaviate data.

    Parameters
    ----------
    client : weaviate.Client
        A weaviate client.
    """

    schema = client.schema.get()
    client.schema.delete_all()
    client.schema.create(schema)


def import_data_from_file(cfg: Configuration, file: str, fail_on_error: bool) -> None:
    """
    Import data from a file.

    Parameters
    ----------
    cfg : Configuration
        A CLI configuration.
    file : str
        The data file path.
    fail_on_error : bool
        # TODO:
    """
    importer = DataFileImporter(cfg.client, file, fail_on_error)
    importer.load()


class DataFileImporter:

    def __init__(self, client: weaviate.Client, data_path: str, fail_on_error: bool):
        """
        Initialize a DataFileImporter.

        Parameters
        ----------
        client : weaviate.Client
            A weaviate client.
        data_path : str
            The data file path.
        fail_on_error : bool
            If True exits at the first error, if False prints the error only.
        """

        self.client = client
        self.fail_on_error = fail_on_error
        self.batcher = weaviate.tools.Batcher(client, return_values_callback=self._exit_on_error)

        with open(data_path, 'r') as data_io:
            self.data = json.load(data_io)

    def _exit_on_error(self, batch_results: list):
        """
        Exit if an error occured.

        Parameters
        ----------
        batch_results : list
            weaviate batch create results.
        """

        for entry in batch_results:
            result = entry.get('result', {})
            error = result.get('errors')
            if error is not None:
                print(error)
                if self.fail_on_error:
                    sys.exit(1)

    def load(self) -> None:
        """
        Load data into weaviate.
        """

        schema = self.client.schema.get()
        print("Validating data")
        vasd = ValidateAndSplitData(self.data, schema)
        vasd.validate_and_split()
        print("Importing data")
        for obj in vasd.data_objects:
            self.batcher.add_data_object(**obj)
        for ref in vasd.data_references:
            self.batcher.add_reference(**ref)
        self.batcher.close()

class ValidateAndSplitData:

    def __init__(self, data: dict, schema: dict):
        """
        Initialize a ValidateAndSplitData class instance.

        Parameters
        ----------
        data : dict
            The objects to be validated.
        schema : dict
            The schema against which to validate the objects.
        """

        self.data = data
        self.schema = dissect_schema(schema)
        self.data_objects = []
        self.data_references = []

    def validate_and_split(self) -> None:
        """ 
        Go through the entire data and validate it against a schema
        if not valid exit with error, if valid split it into the 
        primitive object and the references.
        """

        for obj in self.data.get("classes", []):
            self._validate_obj(obj)

    def _validate_obj(self, obj):
        obj_class_name = obj.get('class')
        schema_definition = self.schema.get(obj_class_name)
        object_id = obj.get('id')

        # Check if class exists
        if schema_definition is None:
            _exit_validation_failed(f"Class {obj_class_name} not in schema!")

        import_object_parameter = {
            'class_name': obj_class_name,
            'uuid': object_id,
            'data_object': {},
        }

        for obj_property_name, obj_property_val in obj.get('properties', {}).items():
            if obj_property_name in schema_definition['primitive']:
                # property is primitive -> add to data import list
                import_object_parameter['data_object'][obj_property_name] = obj_property_val
            elif obj_property_name in schema_definition['ref']:
                # property is reference to a different object
                # convert property into batch request parameters
                ref_parameters = dissect_reference(obj_property_val, obj_class_name, object_id, obj_property_name)
                self.data_references += ref_parameters
            else:
                _exit_validation_failed(f"Property {obj_property_name} of class {obj_class_name} not in schema!")
        self.data_objects.append(import_object_parameter)

def _exit_validation_failed(reason: str):
    """
    Exit if validation failed.

    Parameters
    ----------
    reason : str
        Message to print.
    """

    print("Error validation failed:", reason)
    sys.exit(1)


def dissect_reference(refs: list, from_class: str, from_id: str, from_prop: str) -> list:
    """
    Dissect a reference list into the parametes required for a batch request.

    Parameters
    ----------
    refs : list
        A list of references to be dissected.
    from_class : str
        The object's class name. 
    from_id : str
        The id of the object.
    from_prop : str
        The property name.

    Returns
    -------
    list
        A list of batcher parameters used to upload data.
    """

    result = []
    for ref in refs:
        beacon_split = ref.get('beacon', 'e').split('/')
        ref_batch_parameters = {
            "from_object_class_name": from_class,
            "from_object_uuid": from_id,
            "from_property_name": from_prop,
            "to_object_uuid": beacon_split[-1]
        }
        result.append(ref_batch_parameters)
    return result

def dissect_schema(schema: dict) -> dict:
    """ 
    Dissect the schema into a dict listing all classes with their name as key to have faster
    validation access.

    Parameters
    ----------
    schema : dict
        The schema as exported from weaviate

    Returns
    -------
    dict
        A dict with each class and separated primitive and complex properties.
    """
    dissected = {
    }
    for class_ in schema.get('classes', []):
        prim, ref = _get_schema_properties(class_['properties'])
        dissected[class_['class']] = {
            'primitive': prim,
            'ref': ref,
        }
    return dissected


def _get_schema_properties(properties: list) -> tuple:
    """
    Split properties into references and primmitive types.

    Parameters
    ----------
    properties : list
         A list of class properties.

    Returns
    -------
    tuple
        A tuple of two lists of property names one for primitives one for references.
    """

    properties_primitive = []
    properties_reference = []
    for property_ in properties:
        if is_primitive_prop(property_["dataType"][0]):
            properties_primitive.append(property_['name'])
        else:
            properties_reference.append(property_['name'])
    return (properties_primitive, properties_reference)


def is_primitive_prop(data_type: str) -> bool:
    """
    Check if property is a primitive one.

    Parameters
    ----------
    data_type : str
        Property data type.

    Returns
    -------
    bool
        True if 'data_type' is of a primitive property.
    """

    return data_type in ['text', 'string', 'int', 'boolean', 'number', 'date', 'geoCoordinates', 'phoneNumber']
