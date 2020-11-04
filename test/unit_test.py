import unittest
from semi.commands.misc import _parse_version_from_output
from semi.commands.data import ValidateAndSplitData, dissect_reference
import weaviate

class TestCLI(unittest.TestCase):

    def test_parse_version(self):
        out1 =  """
        Name: weaviate-cli
        Version: 0.1.0rc0
        Summary: Comand line interface to interact with weaviate
        Home-page: UNKNOWN
        Author: SeMI Technologies
        Author-email: hello@semi.technology
        License: UNKNOWN
        Location: /Users/felix/.virtualenvs/testing_cli_4/lib/python3.7/site-packages
        Requires: click, weaviate-client
        Required-by:
        """

        self.assertEqual("0.1.0rc0", _parse_version_from_output(out1))

        out2 = "WARNING: Package(s) not found: weaviate-cli"
        a = "The installed cli version can not be assessed! Run `pip show weaviate-cli` to view the version manually"
        self.assertEqual(a, _parse_version_from_output(out2))

    def test_dissect_reference(self):
        ref = [{'beacon': 'weaviate://localhost/things/b36268d4-a6b5-5274-985f-45f13ce0c642',
          'href': '/v1/things/b36268d4-a6b5-5274-985f-45f13ce0c642'},
         {'beacon': 'weaviate://localhost/things/1c9cd584-88fe-5010-83d0-017cb3fcb446',
          'href': '/v1/things/1c9cd584-88fe-5010-83d0-017cb3fcb446'}]

        dissected = dissect_reference(ref, 'things', 'Group', '2db436b5-0557-5016-9c5f-531412adf9c6', 'members')
        self.assertEqual(2, len(dissected))
        ref_1 = {
            "from_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "from_thing_class_name": "Group",
            "from_thing_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "to_thing_uuid": "b36268d4-a6b5-5274-985f-45f13ce0c642"
        }
        self.assertIn(ref_1, dissected)
        ref_2 = {
            "from_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "from_thing_class_name": "Group",
            "from_thing_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "to_thing_uuid": "1c9cd584-88fe-5010-83d0-017cb3fcb446"
        }

        self.assertIn(ref_2, dissected)

    def test_validate_and_split(self):
        schema = {'actions': {'classes': [], 'type': 'action'}, 'things': {'classes': [{'class': 'Person', 'description': 'A person such as humans or personality known through culture', 'properties': [{'dataType': ['text'], 'description': 'The name of this person', 'name': 'name'}]}, {'class': 'Group', 'description': 'A set of persons who are associated with each other over some common properties', 'properties': [{'dataType': ['text'], 'description': 'The name under which this group is known', 'name': 'name'}, {'dataType': ['Person'], 'description': 'The persons that are part of this group', 'name': 'members'}]}], 'type': 'thing'}}
        data = {"things": [{'class': 'Group', 'creationTimeUnix': 1604480914048, 'id': '2db436b5-0557-5016-9c5f-531412adf9c6', 'lastUpdateTimeUnix': 1604480914048, 'schema': {'members': [{'beacon': 'weaviate://localhost/things/b36268d4-a6b5-5274-985f-45f13ce0c642', 'href': '/v1/things/b36268d4-a6b5-5274-985f-45f13ce0c642'}, {'beacon': 'weaviate://localhost/things/1c9cd584-88fe-5010-83d0-017cb3fcb446', 'href': '/v1/things/1c9cd584-88fe-5010-83d0-017cb3fcb446'}], 'name': 'Legends'}, 'vectorWeights': None}, {'class': 'Person', 'creationTimeUnix': 1604480913954, 'id': '1c9cd584-88fe-5010-83d0-017cb3fcb446', 'lastUpdateTimeUnix': 1604480913954, 'schema': {'name': 'Alan Turing'}, 'vectorWeights': None}, {'class': 'Person', 'creationTimeUnix': 1604480913597, 'id': 'b36268d4-a6b5-5274-985f-45f13ce0c642', 'lastUpdateTimeUnix': 1604480913597, 'schema': {'name': 'John von Neumann'}, 'vectorWeights': None}]}

        vas = ValidateAndSplitData(data, schema)
        vas.validate_and_split()

        self.assertEqual(3, len(vas.data_objects))
        self.assertEqual(2, len(vas.data_references))

        person_1 = {
            "class_name": "Person",
            "uuid": "1c9cd584-88fe-5010-83d0-017cb3fcb446",
            "data_object": {
                "name": "Alan Turing"
            },
            "semantic_type": weaviate.SEMANTIC_TYPE_THINGS
        }
        self.assertIn(person_1, vas.data_objects)
        person_2 = {
            "class_name": "Person",
            "uuid": "b36268d4-a6b5-5274-985f-45f13ce0c642",
            "data_object": {
                "name": "John von Neumann"
            },
            "semantic_type": weaviate.SEMANTIC_TYPE_THINGS
        }
        self.assertIn(person_2, vas.data_objects)
        group = {
            "class_name": "Group",
            "uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "data_object": {
                "name": "Legends"
            },
            "semantic_type": weaviate.SEMANTIC_TYPE_THINGS
        }
        self.assertIn(group, vas.data_objects)

        ref_1 = {
            "from_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "from_thing_class_name": "Group",
            "from_thing_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "to_thing_uuid": "b36268d4-a6b5-5274-985f-45f13ce0c642"
        }
        self.assertIn(ref_1, vas.data_references)
        ref_2 = {
            "from_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "from_thing_class_name": "Group",
            "from_thing_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_semantic_type": weaviate.SEMANTIC_TYPE_THINGS,
            "to_thing_uuid": "1c9cd584-88fe-5010-83d0-017cb3fcb446"
        }
        self.assertIn(ref_2, vas.data_references)
