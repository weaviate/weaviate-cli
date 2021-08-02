import unittest
from semi.commands.data import ValidateAndSplitData, dissect_reference

class TestCLI(unittest.TestCase):

    def test_dissect_reference(self):
        ref = [{'beacon': 'weaviate://localhost/b36268d4-a6b5-5274-985f-45f13ce0c642',
          'href': '/v1/b36268d4-a6b5-5274-985f-45f13ce0c642'},
         {'beacon': 'weaviate://localhost/1c9cd584-88fe-5010-83d0-017cb3fcb446',
          'href': '/v1/1c9cd584-88fe-5010-83d0-017cb3fcb446'}]

        dissected = dissect_reference(ref, 'Group', '2db436b5-0557-5016-9c5f-531412adf9c6', 'members')
        self.assertEqual(2, len(dissected))
        ref_1 = {
            "from_object_class_name": "Group",
            "from_object_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_object_uuid": "b36268d4-a6b5-5274-985f-45f13ce0c642"
        }
        self.assertIn(ref_1, dissected)
        ref_2 = {
            "from_object_class_name": "Group",
            "from_object_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_object_uuid": "1c9cd584-88fe-5010-83d0-017cb3fcb446"
        }

        self.assertIn(ref_2, dissected)

    def test_validate_and_split(self):
        schema = {
            'classes': [
                {
                    'class': 'Person',
                    'description': 'A person such as humans or personality known through culture',
                    'properties': [
                        {
                            'dataType': ['text'],
                            'description': 'The name of this person',
                            'name': 'name'
                        }
                    ]
                },
                {
                    'class': 'Group',
                    'description': 'A set of persons who are associated with each other over some common properties',
                    'properties': [
                        {
                            'dataType': ['text'],
                            'description': 'The name under which this group is known',
                            'name': 'name'
                        },
                        {
                            'dataType': ['Person'],
                            'description': 'The persons that are part of this group',
                            'name': 'members'
                        }
                    ]
                }
            ]
        }
        data = {
            "classes": [
                {
                    'class': 'Group',
                    'creationTimeUnix': 1604480914048,
                    'id': '2db436b5-0557-5016-9c5f-531412adf9c6',
                    'lastUpdateTimeUnix': 1604480914048,
                    'properties': {
                        'members': [
                            {
                                'beacon': 'weaviate://localhost/b36268d4-a6b5-5274-985f-45f13ce0c642',
                                'href': '/v1/b36268d4-a6b5-5274-985f-45f13ce0c642'
                            },
                            {
                                'beacon': 'weaviate://localhost/1c9cd584-88fe-5010-83d0-017cb3fcb446',
                                'href': '/v1/1c9cd584-88fe-5010-83d0-017cb3fcb446'
                            }
                        ],
                        'name': 'Legends'
                    },
                    'vectorWeights': None
                },
                {
                    'class': 'Person',
                    'creationTimeUnix': 1604480913954,
                    'id': '1c9cd584-88fe-5010-83d0-017cb3fcb446',
                    'lastUpdateTimeUnix': 1604480913954,
                    'properties': {
                        'name': 'Alan Turing'
                    },
                    'vectorWeights': None
                },
                {
                    'class': 'Person',
                    'creationTimeUnix': 1604480913597,
                    'id': 'b36268d4-a6b5-5274-985f-45f13ce0c642',
                    'lastUpdateTimeUnix': 1604480913597,
                    'properties': {
                        'name': 'John von Neumann'
                    },
                    'vectorWeights': None
                }
            ]
        }

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
        }
        self.assertIn(person_1, vas.data_objects)
        person_2 = {
            "class_name": "Person",
            "uuid": "b36268d4-a6b5-5274-985f-45f13ce0c642",
            "data_object": {
                "name": "John von Neumann"
            },
        }
        self.assertIn(person_2, vas.data_objects)
        group = {
            "class_name": "Group",
            "uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "data_object": {
                "name": "Legends"
            },
        }
        self.assertIn(group, vas.data_objects)

        ref_1 = {
            "from_object_class_name": "Group",
            "from_object_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_object_uuid": "b36268d4-a6b5-5274-985f-45f13ce0c642"
        }
        self.assertIn(ref_1, vas.data_references)
        ref_2 = {
            "from_object_class_name": "Group",
            "from_object_uuid": "2db436b5-0557-5016-9c5f-531412adf9c6",
            "from_property_name": "members",
            "to_object_uuid": "1c9cd584-88fe-5010-83d0-017cb3fcb446"
        }
        self.assertIn(ref_2, vas.data_references)
