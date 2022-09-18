import unittest
import weaviate
from semi.data.commands import DataFileImporter


class IntegrationTest(unittest.TestCase):

    def test_import_data_from_file(self):
        current_path = '/'.join(__file__.split('/')[:-1])
        client = weaviate.Client("http://localhost:8080")
        client.schema.delete_all()
        client.schema.create(current_path + "/schema.json")

        importer = DataFileImporter(client,current_path + "/data.json", True)
        self.assertIsNone(importer.load())