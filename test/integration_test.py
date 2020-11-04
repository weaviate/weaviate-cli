import unittest
import weaviate
from semi.commands.data import DataFileImporter


class IntegrationTest(unittest.TestCase):

    def test_import_data_from_file(self):
        client = weaviate.Client("http://localhost:8080")
        client.schema.delete_all()
        client.schema.create("./schema.json")

        importer = DataFileImporter(client, "./data.json", True)
        importer.load()