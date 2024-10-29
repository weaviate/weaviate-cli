import click
import json
import numpy as np
import random
import os
from importlib import resources
from weaviate_cli.utils import get_random_string, pp_objects
from weaviate import WeaviateClient
from weaviate.classes.query import MetadataQuery
from weaviate.collections.classes.tenants import TenantActivityStatus
from typing import Dict, List, Optional, Union
import weaviate.classes.config as wvc
from weaviate.collections import Collection
from datetime import datetime, timedelta


class DataManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def __import_json(self, collection: Collection, file_name: str, cl: wvc.ConsistencyLevel, num_objects: Optional[int] = None) -> int:
        counter = 0
        properties: List[wvc.Property] = collection.config.get().properties
        
        try:
            # Use importlib.resources to access the file from the package
            with resources.files('weaviate_cli.datasets').joinpath(file_name).open('r') as f:
                data = json.load(f)
                cl_collection: Collection = collection.with_consistency_level(cl)
                with cl_collection.batch.dynamic() as batch:
                    for obj in data[:num_objects] if num_objects else data:
                        added_obj = {}
                        for prop in properties:
                            if prop.name == "release_date":
                                prop.name = "releaseDate"
                            if prop.name in obj:
                                if prop.data_type == wvc.DataType.NUMBER:
                                    added_obj[prop.name] = float(obj[prop.name])
                                elif prop.data_type == wvc.DataType.DATE:
                                    date = datetime.strptime(obj[prop.name], "%Y-%m-%d")
                                    added_obj[prop.name] = date.strftime("%Y-%m-%dT%H:%M:%SZ")
                                else:
                                    added_obj[prop.name] = obj[prop.name]
                        batch.add_object(properties=added_obj)
                        counter += 1

                if cl_collection.batch.failed_objects:
                    for failed_object in cl_collection.batch.failed_objects:
                        print(f"Failed to add object with UUID {failed_object.original_uuid}: {failed_object.message}")
                    return -1

                expected: int = len(data[:num_objects]) if num_objects else len(data)
                assert counter == expected, f"Expected {expected} objects, but added {counter} objects."
                
        except Exception as e:
            print(f"Error loading data file: {str(e)}")
            return -1
            
        print(f"Finished processing {counter} objects.")
        return counter

    def __generate_data_object(self, limit: int, is_update: bool = False) -> Union[List[Dict], Dict]:
        def create_single_object() -> Dict:
            date = datetime.strptime("1980-01-01", "%Y-%m-%d")
            random_date = date + timedelta(days=random.randint(1, 15_000))
            release_date = random_date.strftime("%Y-%m-%dT%H:%M:%SZ")

            prefix = "update-" if is_update else ""
            return {
                "title": f"{prefix}title" + get_random_string(10),
                "genres": f"{prefix}genre" + get_random_string(3),
                "keywords": f"{prefix}keywords" + get_random_string(3), 
                "popularity": float(random.randint(1, 200)),
                "runtime": f"{prefix}runtime" + get_random_string(3),
                "cast": f"{prefix}cast" + get_random_string(3),
                "originalLanguage": f"{prefix}language" + get_random_string(3),
                "tagline": f"{prefix}tagline" + get_random_string(3),
                "budget": random.randint(1_000_000, 1_000_0000_000),
                "releaseDate": release_date,
                "revenue": random.randint(1_000_000, 10_000_0000_000),
                "status": f"{prefix}status" + get_random_string(3),
            }

        if is_update:
            return create_single_object()
            
        return [create_single_object() for _ in range(limit)]

    def __ingest_data(self, collection: Collection, num_objects: int, cl: wvc.ConsistencyLevel, randomize: bool) -> int:
        if randomize:
            counter = 0
            data_objects = self.__generate_data_object(num_objects)
            cl_collection = collection.with_consistency_level(cl)
            vectorizer = cl_collection.config.get().vectorizer
            dimensions = 1536
            if vectorizer == "text2vec-contextionary":
                dimensions = 300
            elif vectorizer == "text2vec-transformers":
                dimensions = 768
            with cl_collection.batch.dynamic() as batch:
                for obj in data_objects:
                    batch.add_object(
                        properties=obj,
                        vector=np.random.rand(1, dimensions)[0].tolist(),
                    )
                    counter += 1

            if cl_collection.batch.failed_objects:
                for failed_object in cl_collection.batch.failed_objects:
                    print(
                        f"Failed to add object with UUID {failed_object.original_uuid}: {failed_object.message}"
                    )
                return -1
            print(f"Inserted {counter} objects into class '{collection.name}'")
            return counter
        else:
            num_objects_inserted = self.__import_json(
                collection, "movies.json", cl, num_objects
            )
            print(f"Inserted {num_objects_inserted} objects into class '{collection.name}'")
            return num_objects_inserted


    def ingest_data(self, collection: Optional[str], limit: int, consistency_level: str, randomize: bool, auto_tenants: int) -> None:

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class> command"
            )

        col: Collection = self.client.collections.get(collection)
        try:
            tenants = [key for key in col.tenants.get().keys()]
        except Exception as e:
            # Check if the error is due to multi-tenancy being disabled
            if "multi-tenancy is not enabled" in str(e):
                click.echo(
                    f"Collection '{col.name}' does not have multi-tenancy enabled. Skipping tenant information collection."
                )
                tenants = ["None"]
            else:
                raise e
        if (
            auto_tenants > 0
            and col.config.get().multi_tenancy_config.auto_tenant_creation is False
        ):

            raise Exception(
                f"Auto tenant creation is not enabled for class '{col.name}'. Please enable it using <update class> command"
            )

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }
        if auto_tenants > 0:
            if tenants == "None":
                tenants = [f"Tenant--{i}" for i in range(1, auto_tenants + 1)]
            else:
                if len(tenants) < auto_tenants:
                    tenants += [
                        f"Tenant--{i}" for i in range(len(tenants) + 1, auto_tenants + 1)
                    ]

        for tenant in tenants:
            if tenant == "None":
                ret = self.__ingest_data(
                    col,
                    limit,
                    cl_map[consistency_level],
                    randomize,
                )
            else:
                click.echo(f"Processing tenant '{tenant}'")
                ret = self.__ingest_data(
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                    randomize,
                )

            if ret == -1:

                raise Exception(
                    f"Error occurred while ingesting data for tenant '{tenant}'."
                )


    def __update_data(self, collection: Collection, num_objects: int, cl: wvc.ConsistencyLevel, randomize: bool) -> int:
        if randomize:
            res = collection.query.fetch_objects(limit=num_objects)
            if len(res.objects) == 0:
                print(
                    f"No objects found in class '{collection.name}'. Insert objects first using ./ingest_data.py"
                )
                return -1
            data_objects = res.objects
            for obj in data_objects:
                res = collection.with_consistency_level(cl).data.replace(
                    uuid=obj.uuid,
                    properties=self.__generate_data_object(1, True),
                    vector={"default": np.random.rand(1, 1536)[0].tolist()},
                )
            found_objects = len(data_objects)
            assert (
                found_objects == num_objects
            ), f"Found {found_objects} objects, expected {num_objects}"
            print(f"Updated {found_objects} objects into class '{collection.name}'")
            return found_objects
        else:
            res = collection.query.fetch_objects(limit=num_objects)
            if len(res.objects) == 0:
                print(
                    f"No objects found in class '{collection.name}'. Insert objects first using ./ingest_data.py"
                )
                return -1
            data_objects = res.objects
            for obj in data_objects:
                for property, value in obj.properties.items():
                    if isinstance(value, str):
                        obj.properties[property] = "updated-" + value
                    elif isinstance(value, int):
                        obj.properties[property] += 1
                    elif isinstance(value, float):
                        obj.properties[property] += 1.0
                    elif isinstance(value, datetime):
                        obj.properties[property] = value + timedelta(days=1)
                res = collection.with_consistency_level(cl).data.update(
                    uuid=obj.uuid,
                    properties=obj.properties,
                )
            found_objects = len(data_objects)
            assert (
                found_objects == num_objects
            ), f"Found {found_objects} objects, expected {num_objects}"
            print(f"Updated {num_objects} objects into class '{collection.name}'")
            return found_objects


    def update_data(self, collection: str, limit: int, consistency_level: str, randomize: bool) -> None:

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using ./create_class.py"
            )

        col : Collection = self.client.collections.get(collection)
        try:
            tenants = [key for key in col.tenants.get().keys()]
        except Exception as e:
            # Check if the error is due to multi-tenancy being disabled
            if "multi-tenancy is not enabled" in str(e):
                click.echo(
                    f"Collection '{col.name}' does not have multi-tenancy enabled. Skipping tenant information collection."
                )
                tenants = ["None"]

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }

        for tenant in tenants:
            if tenant == "None":
                ret = self.__update_data(
                    col,
                    limit,
                    cl_map[consistency_level],
                    randomize,
                )
            else:
                print(f"Processing tenant '{tenant}'")
                ret = self.__update_data(
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                    randomize,
                )
            if ret == -1:

                raise Exception(
                    f"Failed to update objects in class '{col.name}' for tenant '{tenant}'"
                )

    def __delete_data(self, collection: Collection, num_objects: int, cl: wvc.ConsistencyLevel) -> int:

        res = collection.query.fetch_objects(limit=num_objects)
        if len(res.objects) == 0:
            print(
                f"No objects found in class '{collection.name}'. Insert objects first using <ingest data> command"
            )
            return -1
        data_objects = res.objects

        for obj in data_objects:
            collection.with_consistency_level(cl).data.delete_by_id(uuid=obj.uuid)

        print(f"Deleted {num_objects} objects into class '{collection.name}'")
        return num_objects


    def delete_data(self, collection: str, limit: int, consistency_level: str) -> None:

        if not self.client.collections.exists(collection):
            print(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class> command."
            )

            return 1

        col : Collection = self.client.collections.get(collection)
        try:
            tenants = [key for key in col.tenants.get().keys()]
        except Exception as e:
            # Check if the error is due to multi-tenancy being disabled
            if "multi-tenancy is not enabled" in str(e):
                click.echo(
                    f"Collection '{col.name}' does not have multi-tenancy enabled. Skipping tenant information collection."
                )
                tenants = ["None"]

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }

        for tenant in tenants:
            if tenant == "None":
                ret = self.__delete_data(col, limit, cl_map[consistency_level])
            else:
                click.echo(f"Processing tenant '{tenant}'")
                ret = self.__delete_data(
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                )
            if ret == -1:

                raise Exception(
                    f"Failed to delete objects in class '{col.name}' for tenant '{tenant}'"
                )

    def __query_data(self, collection: Collection, num_objects: int, cl: wvc.ConsistencyLevel, search_type: str, query: str, properties: str) -> None:

        start_time = datetime.now()
        response = None
        if search_type == "fetch":
            # Fetch logic
            response = collection.with_consistency_level(cl).query.fetch_objects(
                limit=num_objects
            )
        elif search_type == "vector":
            # Vector logic
            response = collection.with_consistency_level(cl).query.near_text(
                query=query,
                return_metadata=MetadataQuery(distance=True, certainty=True),
                limit=num_objects,
            )
        elif search_type == "keyword":
            # Keyword logic
            response = collection.with_consistency_level(cl).query.bm25(
                query=query,
                return_metadata=MetadataQuery(score=True, explain_score=True),
                limit=num_objects,
            )
        elif search_type == "hybrid":
            # Hybrid logic
            response = collection.with_consistency_level(cl).query.hybrid(
                query=query, return_metadata=MetadataQuery(score=True), limit=num_objects
            )
        else:
            click.echo(
                f"Invalid search type: {search_type}. Please choose from 'fetch', 'vector', 'keyword', or 'hybrid'."
            )
            return -1

        if response is not None:
            properties_list = [prop.strip() for prop in properties.split(",")]
            pp_objects(response, properties_list)
        else:
            click.echo("No objects found")
            return -1
        end_time = datetime.now()
        latency = end_time - start_time

        print(
            f"Queried {num_objects} objects using {search_type} search into class '{collection.name}' in {latency.total_seconds()} s"
        )
        return num_objects


    def query_data(
        self, collection: str, search_type: str, query: str, consistency_level: str, limit: int, properties: str
    ) -> None:

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class> command."
            )

        col : Collection = self.client.collections.get(collection)
        try:
            tenants = [
                key
                for key, tenant in col.tenants.get().items()
                if tenant.activity_status == TenantActivityStatus.ACTIVE
            ]
        except Exception as e:
            # Check if the error is due to multi-tenancy being disabled
            if "multi-tenancy is not enabled" in str(e):
                click.echo(
                    f"Collection '{col.name}' does not have multi-tenancy enabled. Skipping tenant information collection."
                )
                tenants = ["None"]

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }

        for tenant in tenants:
            if tenant == "None":
                ret = self.__query_data(
                    col,
                    limit,
                    cl_map[consistency_level],
                    search_type,
                    query,
                    properties,
                )
            else:
                print(f"Querying tenant '{tenant}'")
                ret = self.__query_data(
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                    search_type,
                    query,
                    properties,
                )
            if ret == -1:

                raise Exception(
                    f"Failed to query objects in class '{col.name}' for tenant '{tenant}'"
                )
