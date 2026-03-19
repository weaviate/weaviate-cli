import json
from typing import Optional
from weaviate import WeaviateClient
from weaviate_cli.defaults import (
    GetShardsDefaults,
    UpdateShardsDefaults,
)
import click


class ShardManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def get_shards(
        self,
        collection: Optional[str] = GetShardsDefaults.collection,
        json_output: bool = False,
    ) -> None:
        """
        Retrieve and display shard information for a given collection.

        Parameters:
        collection (str): The name of the collection to retrieve shard information for.
        """

        if collection is not None:
            if not self.client.collections.exists(collection):
                raise Exception(f"Collection '{collection}' does not exist")
            col_obj = self.client.collections.get(collection)
            shards = col_obj.config.get_shards()
            if json_output:
                shards_data = []
                for shard in shards:
                    shards_data.append(
                        {
                            "name": getattr(shard, "name", "N/A"),
                            "status": str(getattr(shard, "status", "N/A")),
                            "vector_queue_size": getattr(
                                shard, "vector_queue_size", None
                            ),
                        }
                    )
                click.echo(
                    json.dumps(
                        {
                            "collection": collection,
                            "shards": shards_data,
                            "total_shards": len(shards),
                        },
                        indent=2,
                        default=str,
                    )
                )
            else:
                click.echo(f"Collection {collection:<29}: Shards {len(shards):<15}")
                for shard in shards:
                    self._print_echo_shard_info(shard)
        else:
            all_collections = self.client.collections.list_all()
            if json_output:
                result = []
                for single_collection in all_collections:
                    col_obj = self.client.collections.get(single_collection)
                    shards = col_obj.config.get_shards()
                    shards_data = []
                    for shard in shards:
                        shards_data.append(
                            {
                                "name": getattr(shard, "name", "N/A"),
                                "status": str(getattr(shard, "status", "N/A")),
                                "vector_queue_size": getattr(
                                    shard, "vector_queue_size", None
                                ),
                            }
                        )
                    result.append(
                        {
                            "collection": single_collection,
                            "shards": shards_data,
                            "total_shards": len(shards),
                        }
                    )
                click.echo(
                    json.dumps(
                        {
                            "collections": result,
                            "total_collections": len(all_collections),
                        },
                        indent=2,
                        default=str,
                    )
                )
            else:
                for single_collection in all_collections:
                    col_obj = self.client.collections.get(single_collection)
                    shards = col_obj.config.get_shards()
                    click.echo(
                        f"Collection {single_collection:<29}: Shards {len(shards):<15}"
                    )
                    for shard in shards:
                        self._print_echo_shard_info(shard)
                click.echo(f"{'':<30}{'':<16}")
                click.echo(f"Total: {len(all_collections)} collections")

    def _print_echo_shard_info(self, shard):
        shard_name = getattr(shard, "name", "N/A")
        vector_indexing_status = getattr(shard, "status", "N/A")
        vector_queue_length = getattr(shard, "vector_queue_size", "N/A")

        click.echo(
            f"Shard Name: {shard_name}, Status: {vector_indexing_status}, Queue Length: {vector_queue_length}"
        )

    def update_shards(
        self,
        status: str = UpdateShardsDefaults.status,
        collection: Optional[str] = UpdateShardsDefaults.collection,
        shards: Optional[str] = UpdateShardsDefaults.shards,
        all: bool = UpdateShardsDefaults.all,
        json_output: bool = False,
    ):
        """
        Update the status of shards in a collection.

        Parameters:
        status (str): The new status to set for the shards.
        collection (str): The name of the collection whose shards are to be updated.
        shards (str): A comma-separated list of shard names to update. If empty, all shards in the collection will be updated.
        all (bool): If True, update shards in all collections. Cannot be used with specific collection or shards.
        """
        if all:
            if collection is not None:
                raise Exception("Cannot use 'all' flag with specific collection")
            if shards is not None:
                raise Exception("Cannot use 'all' flag with specific shards")

            all_collections = self.client.collections.list_all()
            updated = []
            for single_collection in all_collections:
                col_obj = self.client.collections.get(single_collection)
                col_shards = [s.name for s in col_obj.config.get_shards()]
                col_obj.config.update_shards(status, col_shards)
                if json_output:
                    updated.append(
                        {"collection": single_collection, "shards_updated": col_shards}
                    )
                else:
                    click.echo(
                        f"Shards '{col_shards}' updated to state '{status}' for collection '{single_collection}'"
                    )
            if json_output:
                click.echo(
                    json.dumps(
                        {"status": "success", "state": status, "collections": updated},
                        indent=2,
                    )
                )
        elif collection is not None:
            if not self.client.collections.exists(collection):
                raise Exception(f"Collection '{collection}' does not exist")
            col_obj = self.client.collections.get(collection)
            col_shards = [s.name for s in col_obj.config.get_shards()]
            if not shards:
                list_shards = col_shards
            else:
                list_shards = str.split(shards, ",")
                for shard in list_shards:
                    if shard not in col_shards:
                        raise Exception(
                            f"Shard '{shard}' does not exist in collection '{collection}'"
                        )

            col_obj.config.update_shards(status, list_shards)
            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "collection": collection,
                            "shards_updated": list_shards,
                            "state": status,
                        },
                        indent=2,
                    )
                )
            else:
                click.echo(
                    f"Shards '{list_shards}' updated to state '{status}' for collection '{collection}'"
                )
