from typing import Dict, List, Optional
from prettytable import PrettyTable
from weaviate.client import WeaviateClient
from weaviate_cli.types.models import NodeInfo, ShardInfo, CollectionInfo


class NodeManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def get_nodes(
        self,
        minimal: bool = False,
        shards: bool = False,
        collections: bool = False,
        collection: Optional[str] = None,
    ) -> None:
        if minimal:
            nodes = self.__get_nodes_minimal()
            self.__print_minimal_nodes(nodes)
        else:
            nodes = self.__get_nodes_verbose()
            if collection:
                collection_shards = self.get_collection_shards(collection, nodes)
                self.__print_collection_shards(collection, collection_shards)
            elif shards:
                self.__print_shards_info(self.get_shards_objects(nodes))
            elif collections:
                self.__print_collections_info(self.get_collections_objects(nodes))
            else:
                self.__print_nodes_info(self.get_nodes_objects(nodes))

    def get_nodes_objects(self, nodes=None) -> List[NodeInfo]:
        if nodes is None:
            nodes = self.__get_nodes_verbose()

        node_infos = []
        for node in nodes:
            object_count = (
                sum(shard.object_count for shard in node.shards) if node.shards else 0
            )
            shard_count = len(node.shards) if node.shards else 0

            node_infos.append(
                NodeInfo(
                    name=node.name,
                    status=node.status,
                    version=node.version,
                    object_count=object_count,
                    shard_count=shard_count,
                )
            )
        return node_infos

    def get_shards_objects(self, nodes=None) -> List[ShardInfo]:
        if nodes is None:
            nodes = self.__get_nodes_verbose()

        shard_dict: Dict[str, ShardInfo] = {}

        for node in nodes:
            if not node.shards:
                continue

            for shard in node.shards:
                if shard.name not in shard_dict:
                    shard_dict[shard.name] = ShardInfo(
                        name=shard.name,
                        collection=shard.collection,
                        node_objects={},
                        vector_indexing_status=shard.vector_indexing_status,
                        loaded=shard.loaded,
                    )
                shard_dict[shard.name].node_objects[node.name] = shard.object_count

        return list(shard_dict.values())

    def get_collections_objects(self, nodes=None) -> List[CollectionInfo]:
        if nodes is None:
            nodes = self.__get_nodes_verbose()

        collection_dict: Dict[str, CollectionInfo] = {}

        for node in nodes:
            if not node.shards:
                continue

            for shard in node.shards:
                if shard.collection not in collection_dict:
                    collection_dict[shard.collection] = CollectionInfo(
                        name=shard.collection, node_objects={}, total_objects=0
                    )

                current_count = collection_dict[shard.collection].node_objects.get(
                    node.name, 0
                )
                collection_dict[shard.collection].node_objects[node.name] = (
                    current_count + shard.object_count
                )
                collection_dict[shard.collection].total_objects += shard.object_count

        return list(collection_dict.values())

    def get_collection_shards(
        self, collection_name: str, nodes=None
    ) -> List[ShardInfo]:
        """Get shard information for a specific collection."""
        if nodes is None:
            nodes = self.__get_nodes_verbose()

        shard_dict: Dict[str, ShardInfo] = {}

        for node in nodes:
            if not node.shards:
                continue

            for shard in node.shards:
                if shard.collection != collection_name:
                    continue

                if shard.name not in shard_dict:
                    shard_dict[shard.name] = ShardInfo(
                        name=shard.name,
                        collection=shard.collection,
                        node_objects={},
                        vector_indexing_status=shard.vector_indexing_status,
                        loaded=shard.loaded,
                    )
                shard_dict[shard.name].node_objects[node.name] = shard.object_count

        return list(shard_dict.values())

    def __print_collection_shards(self, collection_name: str, shards: List[ShardInfo]):
        """Print shard information for a specific collection."""
        if not shards:
            print(f"No shards found for collection '{collection_name}'")
            return

        table = PrettyTable()
        table.field_names = ["Shard Name", "Node Distribution", "Status", "Loaded"]

        # Get replication factor for the collection
        rf = (
            self.client.collections.get(collection_name)
            .config.get()
            .replication_config.factor
        )

        for shard in shards:
            node_dist = ", ".join(
                [f"{node}: {count}" for node, count in shard.node_objects.items()]
            )
            table.add_row(
                [shard.name, node_dist, shard.vector_indexing_status, shard.loaded]
            )

        print(f"\nCollection: {collection_name} (Replication Factor: {rf})")
        print(table)

    def __get_nodes_verbose(self):
        return self.client.cluster.nodes(output="verbose")

    def __get_nodes_minimal(self):
        return self.client.cluster.nodes(output="minimal")

    def __print_minimal_nodes(self, nodes):
        table = PrettyTable()
        table.field_names = ["Node Name", "Status", "Version"]

        for node in nodes:
            table.add_row([node.name, node.status, node.version])

        print(table)

    def __print_nodes_info(self, nodes: List[NodeInfo]):
        table = PrettyTable()
        table.field_names = [
            "Node Name",
            "Status",
            "Version",
            "Object Count",
            "Shard Count",
        ]

        for node in nodes:
            table.add_row(
                [
                    node.name,
                    node.status,
                    node.version,
                    node.object_count,
                    node.shard_count,
                ]
            )

        print(table)

    def __print_shards_info(self, shards: List[ShardInfo]):
        table = PrettyTable()
        table.field_names = [
            "Shard Name",
            "Collection",
            "Node Distribution",
            "Status",
            "Loaded",
        ]

        for shard in shards:
            node_dist = ", ".join(
                [f"{node}: {count}" for node, count in shard.node_objects.items()]
            )
            table.add_row(
                [
                    shard.name,
                    shard.collection,
                    node_dist,
                    shard.vector_indexing_status,
                    shard.loaded,
                ]
            )

        print(table)

    def __print_collections_info(self, collections: List[CollectionInfo]):
        table = PrettyTable()
        table.field_names = [
            "Col. Name",
            "Repl. Factor",
            "Node Distribution",
            "Total Objects",
        ]

        for collection in collections:
            rf = (
                self.client.collections.get(collection.name)
                .config.get()
                .replication_config.factor
            )
            node_dist = ", ".join(
                [f"{node}: {count}" for node, count in collection.node_objects.items()]
            )
            table.add_row([collection.name, rf, node_dist, collection.total_objects])

        print(table)
