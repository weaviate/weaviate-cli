from typing import Callable, Optional

from prettytable import PrettyTable

import weaviate.classes.replication as wvcr
import weaviate.outputs.cluster as wvoc
import weaviate.outputs.replication as wvor
from weaviate.client import WeaviateClient


class ClusterManager:
    def __init__(self, client: WeaviateClient, printer: Callable[[str], None] = print):
        self.client = client
        self.printer = printer

    def start_replication(
        self,
        collection: str,
        shard: str,
        source_node: str,
        target_node: str,
        type_: str,
    ) -> str:
        """Start a replication in Weaviate."""
        try:
            uuid = self.client.cluster.replicate(
                collection=collection,
                shard=shard,
                source_node=source_node,
                target_node=target_node,
                replication_type=wvcr.ReplicationType(type_),
            )
            return str(uuid)
        except Exception as e:
            raise Exception(f"Error starting replication: {e}")

    def delete_replication(self, op_id: str) -> None:
        """Delete a replication in Weaviate."""
        try:
            self.client.cluster.replications.delete(uuid=op_id)
        except Exception as e:
            raise Exception(f"Error deleting replication with UUID '{op_id}': {e}")

    def delete_all_replication(self) -> None:
        """Delete all replications in Weaviate."""
        try:
            self.client.cluster.replications.delete_all()
        except Exception as e:
            raise Exception(f"Error deleting all replications: {e}")

    def cancel_replication(self, op_id: str) -> None:
        """Cancel a replication operation in Weaviate."""
        try:
            self.client.cluster.replications.cancel(uuid=op_id)
        except Exception as e:
            raise Exception(f"Error cancelling replication with UUID '{op_id}': {e}")

    def get_replication(self, op_id: str, include_history: bool):
        """Get details of a replication operation in Weaviate."""
        try:
            return self.client.cluster.replications.get(
                uuid=op_id, include_history=include_history
            )
        except Exception as e:
            raise Exception(f"Error getting replication with UUID '{op_id}': {e}")

    def get_all_replications(self):
        """Get all replication operations in Weaviate."""
        try:
            return self.client.cluster.replications.list_all()
        except Exception as e:
            raise Exception(f"Error getting replications: {e}")

    def query_replications_by_collection(self, collection: str, include_history: bool):
        """Query replications by collection."""
        try:
            return self.client.cluster.replications.query(
                collection=collection, include_history=include_history
            )
        except Exception as e:
            raise Exception(
                f"Error querying replications for collection '{collection}': {e}"
            )

    def query_replications_by_shard(
        self, collection: str, shard: str, include_history: bool
    ):
        """Query replications by collection and shard."""
        try:
            return self.client.cluster.replications.query(
                collection=collection, shard=shard, include_history=include_history
            )
        except Exception as e:
            raise Exception(
                f"Error querying replications for collection '{collection}' and shard '{shard}': {e}"
            )

    def query_replications_by_node(self, node: str, include_history: bool):
        """Query replications by node."""
        try:
            return self.client.cluster.replications.query(
                target_node=node, include_history=include_history
            )
        except Exception as e:
            raise Exception(f"Error querying replications for node '{node}': {e}")

    def query_all_replications(self, include_history: bool):
        """Get all replication operations in Weaviate."""
        try:
            return self.client.cluster.replications.query(
                include_history=include_history
            )
        except Exception as e:
            raise Exception(f"Error getting replications: {e}")

    def print_replication(self, replication: wvor.ReplicateOperation) -> None:
        """Print replication in a human-readable format."""

        self.printer(f"{'-' * 6} {replication.uuid} {'-' * 6}")  # uuid is 36 chars long
        self.printer(f"Type: {replication.transfer_type.value}")
        self.printer(f"Collection: {replication.collection}")
        self.printer(f"Shard: {replication.shard}")
        self.printer(f"Source Node: {replication.source_node}")
        self.printer(f"Target Node: {replication.target_node}")

        self.printer(f"Status: {replication.status.state.value}")
        last_error = self.__get_last_error(replication)
        if last_error != "":
            self.printer(f"Last Error: {self.__get_last_error(replication)}")

        if len(replication.status.errors) > 0:
            self.printer("Errors:")
            for error in replication.status.errors:
                self.printer(f"  - {error}")

        self.printer("-" * 50)

        if replication.status_history is not None:
            self.printer("Status History:")
            for status in replication.status_history:
                self.printer(f"  - {status.state.value}")
                if len(status.errors) > 0:
                    self.printer("    Errors:")
                    for error in status.errors:
                        self.printer(f"      - {error}")
                    self.printer("-" * 50)

    def print_replications(self, replications: wvor.ReplicateOperations) -> None:
        """Print a list of replications in a human-readable format."""
        if not replications:
            self.printer("No replications found.")
            return

        table = PrettyTable()
        table.field_names = [
            "UUID",
            "Status",
            "Collection",
            "Shard",
            "Source Node",
            "Target Node",
            "Type",
            "History",
            "Last Error",
        ]

        for replication in replications:
            table.add_row(
                [
                    replication.uuid,
                    replication.status.state.value,
                    replication.collection,
                    replication.shard,
                    replication.source_node,
                    replication.target_node,
                    replication.transfer_type.value,
                    (
                        " | ".join(
                            status.state.value for status in replication.status_history
                        )
                        if replication.status_history
                        else ""
                    ),
                    self.__get_last_error(replication),
                ]
            )

        self.printer(str(table))

    def __get_last_error(self, replication: wvor.ReplicateOperation) -> str:
        """Get the last error from a replication operation."""
        if len(replication.status.errors) > 0:
            return replication.status.errors[0]
        if replication.status_history is not None:
            for status in replication.status_history:
                if len(status.errors) > 0:
                    return status.errors[0]
        return ""

    def query_sharding_state(self, collection: str, shard: Optional[str]):
        """Query the sharding state of a collection or shard."""
        try:
            return self.client.cluster.query_sharding_state(
                collection=collection, shard=shard
            )
        except Exception as e:
            raise Exception(
                f"Error querying sharding state for collection '{collection}' and shard '{shard}': {e}"
            )

    def print_sharding_state(self, sharding_state: wvoc.ShardingState) -> None:
        """Print the sharding state in a human-readable format."""
        table = PrettyTable()

        table.field_names = ["Shard", "Replicas"]
        for shard in sharding_state.shards:
            replicas = " | ".join(shard.replicas) if shard.replicas else "None"
            table.add_row([shard.name, replicas])

        self.printer(str(table))
