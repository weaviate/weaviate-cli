from dataclasses import dataclass
from typing import Dict


@dataclass
class NodeInfo:
    """Information about a node in the cluster."""

    name: str
    status: str
    version: str
    object_count: int
    shard_count: int


@dataclass
class ShardInfo:
    """Information about a shard in the cluster."""

    name: str
    collection: str
    node_objects: Dict[str, int]  # node_name -> object_count
    vector_indexing_status: str
    loaded: bool


@dataclass
class CollectionInfo:
    """Information about a collection in the cluster."""

    name: str
    node_objects: Dict[str, int]  # node_name -> object_count
    total_objects: int
