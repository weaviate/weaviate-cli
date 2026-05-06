import json
from typing import List, Optional

import click
from weaviate.client import WeaviateClient

try:
    from weaviate.namespaces.models import Namespace

    _NAMESPACE_SUPPORT = True
except ImportError:
    Namespace = None  # type: ignore[assignment,misc]
    _NAMESPACE_SUPPORT = False


class NamespaceManager:
    def __init__(self, client: WeaviateClient):
        if not _NAMESPACE_SUPPORT:
            raise RuntimeError(
                "Namespace support requires a weaviate-client version that includes "
                "the namespaces module. Please upgrade your weaviate-client package."
            )
        self.client = client

    def create_namespace(self, name: str, json_output: bool = False) -> Namespace:
        if not name:
            raise Exception("Namespace name is required.")
        try:
            namespace = self.client.namespaces.create(name=name)
            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "message": f"Namespace '{namespace.name}' created successfully.",
                            "namespace": namespace.name,
                        },
                        indent=2,
                    )
                )
            else:
                click.echo(f"Namespace '{namespace.name}' created successfully.")
            return namespace
        except Exception as e:
            raise Exception(f"Error creating namespace '{name}': {e}")

    def get_namespace(self, name: str) -> Optional[Namespace]:
        if not name:
            raise Exception("Namespace name is required.")
        try:
            return self.client.namespaces.get(name=name)
        except Exception as e:
            raise Exception(f"Error getting namespace '{name}': {e}")

    def list_namespaces(self) -> List[Namespace]:
        try:
            return self.client.namespaces.list_all()
        except Exception as e:
            raise Exception(f"Error listing namespaces: {e}")

    def delete_namespace(self, name: str, json_output: bool = False) -> None:
        if not name:
            raise Exception("Namespace name is required.")
        try:
            self.client.namespaces.delete(name=name)
            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "message": f"Namespace '{name}' deleted successfully.",
                        },
                        indent=2,
                    )
                )
            else:
                click.echo(f"Namespace '{name}' deleted successfully.")
        except Exception as e:
            raise Exception(f"Error deleting namespace '{name}': {e}")

    def print_namespace(self, namespace: Namespace, json_output: bool = False) -> None:
        if json_output:
            click.echo(json.dumps({"name": namespace.name}, indent=2))
        else:
            click.echo(f"Namespace: {namespace.name}")
