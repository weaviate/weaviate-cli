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

    @staticmethod
    def namespace_to_dict(namespace: Namespace) -> dict:
        """Serialize a Namespace to a JSON-friendly dict.

        ``home_node`` and ``state`` are only present on clusters/clients that
        support them (Weaviate 1.38.0+), so they are included only when set.
        """
        payload: dict = {"name": namespace.name}
        home_node = getattr(namespace, "home_node", None)
        if home_node is not None:
            payload["home_node"] = home_node
        state = getattr(namespace, "state", None)
        if state is not None:
            payload["state"] = state
        return payload

    def create_namespace(
        self, name: str, home_node: Optional[str] = None, json_output: bool = False
    ) -> Namespace:
        if not name:
            raise Exception("Namespace name is required.")
        try:
            kwargs: dict = {"name": name}
            if home_node is not None:
                kwargs["home_node"] = home_node
            namespace = self.client.namespaces.create(**kwargs)
            if json_output:
                payload = {
                    "status": "success",
                    "message": f"Namespace '{namespace.name}' created successfully.",
                    "namespace": namespace.name,
                }
                payload.update(
                    {
                        k: v
                        for k, v in self.namespace_to_dict(namespace).items()
                        if k != "name"
                    }
                )
                click.echo(json.dumps(payload, indent=2))
            else:
                click.echo(f"Namespace '{namespace.name}' created successfully.")
            return namespace
        except Exception as e:
            raise Exception(f"Error creating namespace '{name}': {e}")

    def update_namespace(
        self, name: str, home_node: str, json_output: bool = False
    ) -> Namespace:
        if not name:
            raise Exception("Namespace name is required.")
        if not home_node:
            raise Exception("Home node is required.")
        try:
            namespace = self.client.namespaces.update(name=name, home_node=home_node)
            if json_output:
                payload = {
                    "status": "success",
                    "message": f"Namespace '{namespace.name}' updated successfully.",
                    "namespace": namespace.name,
                }
                payload.update(
                    {
                        k: v
                        for k, v in self.namespace_to_dict(namespace).items()
                        if k != "name"
                    }
                )
                click.echo(json.dumps(payload, indent=2))
            else:
                click.echo(
                    f"Namespace '{namespace.name}' updated successfully "
                    f"(home node: {home_node})."
                )
            return namespace
        except Exception as e:
            raise Exception(f"Error updating namespace '{name}': {e}")

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
            click.echo(json.dumps(self.namespace_to_dict(namespace), indent=2))
        else:
            click.echo(f"Namespace: {namespace.name}")
            home_node = getattr(namespace, "home_node", None)
            if home_node is not None:
                click.echo(f"  Home node: {home_node}")
            state = getattr(namespace, "state", None)
            if state is not None:
                click.echo(f"  State: {state}")
