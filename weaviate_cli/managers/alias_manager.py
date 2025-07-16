from typing import Optional
import click
from weaviate.client import WeaviateClient
from weaviate.aliases.alias import AliasReturn


class AliasManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def create_alias(self, alias_name: str, collection: str) -> None:
        try:
            self.client.alias.create(
                alias_name=alias_name, target_collection=collection
            )
            click.echo(
                f"Alias '{alias_name}' to collection '{collection}' created successfully."
            )
        except Exception as e:
            raise Exception(f"Error creating alias '{alias_name}': {e}")

    def update_alias(self, alias_name: str, collection: str) -> None:
        try:
            self.client.alias.update(
                alias_name=alias_name, new_target_collection=collection
            )
            click.echo(
                f"Alias '{alias_name}' to collection '{collection}' updated successfully."
            )
        except Exception as e:
            raise Exception(f"Error updating alias '{alias_name}': {e}")

    def get_alias(self, alias_name: str) -> AliasReturn:
        try:
            return self.client.alias.get(alias_name=alias_name)
        except Exception as e:
            raise Exception(f"Error getting alias '{alias_name}': {e}")

    def delete_alias(self, alias_name: str) -> None:
        try:
            self.client.alias.delete(alias_name=alias_name)
        except Exception as e:
            raise Exception(f"Error deleting alias '{alias_name}': {e}")

    def list_aliases(self, collection: Optional[str] = None) -> dict[str, AliasReturn]:
        try:
            return self.client.alias.list_all(collection=collection)
        except Exception as e:
            raise Exception(f"Error listing aliases: {e}")

    def print_alias(self, alias: AliasReturn) -> None:
        click.echo(f"Alias: {alias.alias} -> {alias.collection}")
