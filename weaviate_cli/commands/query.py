import sys
import click
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager

# Query Group
@click.group()
def query() -> None:
    """Query resources in Weaviate."""
    pass