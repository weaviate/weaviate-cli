import sys
import click
from lib.utils import get_client_from_context
from lib.managers.collection_manager import CollectionManager

# Query Group
@click.group()
def query() -> None:
    """Query resources in Weaviate."""
    pass