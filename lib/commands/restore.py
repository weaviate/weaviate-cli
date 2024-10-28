import sys
import click
from lib.utils import get_client_from_context
from lib.managers.collection_manager import CollectionManager

# Restore Group
@click.group()
def restore() -> None:
    """Restore backups in Weaviate."""
    pass