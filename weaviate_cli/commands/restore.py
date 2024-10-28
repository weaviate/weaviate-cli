import sys
import click
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager

# Restore Group
@click.group()
def restore() -> None:
    """Restore backups in Weaviate."""
    pass