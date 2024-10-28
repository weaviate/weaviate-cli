import sys
import click
from lib.utils import get_client_from_context
from lib.managers.collection_manager import CollectionManager

# Get Group
@click.group()
def get():
    """Create resources in Weaviate."""
    pass

@get.command("collection")
@click.option("--collection", default=None, help="The name of the collection to get.")
@click.pass_context
def get_collection_cli(ctx, collection):
    """Get all collections in Weaviate. If --collection is provided, get the specific collection."""

    try:
        client = get_client_from_context(ctx)
        collection_man = CollectionManager(client)
        # Call the function from get_collections.py with general arguments
        collection_man.get_collection(collection=collection)
    except Exception as e:
        click.echo(f"Error: {e}")
        client.close()
        sys.exit(1)  # Return a non-zero exit code on failure

    client.close()
