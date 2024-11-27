import click
import sys
import os
import json
from weaviate_cli.utils import get_client_from_context
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.defaults import CreateCollectionDefaults
from weaviate_cli.managers.batch_manager import BatchManager


@click.group()
def batch() -> None:
    """Batch operations in Weaviate."""
    pass


@batch.command("insert")
@click.option(
    "--collection",
    required=True,
    help="The name of the collection (class) to insert data into.",
)
@click.option(
    "--path",
    required=True,
    type=str,
    help="Path to the JSON file containing the data.",
)
@click.option(
    "--vectorizer",
    default=CreateCollectionDefaults.vectorizer,
    type=click.Choice(
        ["contextionary", "transformers", "openai", "ollama", "cohere", "jinaai"]
    ),
    help="Vectorizer to use.",
)
@click.option(
    "--shards",
    default=1,
    help="Number of shards for the collection (default: 1).",
)
@click.option(
    "--replication-factor",
    default=1,
    help="Replication factor for the collection (default: 1).",
)
@click.pass_context
def batch_insert_cli(ctx, collection, path, vectorizer, shards, replication_factor):
    """
    Insert data into a Weaviate collection (class) in batch mode.
    """
    # Validate the file path and extension
    if not os.path.isfile(path):
        click.echo(f"Error: The file {path} does not exist.")
        sys.exit(1)
    if not path.endswith(".json"):
        click.echo("Error: The file must have a .json extension.")
        sys.exit(1)

    # Load the JSON data
    try:
        with open(path, "r") as file:
            data = json.load(file)
    except json.JSONDecodeError:
        click.echo(f"Error: The file {path} is not a valid JSON file.")
        sys.exit(1)

    # Validate JSON structure
    if not isinstance(data, list) or not all(isinstance(obj, dict) for obj in data):
        click.echo(
            "Error: The JSON file must contain a list of objects (e.g., [{...}, {...}])."
        )
        sys.exit(1)

    # Initialize the Weaviate client
    client = None
    try:
        client = get_client_from_context(ctx)
        batch_manager = BatchManager(client)

        # Create the collection (if it doesn't exist)
        click.echo(f"Ensuring collection '{collection}' exists...")
        batch_manager.create_collection(
            collection=collection,
            vectorizer=vectorizer,
            shards=shards,
            replication_factor=replication_factor,
            force_auto_schema=True,
        )

        # Insert the data in batch mode
        click.echo(f"Inserting data into collection '{collection}'...")
        batch_manager.batch_insert(collection, data)

    except WeaviateConnectionError as wce:
        click.echo(f"Connection error: {wce}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)
    finally:
        if client:
            client.close()
