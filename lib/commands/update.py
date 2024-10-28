import sys
import click
from typing import Optional, Union
from lib.utils import get_client_from_context
from lib.managers.collection_manager import CollectionManager

# Update Group
@click.group()
def update() -> None:
    """Update resources in Weaviate."""
    pass

@update.command("collection")
@click.option(
    "--collection", default="Movies", help="The name of the collection to update."
)
@click.option(
    "--async_enabled", default=None, type=bool, help="Enable async (default: None)."
)
@click.option(
    "--vector_index",
    default=None,
    type=click.Choice(["hnsw", "flat", "hnsw_pq", "hnsw_bq", "hnsw_sq", "flat_bq"]),
    help='Vector index type (default: "None").',
)
@click.option(
    "--description", default=None, help="collection description (default: None)."
)
@click.option(
    "--training_limit",
    default=10000,
    help="Training limit for PQ and SQ (default: 10000).",
)
@click.option(
    "--auto_tenant_creation",
    default=None,
    type=bool,
    help="Enable auto tenant creation (default: None).",
)
@click.option(
    "--auto_tenant_activation",
    default=None,
    type=bool,
    help="Enable auto tenant activation (default: None).",
)
@click.pass_context
def update_collection_cli(
    ctx: click.Context,
    collection: str,
    async_enabled: Optional[bool],
    vector_index: Optional[str],
    description: Optional[str],
    training_limit: int,
    auto_tenant_creation: Optional[bool],
    auto_tenant_activation: Optional[bool],
) -> None:
    """Update a collection in Weaviate."""

    try:
        client = get_client_from_context(ctx)
        collection_man = CollectionManager(client)
        # Call the function from update_collection.py with general and specific arguments
        collection_man.update_collection(
            collection=collection,
            async_enabled=async_enabled,
            vector_index=vector_index,
            description=description,
            training_limit=training_limit,
            auto_tenant_creation=auto_tenant_creation,
            auto_tenant_activation=auto_tenant_activation,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        client.close()
        sys.exit(1)  # Return a non-zero exit code on failure

    client.close()