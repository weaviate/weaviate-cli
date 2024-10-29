import sys
import click
from typing import Optional, Union
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.shard_manager import ShardManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate.exceptions import WeaviateConnectionError
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

    client = None
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
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)


@update.command("tenants")
@click.option(
    "--collection", default="Movies", help="The name of the collection to update."
)
@click.option(
    "--tenant_suffix",
    default="Tenant--",
    help="The suffix to add to the tenant name (default: 'Tenant--').",
)
@click.option(
    "--number_tenants", default=100, help="Number of tenants to update (default: 100)."
)
@click.option(
    "--state",
    default="active",
    type=click.Choice(["hot", "active", "cold", "inactive", "frozen", "offloaded"]),
)
@click.pass_context
def update_tenants_cli(ctx, collection, tenant_suffix, number_tenants, state):
    """Update tenants in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        tenant_manager = TenantManager(client)
        tenant_manager.update_tenants(
            collection=collection,
            tenant_suffix=tenant_suffix,
            number_tenants=number_tenants,
            state=state,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)


@update.command("shards")
@click.option(
    "--collection",
    default=None,
    help="The name of the collection to update.",
)
@click.option(
    "--status",
    default="READY",
    type=click.Choice(["READY", "READONLY"]),
    help="The status of the shards.",
)
@click.option(
    "--shards",
    default=None,
    help="Comma separated list of shards to update.",
)
@click.option(
    "--all",
    is_flag=True,
    help="Update all collections.",
)

@click.pass_context
def update_shards_cli(
    ctx: click.Context,
    status: str,
    collection: Optional[str],
    shards: Optional[str],
    all: bool
) -> None:
    """Update shards in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        shard_man = ShardManager(client)
        # Call the function from update_shards.py with general and specific arguments
        shard_man.update_shards(
            status=status,
            collection=collection,
            shards=shards,
            all=all,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)

@update.command("data")
@click.option(
    "--collection", default="Movies", help="The name of the collection to update."
)
@click.option(
    "--limit", default=100, help="Number of objects to update (default: 100)."
)
@click.option(
    "--consistency_level",
    default="quorum",
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.option("--randomize", is_flag=True, help="Randomize the data (default: False).")
@click.pass_context
def update_data_cli(ctx, collection, limit, consistency_level, randomize):
    """Update data in a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        data_manager = DataManager(client)
        # Call the function from update_data.py with general and specific arguments
        data_manager.update_data(
            collection=collection,
            limit=limit,
            consistency_level=consistency_level,
            randomize=randomize,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)
