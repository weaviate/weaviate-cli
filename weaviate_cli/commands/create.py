import sys
import click
from typing import Optional
from weaviate_cli.managers.backup_manager import BackupManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate.exceptions import WeaviateConnectionError
# Create Group
@click.group()
def create() -> None:
    """Create resources in Weaviate."""
    pass

# Subcommand to create a collection
@create.command("collection")
@click.option(
    "--collection", default="Movies", help="The name of the collection to create."
)
@click.option(
    "--replication_factor", default=3, help="Replication factor (default: 3)."
)
@click.option("--async_enabled", is_flag=True, help="Enable async (default: False).")
@click.option(
    "--vector_index",
    default="hnsw",
    type=click.Choice(
        ["hnsw", "flat", "dynamic", "hnsw_pq", "hnsw_bq", "hnsw_sq", "flat_bq"]
    ),
    help="Vector index type (default: 'hnsw').",
)
@click.option(
    "--inverted_index",
    default=None,
    type=click.Choice(["timestamp", "null", "length"]),
    help="Inverted index properties (default: None). Options: 'timestamp'==index_timestamps, 'null'==index_null_state, 'length'==index_property_length.",
)
@click.option(
    "--training_limit",
    default=10000,
    help="Training limit for PQ and SQ (default: 10000).",
)
@click.option(
    "--multitenant", is_flag=True, help="Enable multitenancy (default: False)."
)
@click.option(
    "--auto_tenant_creation",
    is_flag=True,
    help="Enable auto tenant creation (default: False).",
)
@click.option(
    "--auto_tenant_activation",
    is_flag=True,
    help="Enable auto tenant activation (default: False).",
)
@click.option("--auto_schema", default=True, help="Enable auto-schema (default: True).")
@click.option("--shards", default=1, help="Number of shards (default: 1).")
@click.option(
    "--vectorizer",
    default=None,
    type=click.Choice(["contextionary", "transformers", "openai", "ollama"]),
    help="Vectorizer to use.",
)
@click.pass_context
def create_collection_cli(
    ctx: click.Context,
    collection: str,
    replication_factor: int,
    async_enabled: bool,
    vector_index: str,
    inverted_index: Optional[str],
    training_limit: int,
    multitenant: bool,
    auto_tenant_creation: bool,
    auto_tenant_activation: bool,
    auto_schema: bool,
    shards: int,
    vectorizer: Optional[str],
) -> None:
    """Create a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        # Call the function from create_collection.py passing both general and specific arguments
        collection_man = CollectionManager(client)
        collection_man.create_collection(
            collection=collection,
            replication_factor=replication_factor,
            async_enabled=async_enabled,
            vector_index=vector_index,
            inverted_index=inverted_index,
            training_limit=training_limit,
            multitenant=multitenant,
            auto_tenant_creation=auto_tenant_creation,
            auto_tenant_activation=auto_tenant_activation,
            auto_schema=auto_schema,
            shards=shards,
            vectorizer=vectorizer,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)


# Subcommand to create tenants
@create.command("tenants")
@click.option(
    "--collection", default="Movies", help="The name of the collection to create."
)
@click.option(
    "--tenant_suffix",
    default="Tenant--",
    help="The suffix to add to the tenant name (default: 'Tenant--').",
)
@click.option(
    "--number_tenants", default=100, help="Number of tenants to create (default: 100)."
)
@click.option(
    "--state",
    default="active",
    type=click.Choice(["hot", "active", "cold", "inactive", "frozen", "offloaded"]),
)
@click.pass_context
def create_tenants_cli(ctx, collection, tenant_suffix, number_tenants, state):
    """Create tenants in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        # Call the function from create_tenants.py with general and specific arguments
        tenant_manager = TenantManager(client)
        tenant_manager.create_tenants(
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


@create.command("backup")
@click.option(
    "--backend",
    default="s3",
    type=click.Choice(["s3", "gcs", "filesystem"]),
    help="The backend used for storing the backups (default: s3).",
)
@click.option(
    "--backup_id",
    default="test-backup",
    help="Identifier used for the backup (default: test-backup).",
)
@click.option(
    "--include",
    default=None,
    help="Comma separated list of collections to include in the backup. If not provided, all collections will be included.",
)
@click.option(
    "--exclude",
    default=None,
    help="Comma separated list of collections to exclude from the backup. If not provided, all collections will be included.",
)
@click.option(
    "--wait", is_flag=True, help="Wait for the backup to complete before returning."
)
@click.option(
    "--cpu_for_backup",
    default=40,
    help="The percentage of CPU to use for the backup (default: 40). The larger, the faster it will occur, but it will also consume more memory.",
)
@click.pass_context
def create_backup_cli(ctx, backend, backup_id, include, exclude, wait, cpu_for_backup):
    """Create a backup in Weaviate."""
    
    client = None
    try:
        client = get_client_from_context(ctx)
        backup_manager = BackupManager(client)
        backup_manager.create_backup(
            backend=backend,
            backup_id=backup_id,
            include=include,
            exclude=exclude,
            wait=wait,
            cpu_for_backup=cpu_for_backup,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)


# Subcommand to ingest data
@create.command("data")
@click.option(
    "--collection",
    default="Movies",
    help="The name of the collection to ingest data into.",
)
@click.option(
    "--limit", default=1000, help="Number of objects to import (default: 1000)."
)
@click.option(
    "--consistency_level",
    default="quorum",
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.option("--randomize", is_flag=True, help="Randomize the data (default: False).")
@click.option(
    "--auto_tenants",
    default=0,
    help="Number of tenants for which we will send data. NOTE: Requires class with --auto_tenant_creation (default: 0).",
)
@click.pass_context
def create_data_cli(ctx, collection, limit, consistency_level, randomize, auto_tenants):
    """Ingest data into a collection in Weaviate."""
    
    client = None
    try:
        client = get_client_from_context(ctx)
        data_manager = DataManager(client)
        # Call the function from ingest_data.py with general and specific arguments
        data_manager.ingest_data(
            collection=collection,
            limit=limit,
            consistency_level=consistency_level,
            randomize=randomize,
            auto_tenants=auto_tenants,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)
