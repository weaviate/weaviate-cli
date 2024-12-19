import sys
import click
from typing import Optional

from weaviate_cli.completion.complete import (
    role_name_complete,
    collection_name_complete,
)
from weaviate_cli.managers.backup_manager import BackupManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate_cli.managers.role_manager import RoleManager
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.defaults import (
    CreateBackupDefaults,
    CreateCollectionDefaults,
    CreateTenantsDefaults,
    CreateDataDefaults,
    CreateRoleDefaults,
    PERMISSION_HELP_STRING,
)


# Create Group
@click.group()
def create() -> None:
    """Create resources in Weaviate."""
    pass


# Subcommand to create a collection
@create.command("collection")
@click.option(
    "--collection",
    default=CreateCollectionDefaults.collection,
    help="The name of the collection to create.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--replication_factor",
    default=CreateCollectionDefaults.replication_factor,
    help="Replication factor (default: 3).",
)
@click.option(
    "--async_enabled",
    is_flag=True,
    help="Enable async (default: False).",
)
@click.option(
    "--vector_index",
    default=CreateCollectionDefaults.vector_index,
    type=click.Choice(
        [
            "hnsw",
            "flat",
            "dynamic",
            "hnsw_pq",
            "hnsw_bq",
            "hnsw_sq",
            "hnsw_acorn",
            "flat_bq",
        ]
    ),
    help="Vector index type (default: 'hnsw').",
)
@click.option(
    "--inverted_index",
    default=CreateCollectionDefaults.inverted_index,
    type=click.Choice(["timestamp", "null", "length"]),
    help="Inverted index properties (default: None). Options: 'timestamp'==index_timestamps, 'null'==index_null_state, 'length'==index_property_length.",
)
@click.option(
    "--training_limit",
    default=CreateCollectionDefaults.training_limit,
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
@click.option(
    "--force_auto_schema",
    is_flag=True,
    help="Force auto-schema (default: False). If passed, no properties will be added to the collection and it will be the auto-schema the one that infers the properties.",
)
@click.option(
    "--shards",
    default=CreateCollectionDefaults.shards,
    help="Number of shards (default: 1).",
)
@click.option(
    "--vectorizer",
    default=CreateCollectionDefaults.vectorizer,
    type=click.Choice(
        [
            "contextionary",
            "transformers",
            "openai",
            "ollama",
            "cohere",
            "jinaai",
            "weaviate",
        ]
    ),
    help="Vectorizer to use.",
)
@click.option(
    "--replication_deletion_strategy",
    default=CreateCollectionDefaults.replication_deletion_strategy,
    type=click.Choice(
        ["delete_on_conflict", "no_automated_resolution", "time_based_resolution"]
    ),
    help="Replication deletion strategy (default: 'delete_on_conflict').",
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
    force_auto_schema: bool,
    shards: int,
    vectorizer: Optional[str],
    replication_deletion_strategy: str,
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
            force_auto_schema=force_auto_schema,
            shards=shards,
            vectorizer=vectorizer,
            replication_deletion_strategy=replication_deletion_strategy,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


# Subcommand to create tenants
@create.command("tenants")
@click.option(
    "--collection",
    default=CreateTenantsDefaults.collection,
    help="The name of the collection to create.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--tenant_suffix",
    default=CreateTenantsDefaults.tenant_suffix,
    help="The suffix to add to the tenant name (default: 'Tenant--').",
)
@click.option(
    "--number_tenants",
    default=CreateTenantsDefaults.number_tenants,
    help="Number of tenants to create (default: 100).",
)
@click.option(
    "--state",
    default=CreateTenantsDefaults.state,
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
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@create.command("backup")
@click.option(
    "--backend",
    default=CreateBackupDefaults.backend,
    type=click.Choice(["s3", "gcs", "filesystem"]),
    help="The backend used for storing the backups (default: s3).",
)
@click.option(
    "--backup_id",
    default=CreateBackupDefaults.backup_id,
    help="Identifier used for the backup (default: test-backup).",
)
@click.option(
    "--include",
    default=CreateBackupDefaults.include,
    help="Comma separated list of collections to include in the backup. If not provided, all collections will be included.",
)
@click.option(
    "--exclude",
    default=CreateBackupDefaults.exclude,
    help="Comma separated list of collections to exclude from the backup. If not provided, all collections will be included.",
)
@click.option(
    "--wait", is_flag=True, help="Wait for the backup to complete before returning."
)
@click.option(
    "--cpu_for_backup",
    default=CreateBackupDefaults.cpu_for_backup,
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
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


# Subcommand to ingest data
@create.command("data")
@click.option(
    "--collection",
    default=CreateDataDefaults.collection,
    help="The name of the collection to ingest data into.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--limit",
    default=CreateDataDefaults.limit,
    help="Number of objects to import (default: 1000).",
)
@click.option(
    "--consistency_level",
    default=CreateDataDefaults.consistency_level,
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.option("--randomize", is_flag=True, help="Randomize the data (default: False).")
@click.option(
    "--auto_tenants",
    default=CreateDataDefaults.auto_tenants,
    help="Number of tenants for which we will send data. NOTE: Requires class with --auto_tenant_creation (default: 0).",
)
@click.option(
    "--vector_dimensions",
    default=CreateDataDefaults.vector_dimensions,
    help="Number of vector dimensions to be used when the data is randomized.",
)
@click.option(
    "--uuid",
    default=None,
    help="UUID of the object to be used when the data is randomized. It requires --limit=1 and --randomize to be enabled.",
)
@click.pass_context
def create_data_cli(
    ctx,
    collection,
    limit,
    consistency_level,
    randomize,
    auto_tenants,
    vector_dimensions,
    uuid,
):
    """Ingest data into a collection in Weaviate."""

    if vector_dimensions != 1536 and not randomize:
        click.echo(
            "Error: --vector_dimensions has no effect unless --randomize is enabled."
        )
        sys.exit(1)

    if uuid is not None and not randomize:
        click.echo("Error: --uuid has no effect unless --randomize is enabled.")
        sys.exit(1)

    if uuid is not None and limit != 1:
        click.echo("Error: --uuid has no effect unless --limit=1 is enabled.")
        sys.exit(1)

    client = None
    try:
        client = get_client_from_context(ctx)
        data_manager = DataManager(client)
        # Call the function from ingest_data.py with general and specific arguments
        data_manager.create_data(
            collection=collection,
            limit=limit,
            consistency_level=consistency_level,
            randomize=randomize,
            auto_tenants=auto_tenants,
            vector_dimensions=vector_dimensions,
            uuid=uuid,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@create.command("role")
@click.option(
    "--role_name",
    default=CreateRoleDefaults.role_name,
    help="The name of the role to create.",
    shell_complete=role_name_complete,
)
@click.option(
    "-p",
    "--permission",
    multiple=True,
    required=True,
    help=PERMISSION_HELP_STRING,
)
@click.pass_context
def create_role_cli(ctx: click.Context, role_name: str, permission: tuple[str]) -> None:
    """Create a role in Weaviate."""
    client = None
    try:
        client = get_client_from_context(ctx)
        role_man = RoleManager(client)
        role_man.create_role(role_name=role_name, permissions=permission)
        click.echo(f"Role '{role_name}' created successfully in Weaviate.")
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
