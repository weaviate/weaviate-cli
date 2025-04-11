import sys
import click
import json
from typing import Optional, Union

from weaviate_cli.completion.complete import collection_name_complete
from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.managers.user_manager import UserManager
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.shard_manager import ShardManager
from weaviate_cli.managers.data_manager import DataManager
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.defaults import UpdateCollectionDefaults
from weaviate_cli.defaults import UpdateTenantsDefaults
from weaviate_cli.defaults import UpdateShardsDefaults
from weaviate_cli.defaults import UpdateDataDefaults
from weaviate_cli.defaults import UpdateUserDefaults


# Update Group
@click.group()
def update() -> None:
    """Update resources in Weaviate."""
    pass


@update.command("collection")
@click.option(
    "--collection",
    default=UpdateCollectionDefaults.collection,
    help="The name of the collection to update.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--async_enabled",
    default=UpdateCollectionDefaults.async_enabled,
    type=bool,
    help="Enable async (default: None).",
)
@click.option(
    "--vector_index",
    default=UpdateCollectionDefaults.vector_index,
    type=click.Choice(
        ["hnsw", "flat", "hnsw_pq", "hnsw_bq", "hnsw_sq", "flat_bq", "hnsw_acorn"]
    ),
    help='Vector index type (default: "None").',
)
@click.option(
    "--description",
    default=UpdateCollectionDefaults.description,
    help="collection description (default: None).",
)
@click.option(
    "--training_limit",
    default=UpdateCollectionDefaults.training_limit,
    help="Training limit for PQ and SQ (default: 10000).",
)
@click.option(
    "--auto_tenant_creation",
    default=UpdateCollectionDefaults.auto_tenant_creation,
    type=bool,
    help="Enable auto tenant creation (default: None).",
)
@click.option(
    "--auto_tenant_activation",
    default=UpdateCollectionDefaults.auto_tenant_activation,
    type=bool,
    help="Enable auto tenant activation (default: None).",
)
@click.option(
    "--replication_deletion_strategy",
    default=UpdateCollectionDefaults.replication_deletion_strategy,
    type=click.Choice(
        ["delete_on_conflict", "no_automated_resolution", "time_based_resolution"]
    ),
    help="Replication deletion strategy.",
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
    replication_deletion_strategy: Optional[str],
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


@update.command("tenants")
@click.option(
    "--collection",
    default=UpdateTenantsDefaults.collection,
    help="The name of the collection to update.",
)
@click.option(
    "--tenant_suffix",
    default=UpdateTenantsDefaults.tenant_suffix,
    help="The suffix to add to the tenant name (default: 'Tenant-').",
)
@click.option(
    "--number_tenants",
    default=UpdateTenantsDefaults.number_tenants,
    help="Number of tenants to update (default: 100).",
)
@click.option(
    "--state",
    default=UpdateTenantsDefaults.state,
    type=click.Choice(["hot", "active", "cold", "inactive", "frozen", "offloaded"]),
)
@click.option(
    "--tenants",
    default=UpdateTenantsDefaults.tenants,
    help="Comma separated list of tenants to update. Ex: 'Tenant-1,Tenant-2,Tenant-3,Tenant-4'",
)
@click.pass_context
def update_tenants_cli(ctx, collection, tenant_suffix, number_tenants, state, tenants):
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
            tenants=tenants,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@update.command("shards")
@click.option(
    "--collection",
    default=UpdateShardsDefaults.collection,
    help="The name of the collection to update.",
)
@click.option(
    "--status",
    default=UpdateShardsDefaults.status,
    type=click.Choice(["READY", "READONLY"]),
    help="The status of the shards.",
)
@click.option(
    "--shards",
    default=UpdateShardsDefaults.shards,
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
    all: bool,
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
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@update.command("data")
@click.option(
    "--collection",
    default=UpdateDataDefaults.collection,
    help="The name of the collection to update.",
)
@click.option(
    "--limit",
    default=UpdateDataDefaults.limit,
    help="Number of objects to update (default: 100).",
)
@click.option(
    "--consistency_level",
    default=UpdateDataDefaults.consistency_level,
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.option("--randomize", is_flag=True, help="Randomize the data (default: False).")
@click.option(
    "--skip-seed", is_flag=True, help="Skip seeding the random data (default: False)."
)
@click.option(
    "--verbose",
    is_flag=True,
    default=UpdateDataDefaults.verbose,
    help="Show detailed progress information (default: True).",
)
@click.pass_context
def update_data_cli(
    ctx, collection, limit, consistency_level, randomize, skip_seed, verbose
):
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
            skip_seed=skip_seed,
            verbose=verbose,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@update.command("user")
@click.option(
    "--user_name",
    default=UpdateUserDefaults.user_name,
    help="The name of the user to update.",
)
@click.option(
    "--rotate_api_key",
    is_flag=True,
    default=UpdateUserDefaults.rotate_api_key,
    help="Rotate the api key for the user.",
)
@click.option(
    "--activate",
    is_flag=True,
    default=UpdateUserDefaults.activate,
    help="Activate the user.",
)
@click.option(
    "--deactivate",
    is_flag=True,
    default=UpdateUserDefaults.deactivate,
    help="Deactivate the user.",
)
@click.option(
    "--store",
    is_flag=True,
    help="Store the rotated API key in the config file. Only works when auth type is 'user' and --rotate_api_key is used.",
)
@click.pass_context
def update_user_cli(
    ctx: click.Context,
    user_name: str,
    rotate_api_key: bool,
    activate: bool,
    deactivate: bool,
    store: bool,
) -> None:
    """Update a user in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        user_man = UserManager(client)
        api_key = user_man.update_user(
            user_name=user_name,
            rotate_api_key=rotate_api_key,
            activate=activate,
            deactivate=deactivate,
        )

        # If rotate_api_key is True and store is True, store the new API key
        if rotate_api_key and store and api_key:
            config_manager = ctx.obj.get("config")
            if not config_manager:
                click.echo(
                    "Error: --store option requires a config manager in the context."
                )
                return

            # Get the config file path from the ConfigManager
            config_path = config_manager.config_path

            # Read the current config directly from the file to ensure we have the latest
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            # Check if auth type is "user"
            if "auth" not in config or config["auth"].get("type") != "user":
                click.echo(
                    "Error: Cannot store API key. Config file must have auth type 'user'."
                )
                return

            # Add or update the user's API key
            config["auth"][user_name] = api_key

            # Write back to config file
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4)

            click.echo(
                f"API key for user '{user_name}' has been rotated and stored in config file at {config_path}"
            )
        elif rotate_api_key and api_key:
            click.echo(
                f"API key for user '{user_name}' rotated successfully: \n{api_key}"
            )
        elif activate:
            click.echo(f"User '{user_name}' activated successfully.")
        elif deactivate:
            click.echo(f"User '{user_name}' deactivated successfully.")

    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
