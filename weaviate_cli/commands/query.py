import sys
import click

from typing import Optional

from weaviate_cli.completion.complete import collection_name_complete
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.data_manager import DataManager
from weaviate_cli.managers.cluster_manager import ClusterManager
from weaviate.exceptions import WeaviateConnectionError
from weaviate_cli.defaults import QueryDataDefaults


# Query Group
@click.group()
def query() -> None:
    """Query resources in Weaviate."""
    pass


@query.command("data")
@click.option(
    "--collection",
    default=QueryDataDefaults.collection,
    help="The name of the collection to query.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--search_type",
    default=QueryDataDefaults.search_type,
    type=click.Choice(["fetch", "vector", "keyword", "hybrid", "uuid"]),
    help='Search type (default: "fetch").',
)
@click.option(
    "--query",
    default=QueryDataDefaults.query,
    help="Query string for the search. Only used when search type is vector, keyword or hybrid (default: 'Action movie').",
)
@click.option(
    "--consistency_level",
    default=QueryDataDefaults.consistency_level,
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.option(
    "--limit",
    default=QueryDataDefaults.limit,
    help="Number of objects to query (default: 10).",
)
@click.option(
    "--properties",
    default=QueryDataDefaults.properties,
    help="Properties from the object to display (default: 'title, keywords').",
)
@click.option(
    "--tenants",
    default=QueryDataDefaults.tenants,
    help="Tenants to query (default: 'None').",
)
@click.option(
    "--target_vector",
    default=QueryDataDefaults.target_vector,
    help="Target vector to query (default: 'None').",
)
@click.pass_context
def query_data_cli(
    ctx,
    collection,
    search_type,
    query,
    consistency_level,
    limit,
    properties,
    tenants,
    target_vector,
):
    """Query data in a collection in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        data_manager = DataManager(client)
        # Call the function from query_data.py with general and specific arguments
        data_manager.query_data(
            collection=collection,
            search_type=search_type,
            query=query,
            consistency_level=consistency_level,
            limit=limit,
            properties=properties,
            tenants=tenants,
            target_vector=target_vector,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@query.command(
    "replications",
    help="Query replication operations in Weaviate. If no options are provided, all replications will be queried.",
)
@click.option(
    "--collection",
    default=None,
    help="The name of the collection to query replications for.",
)
@click.option(
    "--shard",
    default=None,
    help="The shard to query replications for. If not provided, all shards will be queried.",
)
@click.option(
    "--target-node",
    default=None,
    help="The target node to query replications for. If not provided, all nodes will be queried.",
)
@click.option(
    "--history/--no-history",
    default=False,
    help="Include the history of the replication operations.",
)
@click.pass_context
def query_replications_cli(
    ctx: click.Context,
    collection: Optional[str],
    shard: Optional[str],
    target_node: Optional[str],
    history: bool,
) -> None:
    """Query replication operations by collection, collection and shard, or target node in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        manager = ClusterManager(client, click.echo)

        if shard and not collection:
            click.echo("Please provide a collection when specifying a shard.")
            sys.exit(1)

        if target_node and (collection or shard):
            click.echo(
                "Please provide either collection and shard, or node, but not both."
            )
            sys.exit(1)

        if collection and not shard:
            ops = manager.query_replications_by_collection(collection, history)
        elif collection and shard:
            ops = manager.query_replications_by_shard(collection, shard, history)
        elif target_node:
            ops = manager.query_replications_by_node(target_node, history)
        else:
            ops = manager.query_all_replications(history)

        for op in ops:
            manager.print_replication(op)
        if not ops:
            click.echo("No replication operations found.")

    except WeaviateConnectionError as e:
        click.echo(f"Connection error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()


@query.command("sharding-state")
@click.argument("collection")
@click.option(
    "--shard",
    default=None,
    help="The shard to query. If not provided, the state of all shards will be queried.",
)
@click.pass_context
def query_sharding_state_cli(
    ctx: click.Context,
    collection: str,
    shard: Optional[str],
):
    """Query the sharding state of a COLLECTION in Weaviate."""

    client = None
    try:
        client = get_client_from_context(ctx)
        manager = ClusterManager(client, click.echo)

        state = manager.query_sharding_state(collection, shard)

        if not state:
            click.echo(f"No sharding state found for collection '{collection}'")
            return

        if shard:
            click.echo(
                f"Sharding state for collection '{collection}', shard '{shard}':"
            )
        else:
            click.echo(f"Sharding state for collection '{collection}':")

        manager.print_sharding_state(state)

    except WeaviateConnectionError as e:
        click.echo(f"Connection error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
