import sys
import click

from weaviate_cli.completion.complete import collection_name_complete
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.data_manager import DataManager
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
@click.pass_context
def query_data_cli(
    ctx, collection, search_type, query, consistency_level, limit, properties
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
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
