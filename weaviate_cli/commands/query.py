import sys
import click
from weaviate_cli.utils import get_client_from_context
from weaviate_cli.managers.data_manager import DataManager
from weaviate.exceptions import WeaviateConnectionError
# Query Group
@click.group()
def query() -> None:
    """Query resources in Weaviate."""
    pass

@query.command("data")
@click.option(
    "--collection", default="Movies", help="The name of the collection to query."
)
@click.option(
    "--search_type",
    default="fetch",
    type=click.Choice(["fetch", "vector", "keyword", "hybrid"]),
    help='Search type (default: "fetch").',
)
@click.option(
    "--query",
    default="Action movie",
    help="Query string for the search. Only used when search type is vector, keyword or hybrid (default: 'Action movie').",
)
@click.option(
    "--consistency_level",
    default="quorum",
    type=click.Choice(["quorum", "all", "one"]),
    help="Consistency level (default: 'quorum').",
)
@click.option("--limit", default=10, help="Number of objects to query (default: 10).")
@click.option(
    "--properties",
    default="title,keywords",
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
    finally:
        if client:
            client.close()
            if isinstance(e, WeaviateConnectionError):
                sys.exit(1)
