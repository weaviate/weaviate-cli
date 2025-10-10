import asyncio
import click
import sys
from typing import Optional, List
from weaviate_cli.utils import get_async_client_from_context
from weaviate_cli.managers.benchmark_manager import BenchmarkQPSManager
from weaviate_cli.defaults import CreateBenchmarkDefaults


@click.group()
def benchmark():
    """Benchmark operations for Weaviate collections."""
    pass


@benchmark.command(
    "qps",
    help="Run QPS (Queries Per Second) benchmark on a collection.",
)
@click.option(
    "--collection",
    required=True,
    default=CreateBenchmarkDefaults.collection,
    help="The name of the collection to benchmark.",
)
@click.option(
    "--max-duration",
    default=CreateBenchmarkDefaults.max_duration,
    help="Maximum total test duration in seconds. Default is 300 seconds.",
)
@click.option(
    "--certainty",
    is_flag=True,
    default=CreateBenchmarkDefaults.certainty,
    help="Whether to compute and display certainty percentiles. Default is False.",
)
@click.option(
    "--output",
    default=CreateBenchmarkDefaults.output,
    type=click.Choice(["stdout", "csv"]),
    help="The output format of the benchmark results. Default is stdout.",
)
@click.option(
    "--query-type",
    default=CreateBenchmarkDefaults.query_type,
    type=click.Choice(["hybrid", "bm25", "near_text"]),
    help="The type of search query to perform. Default is hybrid.",
)
@click.option(
    "--consistency-level",
    default=CreateBenchmarkDefaults.consistency_level,
    type=click.Choice(["ONE", "QUORUM", "ALL"]),
    help="The consistency level to use for the benchmark. Default is QUORUM.",
)
@click.option(
    "--limit",
    default=CreateBenchmarkDefaults.limit,
    help="Limit of results per query. Default is 10.",
)
@click.option(
    "--qps",
    type=int,
    help="Fixed QPS to run the test without incrementing (optional).",
)
@click.option(
    "--warmup-duration",
    default=CreateBenchmarkDefaults.warmup_duration,
    help="Duration of warmup phase in seconds. Default is 5 seconds.",
)
@click.option(
    "--test-duration",
    default=CreateBenchmarkDefaults.test_duration,
    help="Duration of each test phase in seconds. Default is 10 seconds.",
)
@click.option(
    "--latency-threshold",
    default=CreateBenchmarkDefaults.latency_threshold,
    help="Latency threshold in milliseconds to stop the test. Default is 10000 milliseconds.",
)
@click.option(
    "--query-terms",
    multiple=True,
    help="Custom query terms to use for benchmarking (can specify multiple).",
)
@click.option(
    "--concurrency",
    type=int,
    help="Concurrency level to run the test with. By default, it will be automatically determined based on the QPS and latency.",
)
@click.pass_context
def benchmark_qps(
    ctx: click.Context,
    collection: str,
    max_duration: int,
    certainty: bool,
    output: str,
    query_type: str,
    consistency_level: str,
    limit: int,
    qps: Optional[int],
    warmup_duration: int,
    test_duration: int,
    latency_threshold: int,
    query_terms: tuple,
    concurrency: Optional[int],
) -> None:
    """Run QPS benchmark on the specified collection."""
    async_client = None
    try:
        async_client = get_async_client_from_context(ctx)
        manager = BenchmarkQPSManager(async_client)

        # Convert tuple to list for query terms
        query_terms_list = list(query_terms) if query_terms else None

        asyncio.run(
            manager.run_benchmark(
                collection=collection,
                max_duration=max_duration,
                certainty=certainty,
                output=output,
                query_type=query_type,
                consistency_level=consistency_level,
                limit=limit,
                qps=qps,
                query_terms=query_terms_list,
                warmup_duration=warmup_duration,
                test_duration=test_duration,
                latency_threshold=latency_threshold,
                concurrency=concurrency,
            )
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)
    finally:
        # The async client is managed by the context manager in the benchmark manager
        # No need to manually close it here
        pass


# Future benchmark types can be added here as new commands
# @benchmark.command("latency", help="Run latency benchmark on a collection.")
# def benchmark_latency():
#     pass

# @benchmark.command("throughput", help="Run throughput benchmark on a collection.")
# def benchmark_throughput():
#     pass
