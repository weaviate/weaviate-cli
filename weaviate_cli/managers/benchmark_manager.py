import asyncio
import csv
import random
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

import numpy as np
import click
from weaviate.collections import Collection, CollectionAsync
from weaviate.collections.classes.filters import _FilterOr
import weaviate.classes as wvc
from weaviate.classes.init import AdditionalConfig, Timeout


class BenchmarkManager(ABC):
    """Abstract base class for benchmark managers."""

    def __init__(self, async_client) -> None:
        self.async_client = async_client

    @abstractmethod
    async def run_benchmark(self, **kwargs) -> None:
        """Run the benchmark. Must be implemented by subclasses."""
        pass

    def _get_default_query_terms(self) -> List[str]:
        """Get default query terms for benchmarking."""
        # Try to read from queries.txt if it exists
        queries_file = Path("queries.txt")
        if queries_file.exists():
            with open(queries_file, "r") as file:
                return [line.strip() for line in file if line.strip()]

        # Fallback to default terms
        return [
            "action movie",
            "comedy film",
            "drama series",
            "sci-fi thriller",
            "romantic comedy",
            "horror movie",
            "documentary",
            "animation",
            "adventure film",
            "mystery thriller",
        ]

    def _setup_csv_output(
        self, output: str, certainty: bool
    ) -> Tuple[Optional[csv.writer], Optional[Any]]:
        """Setup CSV output if requested."""
        if output != "csv":
            return None, None

        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp_str}.csv"
        csv_file = open(filename, "w", newline="")
        csv_writer = csv.writer(csv_file)

        header = [
            "timestamp",
            "phase_name",
            "p50_latency",
            "p90_latency",
            "p95_latency",
            "p99_latency",
        ]
        if certainty:
            header += [
                "p50_certainty",
                "p90_certainty",
                "p95_certainty",
                "p99_certainty",
            ]
        header += ["total_queries", "actual_qps"]
        csv_writer.writerow(header)

        return csv_writer, csv_file

    async def _run_query_and_collect_latency(
        self,
        collection_obj: CollectionAsync,
        query_term: str,
        limit: int,
        filters: Optional[List],
        query_type: str,
    ) -> Tuple[Optional[int], Optional[List[float]]]:
        """Run a single query and collect latency and certainty data."""
        start_time = time.time()

        try:
            if query_type == "hybrid":
                response = await collection_obj.query.hybrid(
                    query=query_term,
                    filters=None if not filters else _FilterOr(filters),
                    return_metadata=wvc.query.MetadataQuery(certainty=True),
                    limit=limit,
                )
            elif query_type == "bm25":
                response = await collection_obj.query.bm25(
                    query=query_term,
                    filters=None if not filters else _FilterOr(filters),
                    return_metadata=wvc.query.MetadataQuery(certainty=True),
                    limit=limit,
                )
            elif query_type == "near_text":
                response = await collection_obj.query.near_text(
                    query=query_term,
                    filters=None if not filters else _FilterOr(filters),
                    return_metadata=wvc.query.MetadataQuery(certainty=True),
                    limit=limit,
                )
            else:
                raise ValueError(f"Unsupported query type: {query_type}")

            took = int((time.time() - start_time) * 1000)
            actual_results = len(response.objects)

            certainties = [
                o.metadata.certainty
                for o in response.objects
                if hasattr(o.metadata, "certainty")
            ]

            click.echo(f"Query '{query_term[:30]}...' took: {took}ms")

            return took, certainties

        except asyncio.CancelledError:
            return None, None
        except Exception as exc:
            click.echo(f"Query '{query_term[:30]}...' generated an exception: {exc}")
            return None, None

    def _report_percentiles(
        self,
        response_times: List[int],
        certainty_values: List[float],
        show_certainty: bool,
        csv_writer: Optional[csv.writer],
        phase_name: str,
    ) -> None:
        """Report current percentiles during the benchmark."""
        p50 = np.percentile(response_times, 50)
        p90 = np.percentile(response_times, 90)
        p95 = np.percentile(response_times, 95)
        p99 = np.percentile(response_times, 99)

        click.echo(f"Current P50 latency: {p50:.2f} ms")
        click.echo(f"Current P90 latency: {p90:.2f} ms")
        click.echo(f"Current P95 latency: {p95:.2f} ms")
        click.echo(f"Current P99 latency: {p99:.2f} ms\n")

        if show_certainty and certainty_values:
            p50_certainty = np.percentile(certainty_values, 50)
            p90_certainty = np.percentile(certainty_values, 90)
            p95_certainty = np.percentile(certainty_values, 95)
            p99_certainty = np.percentile(certainty_values, 99)

            click.echo(f"Current P50 certainty: {p50_certainty:.2f}")
            click.echo(f"Current P90 certainty: {p90_certainty:.2f}")
            click.echo(f"Current P95 certainty: {p95_certainty:.2f}")
            click.echo(f"Current P99 certainty: {p99_certainty:.2f}\n")

        if csv_writer:
            row = [
                time.time(),
                phase_name,
                f"{p50:.2f}",
                f"{p90:.2f}",
                f"{p95:.2f}",
                f"{p99:.2f}",
            ]
            if show_certainty and certainty_values:
                row.extend(
                    [
                        f"{p50_certainty:.2f}",
                        f"{p90_certainty:.2f}",
                        f"{p95_certainty:.2f}",
                        f"{p99_certainty:.2f}",
                    ]
                )
            row.extend(["", ""])
            csv_writer.writerow(row)

    def _report_final_results(
        self,
        response_times: List[int],
        certainty_values: List[float],
        show_certainty: bool,
        csv_writer: Optional[csv.writer],
        phase_name: str,
        actual_duration: float,
    ) -> None:
        """Report final results for a benchmark phase."""
        if phase_name == "Main Test":
            p50 = np.percentile(response_times, 50)
            p90 = np.percentile(response_times, 90)
            p95 = np.percentile(response_times, 95)
            p99 = np.percentile(response_times, 99)

            total_queries = len(response_times)
            actual_qps = total_queries / actual_duration

            click.echo(f"Total queries: {total_queries}")
            click.echo(f"Actual QPS: {actual_qps:.2f}")
            click.echo(f"P50 latency: {p50:.2f} ms")
            click.echo(f"P90 latency: {p90:.2f} ms")
            click.echo(f"P95 latency: {p95:.2f} ms")
            click.echo(f"P99 latency: {p99:.2f} ms\n")

            if show_certainty and certainty_values:
                p50_certainty = np.percentile(certainty_values, 50)
                p90_certainty = np.percentile(certainty_values, 90)
                p95_certainty = np.percentile(certainty_values, 95)
                p99_certainty = np.percentile(certainty_values, 99)

                click.echo(f"P50 certainty: {p50_certainty:.2f}")
                click.echo(f"P90 certainty: {p90_certainty:.2f}")
                click.echo(f"P95 certainty: {p95_certainty:.2f}")
                click.echo(f"P99 certainty: {p99_certainty:.2f}")

            if csv_writer:
                row = [
                    time.time(),
                    phase_name,
                    f"{p50:.2f}",
                    f"{p90:.2f}",
                    f"{p95:.2f}",
                    f"{p99:.2f}",
                ]
                if show_certainty and certainty_values:
                    row.extend(
                        [
                            f"{p50_certainty:.2f}",
                            f"{p90_certainty:.2f}",
                            f"{p95_certainty:.2f}",
                            f"{p99_certainty:.2f}",
                        ]
                    )
                row.extend([f"{total_queries}", f"{actual_qps:.2f}"])
                csv_writer.writerow(row)


class BenchmarkQPSManager(BenchmarkManager):
    """Manager for QPS (Queries Per Second) benchmarking."""

    async def run_benchmark(
        self,
        collection: str,
        max_duration: int = 300,
        certainty: bool = False,
        output: str = "stdout",
        query_type: str = "hybrid",
        limit: int = 10,
        qps: Optional[int] = None,
        query_terms: Optional[List[str]] = None,
        warmup_duration: int = 5,
        test_duration: int = 10,
        latency_threshold: int = 10000,
    ) -> None:
        """Run QPS benchmark on the specified collection."""

        # Get query terms
        if not query_terms:
            query_terms = self._get_default_query_terms()

        # Setup CSV output if requested
        csv_writer, csv_file = self._setup_csv_output(output, certainty)

        try:
            # Ensure the client is connected
            await self.async_client.connect()

            # Check if collection exists
            if not await self.async_client.collections.exists(collection):
                raise Exception(f"Collection '{collection}' does not exist")

            # Get the collection object - this returns a CollectionAsync, not awaitable
            collection_obj = self.async_client.collections.get(
                collection
            ).with_consistency_level(wvc.ConsistencyLevel.ONE)

            max_qps = await self._find_max_qps(
                collection_obj=collection_obj,
                query_terms=query_terms,
                limit=limit,
                max_duration=max_duration,
                fixed_qps=qps,
                show_certainty=certainty,
                csv_writer=csv_writer,
                query_type=query_type,
                warmup_duration=warmup_duration,
                test_duration=test_duration,
                latency_threshold=latency_threshold,
            )

            if not qps:
                click.echo(f"\nThe maximum sustainable QPS is approximately {max_qps}.")

        finally:
            if csv_file:
                csv_file.close()
            # Ensure the client is properly closed
            await self.async_client.close()

    async def _run_phase(
        self,
        collection_obj: CollectionAsync,
        query_terms: List[str],
        limit: int,
        qps: int,
        duration: int,
        phase_name: str,
        show_certainty: bool,
        csv_writer: Optional[csv.writer],
        query_type: str,
        latency_threshold: int,
    ) -> Tuple[List[int], bool]:
        """Run a benchmark phase at a specific QPS."""
        interval = 1 / qps
        loop = asyncio.get_event_loop()
        start_time = loop.time()
        end_time = start_time + duration
        response_times = []
        certainty_values = []
        tasks = []
        latency_exceeded = False

        click.echo(f"Starting {phase_name} phase at {qps} QPS for {duration} seconds.")

        async def schedule_tasks():
            next_scheduled_time = loop.time()
            while loop.time() < end_time and not latency_exceeded and not goto_finally:
                # Check if we should stop early
                if latency_exceeded or goto_finally:
                    break

                query_term = random.choice(query_terms)
                filters = []

                task = asyncio.create_task(
                    self._run_query_and_collect_latency(
                        collection_obj,
                        query_term,
                        limit,
                        filters,
                        query_type,
                    )
                )
                tasks.append(task)

                next_scheduled_time += interval
                sleep_duration = next_scheduled_time - loop.time()
                if sleep_duration > 0:
                    # Use shorter sleep intervals to be more responsive to cancellation
                    await asyncio.sleep(min(sleep_duration, 0.1))
                else:
                    next_scheduled_time = loop.time()

        async def report_periodically():
            while loop.time() < end_time and not latency_exceeded and not goto_finally:
                # Use shorter sleep intervals to be more responsive to cancellation
                await asyncio.sleep(1)
                if response_times:
                    self._report_percentiles(
                        response_times,
                        certainty_values,
                        show_certainty,
                        csv_writer,
                        phase_name,
                    )

        scheduler_task = asyncio.create_task(schedule_tasks())
        reporting_task = asyncio.create_task(report_periodically())

        completed_tasks = 0
        goto_finally = False
        try:
            # Add timeout to prevent hanging
            start_wait_time = loop.time()
            while (
                not latency_exceeded
                and not goto_finally
                and (loop.time() < end_time or tasks)
            ):
                # Check for timeout
                if (
                    loop.time() - start_wait_time > duration + 30
                ):  # Add 30 second buffer
                    click.echo(
                        "Warning: Test taking longer than expected, forcing exit..."
                    )
                    latency_exceeded = True
                    goto_finally = True
                    break

                if tasks:
                    try:
                        # Use timeout for wait to prevent hanging
                        done, pending = await asyncio.wait_for(
                            asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED),
                            timeout=1.0,  # 1 second timeout
                        )
                        for task in done:
                            tasks.remove(task)
                            took, certainties = task.result()
                            if took is not None:
                                response_times.append(took)
                                if (
                                    phase_name == "Main Test"
                                    and took > latency_threshold
                                ):
                                    latency_exceeded = True
                                    click.echo(
                                        f"Error: Latency exceeded {latency_threshold} ms. Stopping the test."
                                    )
                                    # Cancel all remaining tasks immediately
                                    for remaining_task in tasks:
                                        remaining_task.cancel()
                                    scheduler_task.cancel()
                                    reporting_task.cancel()
                                    # Force exit from the main loop
                                    goto_finally = True
                                    break
                            completed_tasks += 1
                            if show_certainty and certainties is not None:
                                certainty_values.extend(certainties)

                        # Check if we should exit due to latency threshold
                        if latency_exceeded:
                            break

                    except asyncio.TimeoutError:
                        # If wait times out, continue to next iteration
                        continue
                else:
                    await asyncio.sleep(0.01)

                # Check if we should stop early due to latency threshold
                if latency_exceeded or goto_finally:
                    break

        except asyncio.CancelledError:
            pass
        finally:
            # Cancel all remaining tasks
            for task in tasks:
                task.cancel()

            # Wait for all tasks to complete (with timeout to prevent hanging)
            if tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    click.echo(
                        "Warning: Some tasks took too long to cancel, continuing..."
                    )

            # Cancel and wait for scheduler and reporting tasks
            scheduler_task.cancel()
            reporting_task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        scheduler_task, reporting_task, return_exceptions=True
                    ),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                click.echo(
                    "Warning: Scheduler/reporting tasks took too long to cancel, continuing..."
                )

        actual_duration = loop.time() - start_time
        if response_times:
            self._report_final_results(
                response_times,
                certainty_values,
                show_certainty,
                csv_writer,
                phase_name,
                actual_duration,
            )
        else:
            click.echo("No successful queries were completed during the test.")

        return response_times, latency_exceeded

    async def _find_max_qps(
        self,
        collection_obj: CollectionAsync,
        query_terms: List[str],
        limit: int,
        max_duration: int,
        fixed_qps: Optional[int],
        show_certainty: bool,
        csv_writer: Optional[csv.writer],
        query_type: str,
        warmup_duration: int,
        test_duration: int,
        latency_threshold: int,
    ) -> int:
        """Find the maximum sustainable QPS for the collection."""
        if fixed_qps:
            click.echo(f"\nRunning test at fixed QPS of {fixed_qps}")
            await self._run_phase(
                collection_obj,
                query_terms,
                limit,
                fixed_qps,
                max_duration,
                "Main Test",
                show_certainty,
                csv_writer,
                query_type,
                latency_threshold,
            )
            return fixed_qps

        qps = 10
        max_qps_reached = False

        while not max_qps_reached:
            click.echo(f"\nTesting at {qps} QPS")
            warmup_qps = max(1, qps // 10)

            await self._run_phase(
                collection_obj,
                query_terms,
                limit,
                warmup_qps,
                warmup_duration,
                "Warmup",
                show_certainty,
                csv_writer,
                query_type,
                latency_threshold,
            )

            response_times, latency_exceeded = await self._run_phase(
                collection_obj,
                query_terms,
                limit,
                qps,
                test_duration,
                "Main Test",
                show_certainty,
                csv_writer,
                query_type,
                latency_threshold,
            )

            if latency_exceeded:
                click.echo(
                    f"Latency threshold exceeded at {qps} QPS test phase. Stopping test."
                )
                max_qps_reached = True
            else:
                max_duration -= test_duration + warmup_duration
                if max_duration <= 0:
                    click.echo("Maximum test duration reached. Stopping test.")
                    max_qps_reached = True
                else:
                    qps += 10

        return qps - 10 if latency_exceeded else qps


# Example of how to add new benchmark types in the future:
# class BenchmarkRecallManager(BenchmarkManager):
#     """Manager for recall benchmarking."""
#
#     async def run_benchmark(self, **kwargs) -> None:
#         # Implementation for recall benchmarking
#         pass
#
# class BenchmarkThroughputManager(BenchmarkManager):
#     """Manager for throughput benchmarking."""
#
#     async def run_benchmark(self, **kwargs) -> None:
#         # Implementation for throughput benchmarking
#         pass
