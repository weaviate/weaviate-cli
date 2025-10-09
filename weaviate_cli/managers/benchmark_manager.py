import asyncio
import csv
from io import TextIOWrapper
import random
import time
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Any
from pathlib import Path
from collections import deque

import numpy as np
import click
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.filters import _FilterOr
import weaviate.classes as wvc
from weaviate_cli.defaults import CreateBenchmarkDefaults

# Constants
PER_REQUEST_TIMEOUT_S = 10.0
ROLLING_WINDOW = 5000
CONCURRENCY_SAFETY = 1.3
LATENCY_EWMA_ALPHA = 0.3
QPS_EWMA_ALPHA = 0.3


def _now_ns() -> int:
    return time.perf_counter_ns()


def _ns_to_ms(ns: int) -> float:
    return ns / 1e6


class BenchmarkManager(ABC):

    def __init__(self, async_client) -> None:
        self.async_client = async_client

    @abstractmethod
    async def run_benchmark(self, **kwargs) -> None:
        pass

    def _get_default_query_terms(self) -> List[str]:
        queries_file = Path("queries.txt")
        if queries_file.exists():
            with open(queries_file, "r") as file:
                return [line.strip() for line in file if line.strip()]
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
    ) -> Tuple[Optional[csv.writer], Optional["TextIOWrapper"]]:
        if output != "csv":
            return None, None
        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp_str}.csv"
        try:
            csv_file = open(filename, "w", newline="")
        except Exception as e:
            click.echo(f"Failed to open CSV file '{filename}': {e}")
            return None, None
        try:
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
        except Exception as e:
            csv_file.close()
            click.echo(f"Failed to write header to CSV file '{filename}': {e}")
            return None, None

    async def _run_query_and_collect_latency(
        self,
        collection_obj: CollectionAsync,
        query_term: str,
        limit: int,
        filters: Optional[List],
        query_type: str,
    ) -> Tuple[Optional[int], Optional[List[float]]]:
        t0 = _now_ns()
        try:

            async def _do():
                if query_type == "hybrid":
                    return await collection_obj.query.hybrid(
                        query=query_term,
                        filters=None if not filters else _FilterOr(filters),
                        return_metadata=wvc.query.MetadataQuery(certainty=True),
                        limit=limit,
                    )
                elif query_type == "bm25":
                    return await collection_obj.query.bm25(
                        query=query_term,
                        filters=None if not filters else _FilterOr(filters),
                        return_metadata=wvc.query.MetadataQuery(certainty=True),
                        limit=limit,
                    )
                elif query_type == "near_text":
                    return await collection_obj.query.near_text(
                        query=query_term,
                        filters=None if not filters else _FilterOr(filters),
                        return_metadata=wvc.query.MetadataQuery(certainty=True),
                        limit=limit,
                    )
                else:
                    raise ValueError(f"Unsupported query type: {query_type}")

            response = await asyncio.wait_for(_do(), timeout=PER_REQUEST_TIMEOUT_S)
            took_ms = int(_ns_to_ms(_now_ns() - t0))
            certainties = [
                o.metadata.certainty
                for o in response.objects
                if hasattr(o.metadata, "certainty")
            ]
            return took_ms, certainties

        except asyncio.TimeoutError:
            click.echo(
                f"Query '{query_term[:30]}...' timed out after {PER_REQUEST_TIMEOUT_S}s"
            )
            return None, None
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
        if not response_times:
            return
        tail = response_times[-ROLLING_WINDOW:]
        p50 = np.percentile(tail, 50)
        p90 = np.percentile(tail, 90)
        p95 = np.percentile(tail, 95)
        p99 = np.percentile(tail, 99)
        click.echo(f"Current P50 latency: {p50:.2f} ms")
        click.echo(f"Current P90 latency: {p90:.2f} ms")
        click.echo(f"Current P95 latency: {p95:.2f} ms")
        click.echo(f"Current P99 latency: {p99:.2f} ms")
        if show_certainty and certainty_values:
            ctail = certainty_values[-ROLLING_WINDOW:]
            p50c = np.percentile(ctail, 50)
            p90c = np.percentile(ctail, 90)
            p95c = np.percentile(ctail, 95)
            p99c = np.percentile(ctail, 99)
            click.echo(f"Current P50 certainty: {p50c:.2f}")
            click.echo(f"Current P90 certainty: {p90c:.2f}")
            click.echo(f"Current P95 certainty: {p95c:.2f}")
            click.echo(f"Current P99 certainty: {p99c:.2f}")
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
                row.extend([f"{p50c:.2f}", f"{p90c:.2f}", f"{p95c:.2f}", f"{p99c:.2f}"])
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
        if phase_name != "Main Test":
            return
        if not response_times:
            click.echo("No successful queries were completed in Main Test.")
            return
        p50 = np.percentile(response_times, 50)
        p90 = np.percentile(response_times, 90)
        p95 = np.percentile(response_times, 95)
        p99 = np.percentile(response_times, 99)
        total_queries = len(response_times)
        actual_qps = total_queries / max(actual_duration, 1e-9)
        click.echo(f"\n-------------------------------\n")
        click.echo(f"Total queries: {total_queries}")
        click.echo(f"Actual QPS: {actual_qps:.2f}")
        click.echo(f"P50 latency: {p50:.2f} ms")
        click.echo(f"P90 latency: {p90:.2f} ms")
        click.echo(f"P95 latency: {p95:.2f} ms")
        click.echo(f"P99 latency: {p99:.2f} ms")
        max_latency = max(response_times)
        click.echo(f"Max observed latency: {max_latency:.2f} ms\n")
        if show_certainty and certainty_values:
            p50c = np.percentile(certainty_values, 50)
            p90c = np.percentile(certainty_values, 90)
            p95c = np.percentile(certainty_values, 95)
            p99c = np.percentile(certainty_values, 99)
            click.echo(f"P50 certainty: {p50c:.2f}")
            click.echo(f"P90 certainty: {p90c:.2f}")
            click.echo(f"P95 certainty: {p95c:.2f}")
            click.echo(f"P99 certainty: {p99c:.2f}")
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
                row.extend([f"{p50c:.2f}", f"{p90c:.2f}", f"{p95c:.2f}", f"{p99c:.2f}"])
            row.extend([f"{total_queries}", f"{actual_qps:.2f}"])
            csv_writer.writerow(row)


class BenchmarkQPSManager(BenchmarkManager):
    async def run_benchmark(
        self,
        collection: str,
        max_duration: int = CreateBenchmarkDefaults.max_duration,
        certainty: bool = CreateBenchmarkDefaults.certainty,
        output: str = CreateBenchmarkDefaults.output,
        query_type: str = CreateBenchmarkDefaults.query_type,
        consistency_level: str = CreateBenchmarkDefaults.consistency_level,
        limit: int = CreateBenchmarkDefaults.limit,
        qps: Optional[int] = CreateBenchmarkDefaults.qps,
        query_terms: Optional[List[str]] = CreateBenchmarkDefaults.query_terms,
        warmup_duration: int = CreateBenchmarkDefaults.warmup_duration,
        test_duration: int = CreateBenchmarkDefaults.test_duration,
        latency_threshold: int = CreateBenchmarkDefaults.latency_threshold,
        concurrency: Optional[int] = CreateBenchmarkDefaults.concurrency,
    ) -> None:
        consistency_map = {
            "ONE": wvc.ConsistencyLevel.ONE,
            "QUORUM": wvc.ConsistencyLevel.QUORUM,
            "ALL": wvc.ConsistencyLevel.ALL,
        }
        if not query_terms:
            query_terms = self._get_default_query_terms()
        csv_writer, csv_file = self._setup_csv_output(output, certainty)
        try:
            await self.async_client.connect()
            if not await self.async_client.collections.exists(collection):
                raise Exception(f"Collection '{collection}' does not exist")
            collection_obj = self.async_client.collections.get(
                collection
            ).with_consistency_level(consistency_map[consistency_level])
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
                concurrency=concurrency,
            )
            if not qps:
                click.echo(f"\nThe maximum sustainable QPS is approximately {max_qps}.")
        finally:
            if csv_file:
                csv_file.close()
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
        concurrency: Optional[int] = None,
    ) -> Tuple[List[int], bool]:
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        end_time = start_time + duration
        interval = 1.0 / qps if qps > 0 else float("inf")

        response_times: List[int] = []
        certainty_values: List[float] = []
        latency_exceeded = False
        goto_finally = False

        recent_latencies = deque(maxlen=ROLLING_WINDOW)
        ewma_latency_ms: Optional[float] = None

        completed_total = 0
        ewma_qps = float(qps) if qps > 0 else 0.0
        last_completed_total = 0
        last_report_time = start_time

        def auto_concurrency() -> int:
            if ewma_latency_ms is None:
                return max(1, min(qps or 1, 8))
            avg_sec = max(ewma_latency_ms / 1000.0, 0.001)
            target = int((qps or 1) * avg_sec * CONCURRENCY_SAFETY) or 1
            cap = max((qps or 1) * 4, 32)
            return max(1, min(target, cap))

        initial_conc = concurrency if concurrency is not None else auto_concurrency()
        sem = asyncio.Semaphore(initial_conc)
        queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, initial_conc * 2))

        click.echo(
            f"Starting {phase_name} phase at {qps} QPS for {duration} seconds "
            f"with {'auto' if concurrency is None else f'concurrency={concurrency}'}."
        )

        async def worker():
            nonlocal latency_exceeded, goto_finally, ewma_latency_ms, completed_total
            while not goto_finally:
                try:
                    query_term, _ = await queue.get()
                except asyncio.CancelledError:
                    break
                try:
                    async with sem:
                        took, certainties = await self._run_query_and_collect_latency(
                            collection_obj, query_term, limit, [], query_type
                        )
                except asyncio.CancelledError:
                    took, certainties = None, None
                finally:
                    if took is not None:
                        response_times.append(took)
                        recent_latencies.append(took)
                        ewma_latency_ms = (
                            float(took)
                            if ewma_latency_ms is None
                            else LATENCY_EWMA_ALPHA * float(took)
                            + (1 - LATENCY_EWMA_ALPHA) * ewma_latency_ms
                        )
                        completed_total += 1
                        if phase_name == "Main Test" and took > latency_threshold:
                            latency_exceeded = True
                            goto_finally = True
                        if show_certainty and certainties is not None:
                            certainty_values.extend(certainties)
                    queue.task_done()
                    if latency_exceeded:
                        goto_finally = True

        workers = [asyncio.create_task(worker()) for _ in range(initial_conc)]

        async def producer():
            next_t = loop.time()
            while loop.time() < end_time and not goto_finally:
                await asyncio.sleep(max(0.0, next_t - loop.time()))
                if goto_finally:
                    break
                await queue.put((random.choice(query_terms), loop.time()))
                next_t += interval
                now = loop.time()
                if now - next_t > interval:
                    missed = int((now - next_t) / interval)
                    next_t += missed * interval

        async def reporter():
            nonlocal ewma_qps, last_completed_total, last_report_time
            while loop.time() < end_time and not goto_finally:
                await asyncio.sleep(1.0)
                if response_times:
                    self._report_percentiles(
                        response_times,
                        certainty_values,
                        show_certainty,
                        csv_writer,
                        phase_name,
                    )
                    now = loop.time()
                    dt = max(1e-6, now - last_report_time)
                    delta = completed_total - last_completed_total
                    inst_rate = delta / dt
                    ewma_qps = (
                        QPS_EWMA_ALPHA * inst_rate + (1 - QPS_EWMA_ALPHA) * ewma_qps
                    )
                    last_completed_total = completed_total
                    last_report_time = now
                    click.echo(f"Current QPS: {ewma_qps:.2f}\n")

        prod_task = asyncio.create_task(producer())
        report_task = asyncio.create_task(reporter())

        try:
            await asyncio.wait([prod_task], return_when=asyncio.ALL_COMPLETED)
            try:
                await asyncio.wait_for(queue.join(), timeout=15.0)
            except asyncio.TimeoutError:
                pass
        finally:
            goto_finally = True
            for _ in range(len(workers)):
                try:
                    queue.put_nowait((random.choice(query_terms), loop.time()))
                except asyncio.QueueFull:
                    break
            report_task.cancel()
            for t in workers:
                t.cancel()
            try:
                await asyncio.gather(report_task, *workers, return_exceptions=True)
            except Exception:
                pass

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
        concurrency: Optional[int] = None,
    ) -> int:
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
                concurrency=concurrency,
            )
            return fixed_qps

        qps = 10
        max_qps_reached = False
        latency_exceeded = False

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
                concurrency=concurrency,
            )

            _, latency_exceeded = await self._run_phase(
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
                concurrency=concurrency,
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
