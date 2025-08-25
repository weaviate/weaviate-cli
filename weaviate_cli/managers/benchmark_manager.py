import asyncio
import csv
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

    def _setup_csv_output(self, output: str, certainty: bool) -> Tuple[Optional[csv.writer], Optional[Any]]:
        if output != "csv":
            return None, None
        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp_str}.csv"
        csv_file = open(filename, "w", newline="")
        csv_writer = csv.writer(csv_file)
        header = ["timestamp", "phase_name", "p50_latency", "p90_latency", "p95_latency", "p99_latency"]
        if certainty:
            header += ["p50_certainty", "p90_certainty", "p95_certainty", "p99_certainty"]
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
            certainties = [o.metadata.certainty for o in response.objects if hasattr(o.metadata, "certainty")]
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
        if not response_times:
            return
        p50 = np.percentile(response_times, 50)
        p90 = np.percentile(response_times, 90)
        p95 = np.percentile(response_times, 95)
        p99 = np.percentile(response_times, 99)
        click.echo(f"Current P50 latency: {p50:.2f} ms")
        click.echo(f"Current P90 latency: {p90:.2f} ms")
        click.echo(f"Current P95 latency: {p95:.2f} ms")
        click.echo(f"Current P99 latency: {p99:.2f} ms\n")

        if show_certainty and certainty_values:
            p50c = np.percentile(certainty_values, 50)
            p90c = np.percentile(certainty_values, 90)
            p95c = np.percentile(certainty_values, 95)
            p99c = np.percentile(certainty_values, 99)
            click.echo(f"Current P50 certainty: {p50c:.2f}")
            click.echo(f"Current P90 certainty: {p90c:.2f}")
            click.echo(f"Current P95 certainty: {p95c:.2f}")
            click.echo(f"Current P99 certainty: {p99c:.2f}")

        if csv_writer:
            row = [time.time(), phase_name, f"{p50:.2f}", f"{p90:.2f}", f"{p95:.2f}", f"{p99:.2f}"]
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
        click.echo(f"Total queries: {total_queries}")
        click.echo(f"Actual QPS: {actual_qps:.2f}")
        click.echo(f"P50 latency: {p50:.2f} ms")
        click.echo(f"P90 latency: {p90:.2f} ms")
        click.echo(f"P95 latency: {p95:.2f} ms")
        click.echo(f"P99 latency: {p99:.2f} ms\n")

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
            row = [time.time(), phase_name, f"{p50:.2f}", f"{p90:.2f}", f"{p95:.2f}", f"{p99:.2f}"]
            if show_certainty and certainty_values:
                row.extend([f"{p50c:.2f}", f"{p90c:.2f}", f"{p95c:.2f}", f"{p99c:.2f}"])
            row.extend([f"{total_queries}", f"{actual_qps:.2f}"])
            csv_writer.writerow(row)


class BenchmarkQPSManager(BenchmarkManager):
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
            concurrency: Optional[int] = None,  # if None => auto (scale-up-only)
    ) -> None:
        if not query_terms:
            query_terms = self._get_default_query_terms()

        csv_writer, csv_file = self._setup_csv_output(output, certainty)

        try:
            await self.async_client.connect()
            if not await self.async_client.collections.exists(collection):
                raise Exception(f"Collection '{collection}' does not exist")

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
            concurrency: Optional[int] = None,  # None => auto (scale-up-only)
    ) -> Tuple[List[int], bool]:
        """
        Run a benchmark phase at a specific QPS with bounded or adaptive concurrency.
        Auto mode uses scale-up-only: it increases workers based on observed latency, never decreases mid-phase.
        """
        loop = asyncio.get_event_loop()
        start_time = loop.time()
        end_time = start_time + duration
        interval = 1.0 / qps if qps > 0 else float("inf")

        response_times: List[int] = []
        certainty_values: List[float] = []
        latency_exceeded = False
        goto_finally = False

        # Adaptive bits
        SAFETY = 1.3
        EWMA_ALPHA = 0.3
        ewma_latency_ms: Optional[float] = None
        recent_latencies = deque(maxlen=2000)
        # QPS tracking (smooth & warm-started)
        completed_total = 0
        ewma_qps = float(qps)
        last_completed_total = 0
        last_report_time = start_time
        QPS_EWMA_ALPHA = 0.3

        # Worker pool (we only scale up)
        workers: List[asyncio.Task] = []

        def active_count() -> int:
            return sum(1 for t in workers if not t.done())

        def current_target_workers() -> int:
            if concurrency is not None:
                return max(1, concurrency)
            if ewma_latency_ms is None:
                # start small but viable
                return max(1, min(qps, 8))
            avg_sec = max(ewma_latency_ms / 1000.0, 0.001)
            target = int(qps * avg_sec * SAFETY) or 1
            cap = max(qps * 4, 32)
            return max(1, min(target, cap))

        # Bounded queue for backpressure; sized to initial target (will be conservative)
        queue_maxsize = max(1, current_target_workers() * 2)
        queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)

        click.echo(
            f"Starting {phase_name} phase at {qps} QPS for {duration} seconds "
            f"with {'auto' if concurrency is None else f'concurrency={concurrency}'}."
        )

        async def worker():
            nonlocal latency_exceeded, goto_finally, ewma_latency_ms, completed_total
            while not goto_finally:
                try:
                    item = await queue.get()
                except asyncio.CancelledError:
                    break
                query_term = item
                took = None
                certainties = None
                try:
                    took, certainties = await self._run_query_and_collect_latency(
                        collection_obj, query_term, limit, [], query_type
                    )
                except asyncio.CancelledError:
                    pass
                finally:
                    if took is not None:
                        response_times.append(took)
                        recent_latencies.append(took)
                        ewma_latency_ms = (
                            took if ewma_latency_ms is None
                            else EWMA_ALPHA * took + (1 - EWMA_ALPHA) * ewma_latency_ms
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
                        break

        async def ensure_workers_at_least(target: int):
            # Only scale UP; never down during the phase
            cur = active_count()
            add = max(0, target - cur)
            for _ in range(add):
                workers.append(asyncio.create_task(worker()))

        async def controller():
            # Kick off initial pool
            await ensure_workers_at_least(current_target_workers())
            while loop.time() < end_time and not goto_finally:
                await asyncio.sleep(1.0)
                if goto_finally:
                    break
                await ensure_workers_at_least(current_target_workers())

        async def producer():
            """Metronome: one job per tick. Backpressure if queue fills."""
            nonlocal goto_finally
            next_t = loop.time()
            while loop.time() < end_time and not goto_finally:
                await asyncio.sleep(max(0.0, next_t - loop.time()))
                if goto_finally:
                    break
                query_term = random.choice(query_terms)
                await queue.put(query_term)  # blocks if workers can't keep up
                next_t += interval
                now = loop.time()
                # Drift correction: skip missed ticks if we fell behind
                if now - next_t > interval:
                    missed = int((now - next_t) / interval)
                    next_t += missed * interval

        async def reporter():
            nonlocal ewma_qps, last_completed_total, last_report_time
            while loop.time() < end_time and not goto_finally:
                await asyncio.sleep(1.0)
                if response_times:
                    self._report_percentiles(
                        response_times, certainty_values, show_certainty, csv_writer, phase_name
                    )
                    now = loop.time()
                    dt = max(1e-6, now - last_report_time)
                    delta = completed_total - last_completed_total
                    inst_rate = delta / dt
                    ewma_qps = QPS_EWMA_ALPHA * inst_rate + (1 - QPS_EWMA_ALPHA) * ewma_qps
                    last_completed_total = completed_total
                    last_report_time = now

                    click.echo(f"Current QPS: {ewma_qps:.2f}\n")

        prod_task = asyncio.create_task(producer())
        ctrl_task = asyncio.create_task(controller())
        report_task = asyncio.create_task(reporter())

        try:
            # Wait producer+controller to end (time-bound)
            await asyncio.wait([prod_task, ctrl_task], return_when=asyncio.ALL_COMPLETED)
            # Drain remaining work
            await queue.join()
        finally:
            goto_finally = True
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
                response_times, certainty_values, show_certainty, csv_writer, phase_name, actual_duration
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
            concurrency: Optional[int] = None,  # None => auto
    ) -> int:
        if fixed_qps:
            click.echo(f"\nRunning test at fixed QPS of {fixed_qps}")
            await self._run_phase(
                collection_obj, query_terms, limit, fixed_qps, max_duration, "Main Test",
                show_certainty, csv_writer, query_type, latency_threshold,
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
                collection_obj, query_terms, limit, warmup_qps, warmup_duration, "Warmup",
                show_certainty, csv_writer, query_type, latency_threshold,
                concurrency=concurrency,  # auto or fixed during warmup too
            )

            _, latency_exceeded = await self._run_phase(
                collection_obj, query_terms, limit, qps, test_duration, "Main Test",
                show_certainty, csv_writer, query_type, latency_threshold,
                concurrency=concurrency,
            )

            if latency_exceeded:
                click.echo(f"Latency threshold exceeded at {qps} QPS test phase. Stopping test.")
                max_qps_reached = True
            else:
                max_duration -= test_duration + warmup_duration
                if max_duration <= 0:
                    click.echo("Maximum test duration reached. Stopping test.")
                    max_qps_reached = True
                else:
                    qps += 10

        return qps - 10 if latency_exceeded else qps
