"""Async benchmark engine.

Concurrency model: a single asyncio event loop with **one client per
connection** and ``pipeline_depth`` worker coroutines per connection, all run
concurrently. Each worker awaits one command at a time, so a connection has up
to ``pipeline_depth`` requests in flight. At the default depth of 1 this is the
same "one in-flight request per connection" shape the Java engine's
virtual-thread workers produce.

A connection's key generator and command selector are owned by the connection,
not by the individual worker, so raising ``pipeline_depth`` changes how many
requests are outstanding but not which keys a connection issues. This matches
the C# engine, where one worker owns the generator for all of its pipeline
slots.

A request-based phase target is a single budget shared across all workers, which
they claim from one request at a time -- matching the Java reference's shared
``AtomicLong`` rather than pre-splitting the target per worker.

What ``pipeline_depth > 1`` costs physically depends on the driver, and the two
supported drivers differ. GLIDE multiplexes, so the extra requests ride the one
socket. redis-py serves each concurrent command from its connection pool, so a
client uses up to ``pipeline_depth`` sockets -- bounded there by
``set_max_in_flight``, and recorded in the metrics metadata as ``pipelining`` so
a run says which mechanism produced it. There is no way to give redis-py real
depth on a single socket: ``single_connection_client`` serialises behind a lock,
and ``pipeline()`` batches N commands into one round trip with one shared
latency, which measures something else entirely.

Known limit of this model, deliberate:

* **Single event loop.** Above roughly ``HIGH_CONNECTION_WARN_THRESHOLD``
  concurrent in-flight requests (``connections x pipeline_depth``) the loop
  itself, not the driver, becomes the bottleneck, and the loop's queuing delay
  is attributed to the driver in the reported latency. The Java engine faced the
  same ceiling with a single command-issuing thread (see
  ``docs/ARCHITECTURE.md``) and solved it with multiple issuer threads; this
  engine has no equivalent yet, so it warns above the threshold. Do not compare
  results above it against other engines without accounting for this.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional

from ..client.benchmark_client import AsyncBenchmarkClient
from ..client.factory import BenchmarkClientFactory
from ..command.command import Command, CommandResult
from ..command.factory import CommandFactory
from ..config.driver_config import DriverConfig
from ..config.phase_config import PhaseConfig
from ..config.workload_config import WorkloadConfig
from ..metrics.collector import MetricsCollector
from ..metrics.interval_reporter import IntervalReporter, interval_sidecar_path
from ..metrics.ndjson_writer import NdjsonWriter
from .command_selector import CommandSelector
from .key_generator import Counter, KeyGenerator
from .rate_limiter import RateLimiter

logger = logging.getLogger("resp_bench")

# Above this many concurrent in-flight requests (connections x pipeline_depth)
# the single event loop, not the driver, tends to set the throughput ceiling and
# its queuing delay shows up as driver latency.
HIGH_CONNECTION_WARN_THRESHOLD = 128

# A worker yields at least this often even when every request completes without
# suspending (a cache hit, or an in-memory driver). Without it one connection can
# monopolise a whole duration-based phase while its peers record nothing.
YIELD_EVERY_N_REQUESTS = 64

# Backoff applied after repeated consecutive failures on one connection, so a
# permanently-failing connection cannot spin at full CPU inflating the error
# count. Capped so a transient blip costs almost nothing.
CONSECUTIVE_FAILURE_BACKOFF_AFTER = 8
MAX_FAILURE_BACKOFF_SECONDS = 0.05


async def _gather_all_or_cancel(coros) -> None:
    """Await all coroutines; on the first failure cancel and drain the rest.

    ``asyncio.gather`` deliberately does NOT cancel siblings when one task
    raises, which would leave workers running against clients the caller has
    already closed. This wrapper cancels them and waits for them to finish
    unwinding before re-raising, so no work escapes the phase that started it.
    """
    tasks = [asyncio.ensure_future(c) for c in coros]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


class BenchmarkEngine:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        driver_config: DriverConfig,
        workload_config: WorkloadConfig,
        metrics_path: str,
        commit_id: Optional[str] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._driver_config = driver_config
        self._workload_config = workload_config
        self._writer = NdjsonWriter(metrics_path)
        self._interval_path = interval_sidecar_path(metrics_path)
        self._commit_id = commit_id
        # True if any phase ended in ERROR. The CLI turns this into a non-zero
        # exit code so the matrix runner does not score the cell as a good run.
        self._had_error = False

    @property
    def had_error(self) -> bool:
        return self._had_error

    async def run(self) -> None:
        logger.info("Starting benchmark: %s", self._workload_config.name())
        logger.info(
            "Driver: %s, Server mode: %s",
            self._driver_config.driver_id,
            self._driver_config.mode,
        )
        logger.info(
            "Concurrency: asyncio, one client per connection, "
            "pipeline_depth workers per connection"
        )
        logger.info("Server: %s:%s", self._host, self._port)

        await self._setup_metadata()

        for phase in self._workload_config.phases:
            await self._execute_phase(phase)

        logger.info("Benchmark completed")

    async def _setup_metadata(self) -> None:
        try:
            sample = await BenchmarkClientFactory.create_and_connect(
                self._host, self._port, self._driver_config
            )
            self._writer.set_metadata(
                commit_id=self._commit_id,
                driver_id=self._driver_config.driver_id,
                primary_driver_version=sample.driver_version(),
                secondary_driver_id=self._driver_config.secondary_driver_id(),
                secondary_driver_version=sample.secondary_driver_version(),
                driver_details=sample.driver_details(),
            )
            logger.info(
                "Metadata: commit=%s, driver=%s, version=%s, details=%s",
                self._commit_id or "N/A",
                self._driver_config.driver_id,
                sample.driver_version(),
                sample.driver_details() or "{}",
            )
            await sample.close()
        except Exception as exc:  # noqa: BLE001 - metadata is best-effort
            logger.warning("Failed to get driver version for metadata: %s", exc)
            self._writer.set_metadata(
                commit_id=self._commit_id,
                driver_id=self._driver_config.driver_id,
                primary_driver_version="unknown",
                secondary_driver_id=self._driver_config.secondary_driver_id(),
                secondary_driver_version=None,
            )

    async def _execute_phase(self, phase: PhaseConfig) -> None:
        logger.info("=== Starting phase: %s (%s) ===", phase.id, phase.description)

        depth = phase.effective_pipeline_depth()
        in_flight = phase.connections * depth
        if depth > 1:
            logger.info(
                "pipeline_depth=%d: up to %d requests in flight per connection "
                "(%d total). What that costs physically is driver-specific and is "
                "recorded as 'pipelining' in the metrics metadata.",
                depth,
                depth,
                in_flight,
            )

        if in_flight > HIGH_CONNECTION_WARN_THRESHOLD:
            logger.warning(
                "connections=%d x pipeline_depth=%d = %d concurrent in-flight "
                "requests exceeds %d: the single event loop is likely the "
                "bottleneck rather than the driver, and loop queuing delay is "
                "reported as driver latency. Treat these results with care and do "
                "not compare them directly against other engines.",
                phase.connections,
                depth,
                in_flight,
                HIGH_CONNECTION_WARN_THRESHOLD,
            )

        collector = MetricsCollector()
        status = "ERROR"
        failure: Optional[BaseException] = None
        clients: List[AsyncBenchmarkClient] = []
        try:
            # Inside the guarded region: a failure part-way through opening
            # connections (server down, maxclients reached) or an unsupported
            # command name must still close what was opened and emit a row.
            clients = await self._create_clients(phase)
            commands = CommandFactory.create_all(phase.commands)

            if phase.warmup_requests > 0:
                await self._warmup(clients, phase.warmup_requests)

            # Started here so the measured window covers the workload only, not
            # connection setup or warmup.
            collector.start()
            status = await self._run_workload(phase, clients, commands, collector)
        except BaseException as exc:  # noqa: BLE001 - recorded below, then re-raised
            # BaseException, not Exception: under asyncio.run a Ctrl-C arrives as
            # CancelledError/KeyboardInterrupt, which would otherwise unwind past
            # write_phase_results and lose the row this block exists to guarantee.
            failure = exc
            interrupted = isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError))
            status = "INTERRUPTED" if interrupted else "ERROR"
            logger.error("Phase %s failed (%s): %s", phase.id, status, exc)
        finally:
            # A phase that failed before the workload started still needs real
            # timestamps: nulls would violate the documented schema and the graph
            # scripts would aggregate the row as a 0-RPS data point.
            if collector.start_time is None:
                collector.start()
            collector.stop()
            await self._close_clients(clients)

        # Anything short of COMPLETED must reach the CLI's exit code: an
        # interrupted phase is not a good run either, and the matrix runner would
        # otherwise score the cell as ok.
        if status != "COMPLETED":
            self._had_error = True

        self._writer.write_phase_results(
            phase_id=phase.id,
            status=status,
            connections=phase.connections,
            collector=collector,
            pipeline_depth=depth,
            sockets_per_client=clients[0].sockets_per_client() if clients else 1,
        )
        self._log_phase_summary(phase, collector, status)

        if failure is not None:
            raise failure

    async def _create_clients(self, phase: PhaseConfig) -> List[AsyncBenchmarkClient]:
        logger.info("Creating %d connections...", phase.connections)
        cps_limiter = RateLimiter.create(phase.cps_limit) if phase.has_cps_limit() else None
        depth = phase.effective_pipeline_depth()

        clients: List[AsyncBenchmarkClient] = []
        for _ in range(phase.connections):
            if cps_limiter is not None:
                await cps_limiter.acquire()
            client = await BenchmarkClientFactory.create_and_connect(
                self._host, self._port, self._driver_config, pipeline_depth=depth
            )
            clients.append(client)
        logger.info("All %d connections established", len(clients))
        return clients

    async def _warmup(self, clients: List[AsyncBenchmarkClient], warmup_requests: int) -> None:
        # Exactly `warmup_requests` PINGs per client, matching the shared schema's
        # "number of warmup PING requests per client" and what Java/Ruby/C# issue.
        # Deliberately NOT multiplied by pipeline_depth: opening a pooling driver's
        # extra sockets is the driver's job (see AsyncBenchmarkClient.prime), and
        # scaling warmup with depth would make a depth sweep also sweep warmup work.
        logger.info("Warmup: %d PINGs per client...", warmup_requests)

        async def warm(client: AsyncBenchmarkClient) -> None:
            for _ in range(warmup_requests):
                result = await client.ping()
                # Fail fast on an unreachable/misconfigured server rather than
                # running a whole phase that records only errors.
                if not result.success:
                    raise RuntimeError(f"Warmup PING failed: {result.error}")

        # Cancel-and-drain on first failure so no warmup task keeps pinging a
        # client that _execute_phase has already closed.
        await _gather_all_or_cancel(warm(c) for c in clients)
        logger.info("Warmup completed")

    async def _run_workload(
        self,
        phase: PhaseConfig,
        clients: List[AsyncBenchmarkClient],
        commands: List[Command],
        collector: MetricsCollector,
    ) -> str:
        completion = phase.completion
        depth = phase.effective_pipeline_depth()
        seed_base = phase.keyspace.seed_value()
        shared_counter = Counter()  # shared across workers for sequential_int

        # Constructed here, at the start of the measured window, rather than
        # before warmup: the limiter's schedule starts from its construction
        # time, so building it earlier would let the phase open with a burst of
        # accumulated slots proportional to the warmup duration.
        rate_limiter = RateLimiter.create(phase.rps_limit) if phase.has_rps_limit() else None

        # A request-based target is a single budget SHARED across workers, which
        # each worker claims from one request at a time (matching the Java
        # reference's shared AtomicLong). A slow connection therefore cannot cap
        # the phase -- faster workers absorb the slack and the phase ends when
        # the total budget is exhausted. Duration-based runs use a wall-clock
        # deadline instead.
        target_requests = None if completion.is_duration_based() else completion.total_requests()
        end_time = (
            time.monotonic() + completion.duration_seconds()
            if completion.is_duration_based()
            else None
        )
        request_budget = Counter()

        # Interval reporting records every result into a per-window collector as
        # well. Chosen once here so a phase without it pays nothing per request.
        reporter: Optional[IntervalReporter] = None
        if phase.has_interval_reporting():
            reporter = IntervalReporter(
                path=self._interval_path,
                phase_id=phase.id,
                connections=phase.connections,
                interval_seconds=phase.reporting_interval_seconds,
            )
            phase_record = collector.record
            window_record = reporter.record

            def record(result: CommandResult) -> None:
                phase_record(result)
                window_record(result)

        else:
            record = collector.record

        async def worker(
            client: AsyncBenchmarkClient,
            key_gen: KeyGenerator,
            selector: CommandSelector,
        ) -> None:
            since_yield = 0
            consecutive_failures = 0
            while True:
                if target_requests is not None:
                    # Claim a slot; claims are atomic on the single event loop
                    # (no await between read and increment), so unlike Java no
                    # decrement-on-overshoot is needed.
                    if request_budget.next_value() >= target_requests:
                        break
                elif time.monotonic() >= end_time:
                    break

                if rate_limiter is not None:
                    await rate_limiter.acquire()

                command = None
                succeeded = False
                try:
                    command = selector.select()
                    key = key_gen.next_key()
                    result = await command.execute(client, key)
                    record(result)
                    succeeded = result.success
                except Exception:  # noqa: BLE001 - record failures, keep going
                    record(
                        CommandResult(
                            command_name=command.name if command else "UNKNOWN",
                            latency_micros=0,
                            success=False,
                        )
                    )

                # A request can complete without ever suspending -- a driver that
                # raises before its first await (caught by _measure), or any
                # in-memory/cache-hit response. Such a worker would monopolise the
                # event loop, so guarantee a suspension point.
                since_yield += 1
                if succeeded:
                    consecutive_failures = 0
                    if since_yield >= YIELD_EVERY_N_REQUESTS:
                        since_yield = 0
                        await asyncio.sleep(0)
                else:
                    consecutive_failures += 1
                    since_yield = 0
                    if consecutive_failures >= CONSECUTIVE_FAILURE_BACKOFF_AFTER:
                        # Back off so a permanently-failing connection cannot spin
                        # at full CPU inflating the error count.
                        await asyncio.sleep(
                            min(
                                0.001 * (consecutive_failures - CONSECUTIVE_FAILURE_BACKOFF_AFTER + 1),
                                MAX_FAILURE_BACKOFF_SECONDS,
                            )
                        )
                    else:
                        await asyncio.sleep(0)

        # One key generator and one command selector per CONNECTION, shared by
        # that connection's pipeline slots. Sharing is what keeps the key stream a
        # property of the connection: at any depth, connection i draws from the
        # same RNG stream seeded seed_base + i, so raising pipeline_depth changes
        # concurrency without changing which keys are touched.
        workers = []
        for idx, client in enumerate(clients):
            key_gen = KeyGenerator.create_with_seed(
                phase.keyspace, seed_base + idx, sequential_counter=shared_counter
            )
            selector = CommandSelector(commands)
            for _ in range(depth):
                workers.append(worker(client, key_gen, selector))

        logger.info(
            "Starting %d worker coroutines (%d connections x pipeline_depth %d)...",
            len(workers),
            len(clients),
            depth,
        )
        if reporter is not None:
            reporter.start()
        try:
            # Cancel-and-drain on first failure: a worker that dies must not
            # leave its peers running against clients we are about to close.
            await _gather_all_or_cancel(workers)
        except KeyboardInterrupt:  # pragma: no cover
            logger.warning("Workload interrupted")
            return "INTERRUPTED"
        except Exception as exc:  # noqa: BLE001
            logger.error("Error during workload execution: %s", exc)
            return "ERROR"
        finally:
            # Stops the clock with the workload loop and writes the final partial
            # window, so interval totals sum to the phase totals.
            if reporter is not None:
                await reporter.stop()

        # A phase in which nothing succeeded produced no usable latency data;
        # reporting COMPLETED would let the orchestrator record it as a good run.
        successes = collector.total_requests - collector.total_errors
        if collector.total_requests > 0 and successes == 0:
            logger.error(
                "All %d requests failed; reporting phase as ERROR",
                collector.total_requests,
            )
            return "ERROR"

        logger.info("All operations completed (%d total requests)", collector.total_requests)
        return "COMPLETED"

    async def _close_clients(self, clients: List[AsyncBenchmarkClient]) -> None:
        logger.info("Closing %d connections...", len(clients))
        for client in clients:
            try:
                await client.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error closing client: %s", exc)

    def _log_phase_summary(self, phase, collector, status) -> None:
        duration_s = collector.duration_millis() / 1000.0
        total = collector.total_requests
        errors = collector.total_errors
        rps = round(total / duration_s) if duration_s > 0 else 0
        logger.info("=== Phase %s completed: %s ===", phase.id, status)
        logger.info(
            "  Duration: %.1fs | Requests: %d | Errors: %d | RPS: %d | "
            "connections=%d x depth=%d",
            duration_s,
            total,
            errors,
            rps,
            phase.connections,
            phase.effective_pipeline_depth(),
        )
        for cmd_name, cmd_metrics in collector.command_metrics.items():
            if cmd_metrics.count() == 0:
                continue
            logger.info(
                "  %s: %d req (%d err) | p50=%dus p95=%dus p99=%dus p99.9=%dus | min=%dus max=%dus",
                cmd_name,
                cmd_metrics.requests,
                cmd_metrics.errors,
                cmd_metrics.percentile(50),
                cmd_metrics.percentile(95),
                cmd_metrics.percentile(99),
                cmd_metrics.percentile(99.9),
                cmd_metrics.min(),
                cmd_metrics.max(),
            )
