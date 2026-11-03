"""Interval (soak-test time-series) reporting.

See docs/INTERVAL_REPORTING.md. While a phase with ``reporting_interval_seconds
> 0`` runs its workload, every result is recorded into a per-window
``MetricsCollector`` as well as the cumulative phase collector. A reporter task
on the same event loop swaps the window collector for a fresh one every N
seconds and appends one ``"type": "interval"`` record to a sidecar NDJSON file,
flushed per record so a crash keeps everything written so far.

Single-event-loop design: ``record`` and ``_snapshot`` never await, so they
cannot interleave and no locking is needed. The reporter task is only ever
cancelled while sleeping, never part-way through a snapshot.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import resource
import time
from datetime import datetime, timezone
from typing import IO, Optional

from ..command.command import CommandResult
from .collector import MetricsCollector
from .ndjson_writer import build_command_metrics

logger = logging.getLogger("resp_bench")

_NDJSON_SUFFIX = ".ndjson"
_INTERVAL_SUFFIX = ".interval.ndjson"


def interval_sidecar_path(metrics_path: str) -> str:
    """``x.ndjson`` -> ``x.interval.ndjson``; any other path gets the suffix appended."""
    if metrics_path.endswith(_NDJSON_SUFFIX):
        return metrics_path[: -len(_NDJSON_SUFFIX)] + _INTERVAL_SUFFIX
    return metrics_path + _INTERVAL_SUFFIX


def _iso8601_ms(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _rss_bytes() -> Optional[int]:
    # Current resident set from /proc (Linux). ru_maxrss is deliberately not
    # used: it is the peak, not the current value, so it can never show a leak
    # being reclaimed. Where /proc is unavailable (macOS) the field is omitted.
    try:
        with open("/proc/self/statm", "r", encoding="ascii") as f:
            fields = f.read().split()
        return int(fields[1]) * resource.getpagesize()
    except (OSError, ValueError, IndexError):
        return None


def memory_snapshot() -> dict:
    memory: dict = {}
    rss = _rss_bytes()
    if rss is not None:
        memory["rss_bytes"] = rss
    memory["gc_gen_counts_sum"] = int(sum(gc.get_count()))
    try:
        memory["asyncio_tasks"] = len(asyncio.all_tasks())
    except RuntimeError:  # no running loop
        pass
    return memory


class IntervalReporter:
    def __init__(
        self,
        *,
        path: str,
        phase_id: str,
        connections: int,
        interval_seconds: int,
    ) -> None:
        self._path = path
        self._phase_id = phase_id
        self._connections = connections
        self._interval_seconds = interval_seconds
        self._window = MetricsCollector()
        self._index = 0
        self._file: Optional[IO[str]] = None
        self._task: Optional[asyncio.Task] = None
        self._phase_start_mono = 0.0
        self._window_start_mono = 0.0
        self._window_start_wall: Optional[datetime] = None

    def record(self, result: CommandResult) -> None:
        self._window.record(result)

    def start(self) -> None:
        """Open the sidecar and start the reporter task on the running loop."""
        parent = os.path.dirname(self._path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._file = open(self._path, "a", encoding="utf-8")
        self._phase_start_mono = time.monotonic()
        self._window_start_mono = self._phase_start_mono
        self._window_start_wall = datetime.now(timezone.utc)
        self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self) -> None:
        """Cancel the reporter task, emit the final partial window, close the file."""
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        try:
            if self._window.total_requests > 0:
                self._snapshot()
        finally:
            if self._file is not None:
                self._file.close()
                self._file = None

    async def _run(self) -> None:
        # Deadlines are absolute multiples of the interval from the phase start
        # so windows do not drift by the cost of each snapshot.
        while True:
            deadline = self._phase_start_mono + self._interval_seconds * (self._index + 1)
            await asyncio.sleep(max(0.0, deadline - time.monotonic()))
            try:
                self._snapshot()
            except Exception as exc:  # noqa: BLE001 - reporting must not kill the phase
                logger.warning("Interval report failed: %s", exc)

    def _snapshot(self) -> None:
        window, self._window = self._window, MetricsCollector()
        now_mono = time.monotonic()
        now_wall = datetime.now(timezone.utc)
        duration = now_mono - self._window_start_mono
        requests = window.total_requests

        record = {
            "type": "interval",
            "phase_id": self._phase_id,
            "interval": {
                "index": self._index,
                "start_timestamp": _iso8601_ms(self._window_start_wall),
                "end_timestamp": _iso8601_ms(now_wall),
                "elapsed_s": round(now_mono - self._phase_start_mono, 1),
                "duration_s": round(duration, 1),
            },
            "connections": self._connections,
            "totals": {"requests": requests, "errors": window.total_errors},
            "rps": round(requests / duration) if duration > 0 else 0,
            "metrics": build_command_metrics(window, skip_empty=True),
            "memory": memory_snapshot(),
        }

        self._index += 1
        self._window_start_mono = now_mono
        self._window_start_wall = now_wall
        if self._file is not None:
            self._file.write(json.dumps(record) + "\n")
            self._file.flush()
