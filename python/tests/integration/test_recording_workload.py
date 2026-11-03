"""End-to-end engine tests using the recording driver (no server needed)."""

import json

import pytest

from resp_bench.config.command_config import CommandConfig
from resp_bench.config.completion_config import CompletionConfig
from resp_bench.config.driver_config import DriverConfig
from resp_bench.config.keyspace_config import KeyspaceConfig
from resp_bench.config.phase_config import PhaseConfig
from resp_bench.config.workload_config import WorkloadConfig
from resp_bench.engine.benchmark import BenchmarkEngine


def _driver(**specific):
    return DriverConfig(driver_id="recording", specific_driver_config=specific)


def _workload(phases):
    return WorkloadConfig(schema_version="1.0", benchmark_profile={"name": "t"}, phases=phases)


def _phase(**kw):
    defaults = dict(
        id="P",
        connections=4,
        completion=CompletionConfig(type="requests", requests=400),
        keyspace=KeyspaceConfig(keys_count=100, key_prefix="k:", generation_alg="sequential_int"),
        commands=[CommandConfig(command="set", weight=1.0, data_size_bytes=64)],
        warmup_requests=0,
    )
    defaults.update(kw)
    return PhaseConfig(**defaults)


async def _run(tmp_path, driver, workload):
    out = tmp_path / "metrics.ndjson"
    engine = BenchmarkEngine(
        host="localhost",
        port=6379,
        driver_config=driver,
        workload_config=workload,
        metrics_path=str(out),
    )
    await engine.run()
    return [json.loads(line) for line in out.read_text().splitlines()]


async def test_request_based_completion_totals(tmp_path):
    rows = await _run(tmp_path, _driver(), _workload([_phase()]))
    assert len(rows) == 1
    row = rows[0]
    assert row["phase"]["status"] == "COMPLETED"
    assert row["phase"]["connections"] == 4
    assert row["totals"]["requests"] == 400
    assert row["totals"]["errors"] == 0
    assert row["metrics"]["SET"]["requests"] == 400
    assert row["metrics"]["SET"]["errors"] == 0


async def test_shared_budget_hits_exact_total_when_uneven(tmp_path):
    # The request target is a single shared budget (matching Java), not split
    # per worker, so a target that does not divide evenly across connections
    # must still produce exactly that many requests.
    rows = await _run(
        tmp_path,
        _driver(),
        _workload(
            [_phase(connections=4, completion=CompletionConfig(type="requests", requests=401))]
        ),
    )
    assert rows[0]["totals"]["requests"] == 401
    assert rows[0]["metrics"]["SET"]["requests"] == 401


async def test_error_injection_counts_errors(tmp_path):
    rows = await _run(
        tmp_path,
        _driver(error_rate=1.0),
        _workload([_phase(completion=CompletionConfig(type="requests", requests=100))]),
    )
    row = rows[0]
    assert row["totals"]["requests"] == 100
    assert row["totals"]["errors"] == 100
    # All failed -> empty (but present) histogram: count 0, zero summary, and
    # the hdr block is still emitted (matching the Java reference schema).
    set_metrics = row["metrics"]["SET"]
    assert set_metrics["errors"] == 100
    assert set_metrics["latency"]["count"] == 0
    assert set_metrics["latency"]["summary"]["max"] == 0
    assert set_metrics["latency"]["hdr"]["format"] == "hdr"


async def test_warmup_fails_fast_on_all_errors(tmp_path):
    # A server that fails every request should abort at warmup rather than
    # running a whole phase of error metrics.
    out = tmp_path / "metrics.ndjson"
    engine = BenchmarkEngine(
        host="localhost",
        port=6379,
        driver_config=_driver(error_rate=1.0),
        workload_config=_workload([_phase(warmup_requests=2)]),
        metrics_path=str(out),
    )
    with pytest.raises(RuntimeError, match="Warmup"):
        await engine.run()
    # The failure still produces a row, marked ERROR, so the phase is visible in
    # the metrics output instead of silently vanishing.
    row = json.loads(out.read_text().splitlines()[0])
    assert row["phase"]["status"] == "ERROR"
    assert row["totals"]["requests"] == 0


async def test_two_phases_written_as_two_lines(tmp_path):
    phases = [
        _phase(id="WARMUP", completion=CompletionConfig(type="requests", requests=100)),
        _phase(
            id="STEADY",
            completion=CompletionConfig(type="requests", requests=200),
            commands=[
                CommandConfig(command="get", weight=0.8),
                CommandConfig(command="set", weight=0.2, data_size_bytes=64),
            ],
            warmup_requests=1,
        ),
    ]
    rows = await _run(tmp_path, _driver(), _workload(phases))
    assert [r["phase"]["id"] for r in rows] == ["WARMUP", "STEADY"]
    assert rows[0]["totals"]["requests"] == 100
    assert rows[1]["totals"]["requests"] == 200


async def test_interval_reporting_sidecar(tmp_path):
    phase = _phase(
        connections=2,
        completion=CompletionConfig(type="duration", seconds=3),
        reporting_interval_seconds=1,
    )
    rows = await _run(tmp_path, _driver(), _workload([phase]))
    assert len(rows) == 1  # metrics file holds only the phase record
    sidecar = tmp_path / "metrics.interval.ndjson"
    intervals = [json.loads(line) for line in sidecar.read_text().splitlines()]

    assert len(intervals) >= 3
    assert [r["interval"]["index"] for r in intervals] == list(range(len(intervals)))
    for rec in intervals:
        assert rec["type"] == "interval"
        assert rec["phase_id"] == "P"
        assert rec["connections"] == 2
        assert rec["interval"]["duration_s"] <= 1.5
        set_metrics = rec["metrics"]["SET"]
        assert set_metrics["requests"] == rec["totals"]["requests"]
        assert set_metrics["latency"]["unit"] == "us"
        assert set_metrics["latency"]["hdr"]["format"] == "hdr"
        assert isinstance(rec["memory"]["gc_gen_counts_sum"], int)
        assert rec["memory"]["asyncio_tasks"] >= 1
    elapsed = [r["interval"]["elapsed_s"] for r in intervals]
    assert elapsed == sorted(elapsed)
    assert sum(r["totals"]["requests"] for r in intervals) == rows[0]["totals"]["requests"]
    assert sum(r["totals"]["errors"] for r in intervals) == rows[0]["totals"]["errors"]


async def test_interval_reporting_disabled_writes_no_sidecar(tmp_path):
    await _run(tmp_path, _driver(), _workload([_phase()]))
    assert not (tmp_path / "metrics.interval.ndjson").exists()
