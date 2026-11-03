"""Interval reporting: config parsing/validation and sidecar path derivation."""

import pytest

from resp_bench.config.loader import ConfigLoader
from resp_bench.metrics.interval_reporter import interval_sidecar_path, memory_snapshot


def _workload(**phase_extra):
    phase = {
        "id": "P",
        "connections": 1,
        "completion": {"type": "duration", "seconds": 2},
        "keyspace": {"keys_count": 10, "key_prefix": "k:", "generation_alg": "sequential_int"},
        "commands": [{"command": "get", "weight": 1.0}],
    }
    phase.update(phase_extra)
    return {"schema_version": "1.0", "benchmark_profile": {"name": "t"}, "phases": [phase]}


def test_interval_defaults_to_disabled():
    phase = ConfigLoader.parse_workload_config(_workload()).phases[0]
    assert phase.reporting_interval_seconds == 0
    assert not phase.has_interval_reporting()


@pytest.mark.parametrize("value", [0, 1, 30])
def test_interval_parsed(value):
    phase = ConfigLoader.parse_workload_config(
        _workload(reporting_interval_seconds=value)
    ).phases[0]
    assert phase.reporting_interval_seconds == value
    assert phase.has_interval_reporting() == (value > 0)


@pytest.mark.parametrize("value", [-1, 1.5, "30", True])
def test_invalid_interval_rejected(value):
    with pytest.raises(
        ValueError,
        match=r"invalid phase 'P': reporting_interval_seconds must be a non-negative integer",
    ):
        ConfigLoader.parse_workload_config(_workload(reporting_interval_seconds=value))


@pytest.mark.parametrize(
    "metrics, expected",
    [
        ("results/x/valkey-glide@connections=64.ndjson",
         "results/x/valkey-glide@connections=64.interval.ndjson"),
        ("out.ndjson", "out.interval.ndjson"),
        ("out.json", "out.json.interval.ndjson"),
        ("out", "out.interval.ndjson"),
    ],
)
def test_sidecar_path(metrics, expected):
    assert interval_sidecar_path(metrics) == expected


def test_memory_snapshot_fields_are_ints():
    mem = memory_snapshot()
    assert isinstance(mem["gc_gen_counts_sum"], int)
    for value in mem.values():
        assert isinstance(value, int)
