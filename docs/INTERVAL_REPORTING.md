# Interval Reporting (soak tests)

Implements [#16](https://github.com/Bit-Quill/resp-bench/issues/16) and the runtime-memory part of
[#17](https://github.com/Bit-Quill/resp-bench/issues/17) for the Java, Node.js, Go and Python engines.
The record shape follows the C# prototype on `Aryex/resp-bench@alexl/overtime-reporting`
(sidecar file, per-window HdrHistogram, `memory` block).

## Configuration

A phase may set `reporting_interval_seconds`:

```json
{ "id": "STEADY", "reporting_interval_seconds": 30, "completion": {"type": "duration", "seconds": 43200}, ... }
```

| Value | Meaning |
|---|---|
| absent / `0` | Disabled (default). Engine behaves exactly as before. |
| integer `> 0` | Emit one interval record every N seconds while the phase's workload runs. |
| negative / non-integer | Config validation error. |

## Output file

Interval records go to a **sidecar** next to the metrics file, never into the metrics file itself, so
every existing consumer of `<label>.ndjson` (matrix runner record check, graph generators) is unaffected.

- Path: the `--metrics` path with a trailing `.ndjson` replaced by `.interval.ndjson`
  (`results/x/valkey-glide@connections=64.ndjson` → `results/x/valkey-glide@connections=64.interval.ndjson`).
  If the metrics path does not end in `.ndjson`, append `.interval.ndjson`.
- Opened in **append** mode; created lazily, only when a phase with interval reporting enabled starts.
- One JSON object per line, **flushed after every record**, so a crash at hour 10 keeps hours 0–10.

## Timing

- The interval clock starts when the phase's workload loop starts (after client creation and the
  per-client warmup requests) and stops when the loop ends.
- On phase end, a final **partial** interval is emitted if it recorded any request, so the sum of
  `totals.requests` across a phase's interval records equals that phase's final `totals.requests`.
- `index` starts at 0 per phase.

## Record format

```json
{
  "type": "interval",
  "phase_id": "STEADY",
  "interval": {
    "index": 12,
    "start_timestamp": "2026-11-03T10:00:00.000Z",
    "end_timestamp": "2026-11-03T10:00:30.000Z",
    "elapsed_s": 390.0,
    "duration_s": 30.0
  },
  "connections": 64,
  "totals": { "requests": 1500000, "errors": 0 },
  "rps": 50000,
  "metrics": {
    "GET": {
      "requests": 1200000,
      "errors": 0,
      "latency": {
        "unit": "us",
        "count": 1200000,
        "summary": { "min": 40, "p50": 110, "p95": 230, "p99": 380, "p999": 900, "max": 4100 },
        "hdr": { "format": "hdr", "sigfig": 3, "payload_b64": "HISTF..." }
      }
    }
  },
  "memory": { "rss_bytes": 123456789, "...": "runtime-specific, see below" }
}
```

- `metrics.<CMD>` uses exactly the same keys, command naming and latency units/encoding as the engine's
  final phase record — only scoped to this window (per-window histogram, reset after each snapshot).
  The cumulative phase histogram is unchanged.
- `elapsed_s` = seconds from the phase's workload start to `end_timestamp`; `duration_s` = window length.
  Both rounded to 0.1.
- `rps` = `round(totals.requests / duration_s)`.
- A command with no samples in the window may be omitted.

### `memory` block

All values are integers; a field that cannot be read on the current platform is omitted (never fake 0).
`rss_bytes` (process resident set, from `/proc/self/statm` on Linux) is present in every engine where
readable.

| Engine | Fields |
|---|---|
| Java | `rss_bytes`, `heap_used_bytes`, `heap_committed_bytes`, `heap_max_bytes`, `nonheap_used_bytes`, `direct_buffer_bytes`, `gc_count`, `gc_time_ms`, `thread_count` |
| Node.js | `rss_bytes`, `heap_used_bytes`, `heap_total_bytes`, `external_bytes`, `array_buffers_bytes` |
| Go | `rss_bytes`, `heap_alloc_bytes`, `heap_inuse_bytes`, `heap_sys_bytes`, `sys_bytes`, `num_gc`, `gc_pause_total_ns`, `goroutines` |
| Python | `rss_bytes`, `gc_gen_counts_sum` (`sum(gc.get_count())`; not `len(gc.get_objects())`, which is too slow), `asyncio_tasks` (`len(asyncio.all_tasks())`) |

For GLIDE, native (Rust core) memory lives outside every managed heap: it shows up as
`rss_bytes` minus the managed-heap figures. Growth in `rss_bytes` with flat managed-heap numbers points
at native memory.
