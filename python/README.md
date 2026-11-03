# resp-bench Python Engine

Python implementation of the resp-bench benchmark suite, at parity with the
Java (reference), Ruby, and C# engines.

## Supported Drivers

| Driver | `driver_id` | Package | Notes |
|--------|-------------|---------|-------|
| Valkey GLIDE | `valkey-glide-python` | `valkey-glide` (`import glide`) | Async client |
| redis-py | `redis-py` | `redis` (`redis.asyncio`) | Async client, RESP3, retries disabled |
| Recording | `recording` | — | In-memory; for server-free tests |

Peer drivers are kept deliberately few so each one's configuration can be held
equivalent (same RESP version, same retry policy, same command timeout). A
`valkey-py` driver is a planned follow-up.

> The GLIDE `driver_id` is `valkey-glide-python` (not the bare `valkey-glide`,
> which is the Java driver) — matching the `valkey-glide-ruby` /
> `valkey-glide-csharp` convention.

## Execution model

The engine is asyncio-based. For a phase with `connections = N` and
`pipeline_depth = D`, it creates **N client instances** (one client per
connection — the `client == connection` invariant shared by every engine) and
runs **N × D worker coroutines** concurrently on a single event loop. Each worker
awaits one command at a time, so a connection has up to `D` requests in flight.
At the default `D = 1` this is the async analogue of the Java/Ruby "one
in-flight request per connection" model.

Each connection owns one key generator and one command selector, shared by its
`D` slots, so raising `pipeline_depth` changes how many requests are outstanding
but not which keys a connection touches. That mirrors the C# engine, where one
worker owns the generator for all of its pipeline slots.

### What `pipeline_depth > 1` costs, per driver

The concurrency is the same; the physical cost is not, so each driver records its
mechanism as `pipelining` in the metrics metadata:

| Driver | Mechanism | Sockets per client |
|---|---|---|
| `valkey-glide-python` | multiplexed over one socket | 1 |
| `redis-py` | one pooled connection per in-flight command | up to `pipeline_depth` |

redis-py cannot give real depth on a single socket: `single_connection_client`
serialises behind a lock, and `pipeline()` batches N commands into one round trip
with one shared latency — a different measurement. Its pool is therefore capped
at `pipeline_depth`, so a connection can never quietly become more sockets than
the phase asked for. **Compare `redis-py` depth>1 numbers against GLIDE depth>1
with that difference in mind; they are not the same experiment.**

This one-client-per-connection mapping is this engine's baseline; it is not a
property of the whole suite (among other engines' drivers, `lettuce` and
`redis-rb` are 1:1, but `jedis`/`redisson` pool, `spring-data-*` share a
template, and `stackexchange-redis` multiplexes). Sharing a single multiplexing client across
workers was proposed and declined upstream
([ikolomi/resp-bench#11](https://github.com/ikolomi/resp-bench/issues/11)) in
favour of keeping this baseline.

### Known limits

- **Single event loop.** Above ~128 concurrent in-flight requests
  (`connections × pipeline_depth`) the event loop, not the driver, becomes the
  bottleneck, and loop queuing delay is attributed to the driver in the reported
  latency. The engine warns past that threshold. The Java engine hit the same
  ceiling with one command-issuing thread and added multiple issuer threads; this
  engine has no equivalent yet, so results above the threshold are not directly
  comparable to other engines.

## Interval reporting (soak tests)

Set `reporting_interval_seconds` (integer, `0`/absent = off) on a phase to get a
time series alongside the normal phase record. Every N seconds of the workload
loop, one `"type": "interval"` record is appended (and flushed) to a sidecar next
to the metrics file: `--metrics out/x.ndjson` writes `out/x.interval.ndjson`. Each
record carries a per-window histogram in the same `metrics.<CMD>` shape as the
phase record; a final partial window is written at phase end so interval request
totals sum to the phase totals. The reporter is an asyncio task on the same loop,
cancelled when the workload ends.

The Python `memory` block has `rss_bytes` (current RSS from `/proc/self/statm`;
omitted where unavailable, e.g. macOS), `gc_gen_counts_sum` (`sum(gc.get_count())`)
and `asyncio_tasks` (`len(asyncio.all_tasks())`). Full spec:
[`docs/INTERVAL_REPORTING.md`](../docs/INTERVAL_REPORTING.md).

## Installation

Requires Python 3.10+ (tested on 3.10, 3.11 and 3.12) and a **C compiler**:
`hdrhistogram` publishes no wheels, so pip builds it from source. GitHub's
`ubuntu-latest` and a normal macOS dev box already have one; a slim container
does not (`pip install` fails with `command 'gcc' failed: No such file or
directory` — `apt-get install gcc` fixes it).

```bash
pip install -e .
# with test tooling:
pip install -e ".[dev]"
```

## Usage

```bash
python -m resp_bench \
  --server localhost:6379 \
  --driver ../configs/drivers/default/redis-py.json \
  --workload ../configs/workloads/example-workload.json \
  --metrics output.ndjson

# Show supported drivers and commands
python -m resp_bench --info
```

## Testing

```bash
pytest                      # unit + recording-driver integration (no server needed)
```

See [../docs/ADDING_LANGUAGE.md](../docs/ADDING_LANGUAGE.md) and
[../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) for the shared contracts.
