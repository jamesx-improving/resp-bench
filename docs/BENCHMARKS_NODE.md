# Node.js Benchmarks

Details specific to the Node.js engine (`node/`). See [ARCHITECTURE.md](ARCHITECTURE.md)
for the cross-engine design and [../node/README.md](../node/README.md) for build and
usage.

## Drivers

| `driver_id` | Package | Notes |
|---|---|---|
| `valkey-glide-node` | `@valkey/valkey-glide` | Valkey GLIDE for Node.js. Prebuilt native binaries per platform. |
| `ioredis` | `ioredis` | The most widely used Node.js Redis client. |
| `iovalkey` | `iovalkey` | The Valkey-maintained fork of ioredis; API-identical. |
| `recording` | — | Synthetic-latency client for server-free testing. |

The GLIDE id is `valkey-glide-node`, **not** `valkey-glide` — the latter belongs to
the Java engine in the global `DRIVER_ENGINE_MAP`.

## Concurrency Model

One event loop, one client per connection, one worker per connection. An awaited
command parks its worker, not the loop, so other connections keep progressing —
the analogue of Java's virtual-thread-per-client design.

`pipeline_depth > 1` gives each connection that many independent
issue/await/record slots, so a settled request is replaced immediately rather than
waiting for a batch.

The per-phase request budget is shared across workers and claimed one request at a
time (matching Java's `AtomicLong`), so a slow connection cannot cap the run.

## The Single-Core Ceiling

**Node runs the whole engine on one thread, so one CPU core is the hard ceiling.**
This is the single most important caveat when comparing Node numbers to Java or C#,
which spread issuing across threads.

Measured locally (Apple M-series laptop, Valkey 8 co-located on the same host,
100% GET, 512B values, 10k keys, 5s phases). **Absolute numbers here are not
publishable results** — client and server contend for the same cores. The *shape*
is the point:

Throughput vs connections (`pipeline_depth=1`):

| Connections | glide RPS | ioredis RPS | iovalkey RPS |
|---|---|---|---|
| 1 | 1,139 | 1,584 | 1,646 |
| 10 | 8,424 | 8,467 | 10,360 |
| 50 | 25,065 | 23,071 | 26,268 |
| 100 | 33,639 | 34,898 | 35,087 |
| 200 | 41,812 | 42,748 | 41,144 |

Doubling 100 → 200 connections buys only ~20% more throughput while p50 latency
roughly doubles (2.6ms → 4.5ms). That plateau is the event loop saturating, not the
clients.

Throughput vs `pipeline_depth` at 10 connections (ioredis), with process CPU:

| `pipeline_depth` | RPS | p50 | p99 | CPU (of one core) |
|---|---|---|---|---|
| 1 | 10,124 | 929µs | 2,055µs | 29% |
| 4 | 32,194 | 1,157µs | 2,731µs | 52% |
| 16 | 72,631 | 1,955µs | 4,089µs | 99% |

CPU rises in lockstep with throughput and pins at 99% of a single core, where
throughput stops scaling. **The engine, not the client, is the limit past that
point.**

Practical guidance:

- Prefer raising `pipeline_depth` over raising `connections` to reach high
  throughput on Node — it is far cheaper per unit of RPS.
- When comparing Node against Java/C#, check whether the Node process is CPU-bound.
  If it is at ~100% of a core, you are measuring the engine, not the driver.
- Java addresses the same ceiling with parallel command-issuer threads (see
  [ARCHITECTURE.md](ARCHITECTURE.md) § "Parallel Command Issuers"). The Node
  equivalent would be `worker_threads` with a client partition per worker. That is
  deliberately **not** implemented — it is a follow-up, to be justified by
  measurements rather than assumed.

## Fairness Controls

Node-specific hazards with no analogue in the other engines, each handled
explicitly:

- **Auto-pipelining is forced off** (`enableAutoPipelining: false`). ioredis and
  iovalkey can transparently batch commands issued in the same event-loop tick,
  which would inflate throughput against every other engine while looking like a
  driver win.
- **Reconnects are disabled** (`retryStrategy: () => null`). ioredis' default
  retries forever, so a wrong host would hang a run rather than fail it, and a
  mid-phase reconnect would fold connection setup into request latency.
- **Response decoding is uniform.** `get` returns a `string` in all three drivers,
  so none is charged for a different amount of decoding. GLIDE returns strings by
  default; we do not opt one driver into bytes.
- **SET payloads are allocated once** per command object, so GC churn from payload
  construction is not attributed to the driver.
- **Sub-millisecond rate limits work.** `setTimeout` clamps to ~1ms, so the limiter
  yields via `setImmediate` below that. A 100k rps limit is a 10µs interval; a
  timer-based wait would undershoot by ~100×.
- **Memory is not comparable to the JVM.** The system monitor's RSS samples include
  V8 heap growth, which grows and collects on a different schedule from the JVM's.

## Interval Reporting

Setting `reporting_interval_seconds` on a phase writes per-window records to
`<metrics minus .ndjson>.interval.ndjson`, as specified in
[INTERVAL_REPORTING.md](INTERVAL_REPORTING.md). Node specifics:

- The window is a second, per-command `CommandMetrics` map in `MetricsCollector`,
  swapped out at each tick (`takeWindow`). `record` never awaits, so the swap needs
  no lock. With reporting disabled the hot path only adds a null check.
- The timer is a `setInterval` that is `unref()`'d and cleared at phase end, where
  the final partial window is written. Records are written with `appendFileSync`,
  so each one is on disk before the next is built.
- `memory` comes from `process.memoryUsage()`: `rss_bytes`, `heap_used_bytes`,
  `heap_total_bytes`, `external_bytes`, `array_buffers_bytes`. GLIDE's Rust core
  memory is outside the V8 heap and shows up only in `rss_bytes`.
- Timers run on the same event loop as the workers. A driver whose calls settle
  purely through microtasks (the `recording` driver with no delay) starves the timer
  until the phase ends. Real drivers do socket I/O, so their ticks fire on time.

## Cross-Engine Parity

Verified against the Java reference rather than assumed:

- **Key sequences are byte-identical to Java.** 79,000 keys diffed against Java's
  real `KeyGenerator` across `sequential_int` and `uniform_rand`, 1–16 workers,
  prime `keys_count`, and prefix-width edge cases. `javaRandom.ts` ports
  `java.util.Random` with `BigInt` (the 48-bit LCG multiply reaches ~2^83, past
  what a JS `number` holds exactly) and is anchored to
  `new Random(0).nextInt() == -1155484576`.
- **HDR payloads decode in Java.** `payload_b64` is
  `encodeIntoCompressedBase64()` used directly — it is already base64, so encoding
  it again would produce something Java cannot read. Verified by decoding a
  Node-produced payload with `org.HdrHistogram.Histogram`: identical count and
  percentiles. Range `(1, 600_000_000, 3)`, as in every engine.
- **`summary.min`/`max` match Java's quantization.** Java's `getMinValue()`/
  `getMaxValue()` return the bucket's equivalent bounds, while hdr-histogram-js'
  `minNonZeroValue`/`maxValue` return the raw sample — they diverge above ~1000µs
  (50000µs recorded reads back as 50015 in Java). The engine uses
  `getValueAtPercentile(0)` and `getValueAtPercentile(100)`, which match Java exactly.
- **PING does not consume a key**, matching Java's `PingCommand`, so mixing PING
  into a workload does not shift the key sequence other engines would produce.

## Reproducing the Numbers Above

```bash
make server-standalone-start

# Populate the keyspace, then sweep connections.
make node-run \
  DRIVER=configs/drivers/default/ioredis.json \
  WORKLOAD=configs/workloads/reference/basic-standalone-single-client-1M-reqs.json \
  METRICS_OUTPUT=output/node-ioredis.ndjson

make server-standalone-stop
```

For a full matrix across drivers and connection counts, use the orchestrator —
Node drivers are registered in `DRIVER_ENGINE_MAP`, so it dispatches to
`make node-run` automatically:

```bash
python scripts/run_benchmark_matrix.py --matrix configs/matrices/<matrix>.json
```
