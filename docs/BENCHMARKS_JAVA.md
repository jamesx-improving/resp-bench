# Java Client Benchmarks — Full Details

Detailed performance comparison of Java client libraries running on GitHub Actions runners. Results are automatically updated via CI.

> **Note**: These benchmarks run on shared GitHub Actions runners. Results may have variance between runs due to noisy neighbor effects. The graphs show averages across multiple runs (n=count shown in labels).

## Single Client (1 connection)

### Throughput

![SET RPS](../graphs/java/1-client/rps-SET.png)
![GET RPS](../graphs/java/1-client/rps-GET.png)

### Latency P50

![SET Latency P50](../graphs/java/1-client/latency-p50-SET.png)
![GET Latency P50](../graphs/java/1-client/latency-p50-GET.png)

### Latency P95

![SET Latency P95](../graphs/java/1-client/latency-p95-SET.png)
![GET Latency P95](../graphs/java/1-client/latency-p95-GET.png)

### Latency P99

![SET Latency P99](../graphs/java/1-client/latency-p99-SET.png)
![GET Latency P99](../graphs/java/1-client/latency-p99-GET.png)

### Latency P999

![SET Latency P999](../graphs/java/1-client/latency-p999-SET.png)
![GET Latency P999](../graphs/java/1-client/latency-p999-GET.png)

---

## 10 Concurrent Clients

### Throughput

![SET RPS](../graphs/java/10-clients/rps-SET.png)
![GET RPS](../graphs/java/10-clients/rps-GET.png)

### Latency P50

![SET Latency P50](../graphs/java/10-clients/latency-p50-SET.png)
![GET Latency P50](../graphs/java/10-clients/latency-p50-GET.png)

### Latency P95

![SET Latency P95](../graphs/java/10-clients/latency-p95-SET.png)
![GET Latency P95](../graphs/java/10-clients/latency-p95-GET.png)

### Latency P99

![SET Latency P99](../graphs/java/10-clients/latency-p99-SET.png)
![GET Latency P99](../graphs/java/10-clients/latency-p99-GET.png)

### Latency P999

![SET Latency P999](../graphs/java/10-clients/latency-p999-SET.png)
![GET Latency P999](../graphs/java/10-clients/latency-p999-GET.png)

---

## 100 Concurrent Clients

### Throughput

![SET RPS](../graphs/java/100-clients/rps-SET.png)
![GET RPS](../graphs/java/100-clients/rps-GET.png)

### Latency P50

![SET Latency P50](../graphs/java/100-clients/latency-p50-SET.png)
![GET Latency P50](../graphs/java/100-clients/latency-p50-GET.png)

### Latency P95

![SET Latency P95](../graphs/java/100-clients/latency-p95-SET.png)
![GET Latency P95](../graphs/java/100-clients/latency-p95-GET.png)

### Latency P99

![SET Latency P99](../graphs/java/100-clients/latency-p99-SET.png)
![GET Latency P99](../graphs/java/100-clients/latency-p99-GET.png)

### Latency P999

![SET Latency P999](../graphs/java/100-clients/latency-p999-SET.png)
![GET Latency P999](../graphs/java/100-clients/latency-p999-GET.png)

---

## Drivers Tested

| Driver | Description |
|--------|-------------|
| Jedis | Synchronous Java Redis client |
| Lettuce | Asynchronous/reactive Java Redis client |
| Redisson | Feature-rich Redis Java client |
| Valkey-Glide | Valkey's official Java client |
| Spring Data Valkey (Glide) | Spring Data abstraction over Valkey-Glide |
| Spring Data Valkey (Jedis) | Spring Data abstraction over Jedis |
| Spring Data Valkey (Lettuce) | Spring Data abstraction over Lettuce |
| Spring Data Redis (Jedis) | Spring Data Redis abstraction over Jedis |
| Spring Data Redis (Lettuce) | Spring Data Redis abstraction over Lettuce |

## Workload Configuration

All benchmarks use the same reference workload:
- **1M requests** per phase (STEADY)
- **1M keys** with 16-byte keys, `bench:` prefix
- **512 bytes** data size for SET operations
- **50/50 GET/SET** command mix (uniform random)
- **No rate limiting** (max throughput)

---

## Interval Reporting (soak tests)

Set `reporting_interval_seconds` (integer > 0) on a phase to emit one interval record every N seconds to
a sidecar next to the metrics file (`<label>.ndjson` → `<label>.interval.ndjson`, appended and flushed
per record). Each record carries a per-window HdrHistogram per command (same `metrics.<CMD>` shape as the
phase record) plus a JVM `memory` block: `rss_bytes` (Linux only, from `/proc/self/statm`),
`heap_used_bytes`, `heap_committed_bytes`, `heap_max_bytes`, `nonheap_used_bytes`, `direct_buffer_bytes`,
`gc_count`, `gc_time_ms`, `thread_count`. Absent/`0` disables it; the phase record is unchanged either way.
For GLIDE, native memory shows up as `rss_bytes` growth with flat heap figures. Full spec:
[INTERVAL_REPORTING.md](INTERVAL_REPORTING.md).
