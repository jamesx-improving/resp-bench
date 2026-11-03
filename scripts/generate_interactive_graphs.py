#!/usr/bin/env python3
"""
Interactive Graph Generator for resp-bench benchmark results.

Supports two directory layouts (auto-detected):

  **Legacy layout** — subdirectories per client count:
    results/<N>-clients/<driver>.ndjson

  **Flat layout** — one NDJSON per series, all client counts inside:
    results/<label>.ndjson
    results/_manifest.json  (optional, for rich legends)

Generates a self-contained interactive HTML file with Plotly.js charts:
  1. RPS Scalability — total RPS vs client count per driver/variant
  2. RPS Delta — reference driver % advantage vs each other series
  3. Latency per Percentile — per-command (GET, SET, etc.) latency charts
     for p50, p95, p99, p99.9 with X=client count, Y=latency (µs)
  4. CPU Scalability — avg CPU% during STEADY phase vs client count (if .system.ndjson exists)
  5. RPS/CPU% Efficiency — throughput per unit CPU% vs client count (if .system.ndjson exists)
  6. CPU Efficiency Delta — reference driver % advantage in RPS/CPU% vs each driver

Driver version information is extracted from NDJSON metadata and displayed
in chart legends. For low-level drivers: "jedis (5.2.0)". For spring-data
wrappers with a secondary driver: "spring-data-valkey-glide (0.2.0 / valkey-glide 2.2.3)".

When a _manifest.json is present (produced by run_benchmark_matrix.py),
variant labels are enriched with config parameters (pool_size, env vars, etc.)
and colors are auto-shaded per driver family so that e.g. all SDV-glide variants
get different shades of teal.

Uses total workload RPS (totals.requests / phase.duration) rather than
per-command RPS, because all commands share the same phase timer and
per-command counts divided by total duration just reflects command weights,
not actual per-command throughput.

Latency data is extracted from per-command percentile summaries in the NDJSON
(metrics.<CMD>.latency.summary.{p50, p95, p99, p999}), averaged across
non-outlier runs for each driver and client count.

CPU data is loaded from .system.ndjson files (produced by system_monitor.py).
Each CPU sample has a timestamp_epoch; only samples falling within the
STEADY phase window (start_timestamp..finish_timestamp) are used.

Outlier filtering uses 4-method consensus detection (same as validate_stability.py):
  - Modified Z-Score (MAD-based, threshold=3.5)
  - IQR (box-plot, multiplier=1.5)
  - Percentage Deviation from Median (threshold=15%)
  - Grubbs' Test (alpha=0.05)
A run is discarded when flagged by ≥2 methods.
Latency and CPU data use the same outlier indices as RPS for consistency.

Color families:
  - spring-data-valkey-*: greens/teals
  - spring-data-redis-*: blues
  - Low-level Java drivers (jedis, lettuce, redisson, valkey-glide): reds/oranges/purples
  - When multiple variants of the same driver exist, auto-shading generates
    distinct shades within the driver's color family.

Usage:
    # Legacy layout (subdirectories per client count)
    python scripts/generate_interactive_graphs.py \\
        results/m5.metal-cache.r7g.2xlarge-valkey.8.2.0/reference \\
        --output graphs/interactive/

    # Flat layout (from run_benchmark_matrix.py)
    python scripts/generate_interactive_graphs.py \\
        results/valkey-glide-thread-sweep/ \\
        --output graphs/interactive/valkey-glide-thread-sweep/

    # Custom reference driver and title
    python scripts/generate_interactive_graphs.py \\
        results/m5.metal/run1 \\
        --output graphs/interactive/ \\
        --title "m5.metal → cache.r7g.2xlarge (Valkey 8.0.2)" \\
        --reference valkey-glide
"""

import argparse
import json
import math
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, stdev


# ═══════════════════════════════════════════════════════════════════════════════
# Outlier Detection (from validate_stability.py)
# ═══════════════════════════════════════════════════════════════════════════════

MODIFIED_Z_THRESHOLD = 3.5
IQR_MULTIPLIER = 1.5
PCT_DEVIATION_THRESHOLD = 15.0
GRUBBS_ALPHA = 0.05
CONSENSUS_MIN_METHODS = 2


def _norm_ppf(p):
    """Approximate inverse normal CDF (Beasley-Springer-Moro algorithm)."""
    a = [0, -3.969683028665376e+01, 2.209460984245205e+02,
         -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00]
    b = [0, -5.447609879822406e+01, 1.615858368580409e+02,
         -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01]
    c = [0, -7.784894002430293e-03, -3.223964580411365e-01,
         -2.400758277161838e+00, -2.549732539343734e+00,
         4.374664141464968e+00, 2.938163982698783e+00]
    d = [0, 7.784695709041462e-03, 3.224671290700398e-01,
         2.445134137142996e+00, 3.754408661907416e+00]
    p_low = 0.02425
    p_high = 1.0 - p_low
    if p < p_low:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[1]*q + c[2])*q + c[3])*q + c[4])*q + c[5])*q + c[6]) / \
               ((((d[1]*q + d[2])*q + d[3])*q + d[4])*q + 1.0)
    elif p <= p_high:
        q = p - 0.5
        r = q * q
        return (((((a[1]*r + a[2])*r + a[3])*r + a[4])*r + a[5])*r + a[6]) * q / \
               (((((b[1]*r + b[2])*r + b[3])*r + b[4])*r + b[5])*r + 1.0)
    else:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((c[1]*q + c[2])*q + c[3])*q + c[4])*q + c[5])*q + c[6]) / \
                ((((d[1]*q + d[2])*q + d[3])*q + d[4])*q + 1.0)


def _t_critical_two_sided(df, alpha=0.05):
    """Approximate t-critical value using Cornish-Fisher expansion."""
    p = 1.0 - alpha / 2.0
    z = _norm_ppf(p)
    g1 = (z**3 + z) / 4.0
    g2 = (5*z**5 + 16*z**3 + 3*z) / 96.0
    g3 = (3*z**7 + 19*z**5 + 17*z**3 - 15*z) / 384.0
    return z + g1/df + g2/df**2 + g3/df**3


def grubbs_critical_value(n, alpha=0.05):
    """Compute Grubbs' test critical value for sample size n."""
    if n < 3:
        return float('inf')
    t_crit = _t_critical_two_sided(n - 2, alpha / (2.0 * n))
    return ((n - 1) / math.sqrt(n)) * math.sqrt(t_crit**2 / (n - 2 + t_crit**2))


def detect_modified_zscore(values):
    if len(values) < 3:
        return set()
    med = median(values)
    mad = median([abs(v - med) for v in values])
    if mad == 0:
        mad = mean([abs(v - med) for v in values])
    if mad == 0:
        return set()
    flagged = set()
    for i, v in enumerate(values):
        modified_z = 0.6745 * (v - med) / mad
        if abs(modified_z) > MODIFIED_Z_THRESHOLD:
            flagged.add(i)
    return flagged


def detect_iqr(values):
    if len(values) < 4:
        return set()
    sorted_v = sorted(values)
    n = len(sorted_v)
    q1 = sorted_v[n // 4]
    q3 = sorted_v[(3 * n) // 4]
    iqr = q3 - q1
    lower = q1 - IQR_MULTIPLIER * iqr
    upper = q3 + IQR_MULTIPLIER * iqr
    flagged = set()
    for i, v in enumerate(values):
        if v < lower or v > upper:
            flagged.add(i)
    return flagged


def detect_pct_deviation(values):
    if len(values) < 2:
        return set()
    med = median(values)
    if med == 0:
        return set()
    flagged = set()
    for i, v in enumerate(values):
        pct_dev = abs(v - med) / med * 100
        if pct_dev > PCT_DEVIATION_THRESHOLD:
            flagged.add(i)
    return flagged


def detect_grubbs(values):
    n = len(values)
    if n < 3:
        return set()
    m = mean(values)
    s = stdev(values)
    if s == 0:
        return set()
    g_values = [(abs(v - m) / s, i) for i, v in enumerate(values)]
    g_values.sort(reverse=True)
    g_stat, idx = g_values[0]
    g_crit = grubbs_critical_value(n, GRUBBS_ALPHA)
    if g_stat > g_crit:
        return {idx}
    return set()


def find_consensus_outliers(values):
    """Return set of indices flagged by ≥2 outlier detection methods."""
    if len(values) < 3:
        return set()
    flags_mz = detect_modified_zscore(values)
    flags_iqr = detect_iqr(values)
    flags_pct = detect_pct_deviation(values)
    flags_grubbs = detect_grubbs(values)

    all_indices = flags_mz | flags_iqr | flags_pct | flags_grubbs
    consensus = set()
    for idx in all_indices:
        count = sum(1 for s in [flags_mz, flags_iqr, flags_pct, flags_grubbs] if idx in s)
        if count >= CONSENSUS_MIN_METHODS:
            consensus.add(idx)
    return consensus


# ═══════════════════════════════════════════════════════════════════════════════
# Data Loading
# ═══════════════════════════════════════════════════════════════════════════════

def parse_client_count(dirname):
    """Extract client count from directory name like '1-clients' or '4-clients'."""
    m = re.match(r"^(\d+)-clients?$", dirname)
    return int(m.group(1)) if m else None


def load_steady_records(ndjson_path):
    """Load all STEADY phase records from an NDJSON file."""
    records = []
    with open(ndjson_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if data.get("phase", {}).get("id") == "STEADY":
                    records.append(data)
            except json.JSONDecodeError:
                continue
    return records


def extract_total_rps(records):
    """From a list of STEADY records, extract total RPS values.

    Uses totals.requests / phase.duration_ms for each run.
    Per-command RPS is not meaningful because all commands share the same
    phase duration — the phase ends when ALL commands complete, so
    per-command counts divided by total duration just reflects command
    weights, not actual per-command throughput.

    Returns e.g. [10000.1, 10200.2, ...]
    """
    rps_values = []
    for rec in records:
        duration_ms = rec["phase"]["duration_ms"]
        if duration_ms <= 0:
            continue
        total_requests = rec.get("totals", {}).get("requests", 0)
        rps = total_requests / (duration_ms / 1000.0)
        rps_values.append(rps)
    return rps_values


def extract_latency_percentiles(records):
    """From a list of STEADY records, extract per-command latency percentiles.

    Returns dict:
        { (command, percentile): [value_run1, value_run2, ...] }
    where percentile is one of 'p50', 'p95', 'p99', 'p999'
    and values are in microseconds.
    """
    PERCENTILES = ["p50", "p95", "p99", "p999"]
    result = defaultdict(list)

    for rec in records:
        metrics = rec.get("metrics", {})
        for cmd_name, cmd_data in metrics.items():
            summary = cmd_data.get("latency", {}).get("summary", {})
            for pct in PERCENTILES:
                val = summary.get(pct)
                if val is not None:
                    result[(cmd_name, pct)].append(val)

    return result


def load_all_data(results_dir):
    """
    Load all benchmark data with consensus outlier filtering.

    Returns:
        data[driver] = [(client_count, avg_rps), ...] sorted by client_count
        outlier_stats: dict with filtering statistics
        rps_outlier_map: dict[(driver, client_count)] = set of outlier run indices
            Used by load_cpu_data() to exclude CPU data for the same runs.
    """
    data = defaultdict(list)
    total_runs = 0
    discarded_runs = 0
    rps_outlier_map = {}  # (driver, client_count) -> set of outlier indices

    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        client_count = parse_client_count(subdir.name)
        if client_count is None:
            continue

        for ndjson_file in sorted(subdir.glob("*.ndjson")):
            # Skip .system.ndjson files
            if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
                continue
            driver = ndjson_file.stem
            records = load_steady_records(ndjson_file)
            if not records:
                continue

            rps_values = extract_total_rps(records)
            if not rps_values:
                continue

            total_runs += len(rps_values)

            # Find consensus outliers
            outlier_indices = find_consensus_outliers(rps_values)
            discarded_runs += len(outlier_indices)

            # Store outlier indices for CPU data correlation
            rps_outlier_map[(driver, client_count)] = outlier_indices

            # Keep non-outlier values
            clean_values = [v for i, v in enumerate(rps_values) if i not in outlier_indices]

            if clean_values:
                avg_rps = mean(clean_values)
                data[driver].append((client_count, avg_rps))

    # Sort by client count
    for driver in data:
        data[driver].sort(key=lambda x: x[0])

    stats = {
        "total_runs": total_runs,
        "discarded_runs": discarded_runs,
        "kept_runs": total_runs - discarded_runs,
    }
    return data, stats, rps_outlier_map


# ═══════════════════════════════════════════════════════════════════════════════
# Latency Data Loading
# ═══════════════════════════════════════════════════════════════════════════════

LATENCY_PERCENTILES = ["p50", "p95", "p99", "p999"]


def load_latency_data(results_dir, rps_outlier_map=None):
    """Load per-command latency percentile data with RPS-correlated outlier filtering.

    For each driver and client count, extracts latency percentiles from STEADY
    records and averages them across non-outlier runs.

    Args:
        results_dir: Path to the results directory.
        rps_outlier_map: dict[(driver, client_count)] -> set of outlier run indices
            from load_all_data(). Ensures latency charts use the same run set as RPS.

    Returns:
        latency_data: dict[(driver, command, percentile)] = [(client_count, avg_us), ...]
            sorted by client_count
        commands: sorted list of command names found (e.g. ['GET', 'SET'])
    """
    if rps_outlier_map is None:
        rps_outlier_map = {}

    # Accumulator: (driver, command, percentile) -> list of (client_count, avg_value)
    latency_data = defaultdict(list)
    commands_found = set()

    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        client_count = parse_client_count(subdir.name)
        if client_count is None:
            continue

        for ndjson_file in sorted(subdir.glob("*.ndjson")):
            if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
                continue
            driver = ndjson_file.stem
            records = load_steady_records(ndjson_file)
            if not records:
                continue

            pct_data = extract_latency_percentiles(records)
            if not pct_data:
                continue

            outlier_indices = rps_outlier_map.get((driver, client_count), set())

            for (cmd, pct), values in pct_data.items():
                commands_found.add(cmd)
                clean = [v for i, v in enumerate(values) if i not in outlier_indices]
                if clean:
                    latency_data[(driver, cmd, pct)].append((client_count, mean(clean)))

    # Sort by client count
    for key in latency_data:
        latency_data[key].sort(key=lambda x: x[0])

    return latency_data, sorted(commands_found)


# ═══════════════════════════════════════════════════════════════════════════════
# Driver Version Extraction
# ═══════════════════════════════════════════════════════════════════════════════

def extract_driver_versions(results_dir):
    """Extract driver version info from NDJSON metadata.

    Scans the first STEADY record of each driver to extract:
      - primary_driver_version
      - secondary_driver_id + secondary_driver_version (for spring-data-* wrappers)

    Returns:
        versions: dict[driver_name] = {
            "primary_version": "...",
            "secondary_id": "..." or None,
            "secondary_version": "..." or None,
            "label": "driver (primary_ver)" or "driver (primary_ver / secondary_id secondary_ver)"
        }
    """
    versions = {}

    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        if parse_client_count(subdir.name) is None:
            continue

        for ndjson_file in sorted(subdir.glob("*.ndjson")):
            if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
                continue
            driver = ndjson_file.stem
            if driver in versions:
                continue  # Already found version for this driver

            try:
                with open(ndjson_file) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            if data.get("phase", {}).get("id") != "STEADY":
                                continue
                            meta = data.get("metadata", {})
                            primary_ver = meta.get("primary_driver_version")
                            secondary_id = meta.get("secondary_driver_id")
                            secondary_ver = meta.get("secondary_driver_version")

                            if primary_ver:
                                label = f"{driver} ({primary_ver})"
                                if secondary_id and secondary_ver:
                                    label = f"{driver} ({primary_ver} / {secondary_id} {secondary_ver})"

                                versions[driver] = {
                                    "primary_version": primary_ver,
                                    "secondary_id": secondary_id,
                                    "secondary_version": secondary_ver,
                                    "label": label,
                                }
                            break  # Only need first STEADY record
                        except json.JSONDecodeError:
                            continue
            except (OSError, IOError):
                continue

    return versions


def get_driver_label(driver, driver_versions):
    """Get display label for a driver, including version info if available."""
    if driver_versions and driver in driver_versions:
        return driver_versions[driver]["label"]
    return driver


# ═══════════════════════════════════════════════════════════════════════════════
# CPU Data Loading
# ═══════════════════════════════════════════════════════════════════════════════

def parse_iso_to_epoch(iso_str):
    """Parse Java Instant.toString() format to epoch seconds.

    Handles formats like '2026-03-09T20:01:30.500Z' and '2026-03-09T20:01:30Z'.
    """
    try:
        # Try with fractional seconds
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except (ValueError, AttributeError):
        return None


def load_cpu_samples(cpu_ndjson_path):
    """Load all CPU samples from a .system.ndjson file.

    Returns list of (epoch, cpu_percent) tuples.
    """
    samples = []
    try:
        with open(cpu_ndjson_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    epoch = rec.get("timestamp_epoch")
                    cpu_pct = rec.get("cpu_percent")
                    if epoch is not None and cpu_pct is not None:
                        samples.append((epoch, cpu_pct))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return samples


def extract_steady_time_windows(records):
    """Extract STEADY phase time windows from benchmark records.

    Returns list of (start_epoch, end_epoch) tuples, one per run.
    """
    windows = []
    for rec in records:
        phase = rec.get("phase", {})
        start_ts = phase.get("start_timestamp")
        finish_ts = phase.get("finish_timestamp")
        if start_ts and finish_ts:
            start_epoch = parse_iso_to_epoch(start_ts)
            end_epoch = parse_iso_to_epoch(finish_ts)
            if start_epoch is not None and end_epoch is not None:
                windows.append((start_epoch, end_epoch))
    return windows


def compute_avg_cpu_for_window(samples, start_epoch, end_epoch):
    """Compute average CPU% from samples within a time window."""
    matching = [cpu for epoch, cpu in samples if start_epoch <= epoch <= end_epoch]
    if matching:
        return mean(matching)
    return None


def load_cpu_data(results_dir, rps_outlier_map=None):
    """Load CPU data correlated with STEADY phase windows.

    For each driver and client count, reads the .system.ndjson file and the
    corresponding .ndjson file, extracts STEADY phase time windows, and
    computes average CPU% during each STEADY window.

    CPU data for runs flagged as RPS outliers is excluded to ensure
    consistency between RPS and CPU charts (same runs are kept/discarded).

    Args:
        results_dir: Path to the results directory.
        rps_outlier_map: dict[(driver, client_count)] -> set of outlier run indices
            from load_all_data(). If provided, those runs are excluded from CPU
            averaging instead of running independent outlier detection on CPU values.

    Returns:
        cpu_data[driver] = [(client_count, avg_cpu_pct), ...] sorted by client_count
        has_cpu_data: bool indicating if any CPU data was found
    """
    if rps_outlier_map is None:
        rps_outlier_map = {}

    cpu_data = defaultdict(list)
    has_data = False

    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        client_count = parse_client_count(subdir.name)
        if client_count is None:
            continue

        for ndjson_file in sorted(subdir.glob("*.ndjson")):
            # Skip .system.ndjson files
            if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
                continue

            driver = ndjson_file.stem
            cpu_file = subdir / f"{driver}.system.ndjson"
            if not cpu_file.exists():
                continue

            # Load benchmark records to get STEADY windows
            records = load_steady_records(ndjson_file)
            if not records:
                continue

            windows = extract_steady_time_windows(records)
            if not windows:
                continue

            # Load CPU samples
            cpu_samples = load_cpu_samples(cpu_file)
            if not cpu_samples:
                continue

            # Compute avg CPU for each STEADY window
            cpu_values = []
            for start_e, end_e in windows:
                avg = compute_avg_cpu_for_window(cpu_samples, start_e, end_e)
                if avg is not None:
                    cpu_values.append(avg)

            if not cpu_values:
                continue

            has_data = True

            # Use RPS outlier indices to exclude the same runs,
            # ensuring CPU and RPS charts are based on identical run sets.
            outlier_indices = rps_outlier_map.get((driver, client_count), set())
            clean = [v for i, v in enumerate(cpu_values) if i not in outlier_indices]

            if clean:
                cpu_data[driver].append((client_count, mean(clean)))

    # Sort by client count
    for driver in cpu_data:
        cpu_data[driver].sort(key=lambda x: x[0])

    return cpu_data, has_data


# ═══════════════════════════════════════════════════════════════════════════════
# Color Configuration
# ═══════════════════════════════════════════════════════════════════════════════

# 3 families with distinct colors
DRIVER_COLORS = {
    # spring-data-valkey family — greens/teals
    "spring-data-valkey-glide":   "#00897B",  # teal 600
    "spring-data-valkey-jedis":   "#43A047",  # green 600
    "spring-data-valkey-lettuce": "#7CB342",  # light green 600

    # spring-data-redis family — blues
    "spring-data-redis-jedis":    "#1E88E5",  # blue 600
    "spring-data-redis-lettuce":  "#5C6BC0",  # indigo 400

    # Low-level Java drivers — reds/oranges/purples
    "valkey-glide": "#E53935",  # red 600
    "jedis":        "#FB8C00",  # orange 600
    "lettuce":      "#F4511E",  # deep orange 600
    "redisson":     "#8E24AA",  # purple 600

    # C# (.NET) drivers — cyan/pink
    "stackexchange-redis": "#00ACC1",  # cyan 600
    "valkey-glide-csharp": "#D81B60",  # pink 600

    # Node.js drivers — yellows/browns
    "valkey-glide-node": "#F9A825",  # yellow 800
    "ioredis":           "#6D4C41",  # brown 600
    "iovalkey":          "#546E7A",  # blue grey 600
}

DRIVER_FAMILIES = {
    "spring-data-valkey-glide":   "spring-data-valkey",
    "spring-data-valkey-jedis":   "spring-data-valkey",
    "spring-data-valkey-lettuce": "spring-data-valkey",
    "spring-data-redis-jedis":    "spring-data-redis",
    "spring-data-redis-lettuce":  "spring-data-redis",
    "valkey-glide":               "low-level",
    "jedis":                      "low-level",
    "lettuce":                    "low-level",
    "redisson":                   "low-level",
    "stackexchange-redis":        "csharp",
    "valkey-glide-csharp":        "csharp",
    "valkey-glide-node":          "node",
    "ioredis":                    "node",
    "iovalkey":                   "node",
}

FAMILY_ORDER = ["spring-data-valkey", "spring-data-redis", "low-level", "csharp", "node"]

FAMILY_LABELS = {
    "spring-data-valkey": "Spring Data Valkey",
    "spring-data-redis":  "Spring Data Redis",
    "low-level":          "Low-Level Java Drivers",
    "csharp":             "C# (.NET) Drivers",
    "node":               "Node.js Drivers",
}

# Fallback color for unknown drivers
_FALLBACK_COLORS = ["#795548", "#607D8B", "#9E9E9E", "#FF5722", "#00BCD4"]
_fallback_idx = 0


def get_driver_color(driver):
    global _fallback_idx
    if driver in DRIVER_COLORS:
        return DRIVER_COLORS[driver]
    color = _FALLBACK_COLORS[_fallback_idx % len(_FALLBACK_COLORS)]
    _fallback_idx += 1
    DRIVER_COLORS[driver] = color
    return color


def get_driver_family(driver):
    return DRIVER_FAMILIES.get(driver, "other")


# ═══════════════════════════════════════════════════════════════════════════════
# Auto-Shade Color Generation (for multi-variant per driver)
# ═══════════════════════════════════════════════════════════════════════════════

# Base hue/color per driver name (used for auto-shading when a driver has
# multiple config variants). The base color is the "600" shade; lighter/darker
# variants are generated automatically.
DRIVER_BASE_COLORS = {
    "spring-data-valkey-glide":   (174, 63, 48),  # teal HSL approx
    "spring-data-valkey-jedis":   (122, 50, 45),
    "spring-data-valkey-lettuce": (88, 47, 48),
    "spring-data-redis-jedis":    (211, 79, 50),
    "spring-data-redis-lettuce":  (232, 43, 55),
    "valkey-glide":               (2, 76, 55),
    "jedis":                      (33, 96, 49),
    "lettuce":                    (14, 89, 50),
    "redisson":                   (277, 65, 40),
    "stackexchange-redis":        (187, 80, 38),  # cyan HSL
    "valkey-glide-csharp":        (340, 75, 47),  # pink HSL
}

# Line dash patterns for distinguishing variants
LINE_DASHES = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]


def _hsl_to_hex(h, s, l):
    """Convert HSL to hex color string."""
    s = s / 100.0
    l = l / 100.0
    c = (1 - abs(2 * l - 1)) * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = l - c / 2

    if h < 60:
        r, g, b = c, x, 0
    elif h < 120:
        r, g, b = x, c, 0
    elif h < 180:
        r, g, b = 0, c, x
    elif h < 240:
        r, g, b = 0, x, c
    elif h < 300:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x

    r = int((r + m) * 255)
    g = int((g + m) * 255)
    b = int((b + m) * 255)
    return f"#{r:02X}{g:02X}{b:02X}"


def assign_variant_colors(series_labels, manifest=None):
    """Assign colors to series labels, auto-shading when multiple variants of the same driver exist.

    Args:
        series_labels: list of series label strings
        manifest: parsed _manifest.json dict (optional)

    Returns:
        colors: dict[label] -> hex color string
        dashes: dict[label] -> dash pattern string
        families: dict[label] -> family name for legend grouping
        display_labels: dict[label] -> rich display label
    """
    colors = {}
    dashes = {}
    families = {}
    display_labels = {}

    # Group series by driver name
    driver_groups = defaultdict(list)
    for label in series_labels:
        driver_name = _infer_driver_name(label, manifest)
        driver_groups[driver_name].append(label)

    for driver_name, labels in driver_groups.items():
        base_hsl = DRIVER_BASE_COLORS.get(driver_name)
        family = get_driver_family(driver_name)

        if len(labels) == 1:
            # Single variant — use the standard color
            label = labels[0]
            colors[label] = get_driver_color(driver_name)
            dashes[label] = "solid"
            families[label] = family
            display_labels[label] = _build_display_label(label, manifest)
        else:
            # Multiple variants — generate shades
            h, s, l = base_hsl if base_hsl else (0, 0, 50)
            n = len(labels)

            # Spread lightness from 30% to 70% around the base
            l_min = max(25, l - 20)
            l_max = min(75, l + 20)

            for i, label in enumerate(sorted(labels)):
                if n > 1:
                    variant_l = l_min + (l_max - l_min) * i / (n - 1)
                else:
                    variant_l = l
                colors[label] = _hsl_to_hex(h, s, variant_l)
                dashes[label] = LINE_DASHES[i % len(LINE_DASHES)]
                families[label] = family
                display_labels[label] = _build_display_label(label, manifest)

    return colors, dashes, families, display_labels


def _infer_driver_name(label, manifest=None):
    """Infer the base driver name from a series label.

    If a manifest is available, uses the driver_name field.
    Otherwise, tries to match known driver names as a prefix of the label.
    """
    if manifest and "variants" in manifest:
        variant_info = manifest["variants"].get(label, {})
        driver_name = variant_info.get("driver_name")
        if driver_name:
            return driver_name

    # Heuristic: try matching known driver names (longest first)
    # Labels use @ as separator: "driver-name@param1=val,param2=val"
    known = sorted(DRIVER_COLORS.keys(), key=len, reverse=True)
    for name in known:
        if label == name or label.startswith(name + "@"):
            return name

    return label


def _build_display_label(label, manifest=None):
    """Build a rich display label from manifest metadata."""
    if not manifest or "variants" not in manifest:
        return label

    variant_info = manifest["variants"].get(label, {})
    if not variant_info:
        return label

    driver_name = variant_info.get("driver_name", label)
    params = variant_info.get("params", {})
    bindings = variant_info.get("bindings", {})

    if not params and not bindings:
        return label

    parts = [driver_name]
    for k, v in sorted(params.items()):
        if k == "env" and isinstance(v, dict):
            for ek, ev in sorted(v.items()):
                short = ek.replace("GLIDE_TOKIO_WORKER_THREADS", "tokio") \
                          .replace("GLIDE_CALLBACK_WORKER_THREADS", "callback")
                parts.append(f"{short}={ev}")
        else:
            parts.append(f"{k}={v}")
    for k, v in sorted(bindings.items()):
        ref = v[1:] if isinstance(v, str) and v.startswith("$") else v
        parts.append(f"{k}={ref}")

    return " | ".join([parts[0], ", ".join(parts[1:])]) if len(parts) > 1 else parts[0]


# ═══════════════════════════════════════════════════════════════════════════════
# Layout Detection & Manifest Loading
# ═══════════════════════════════════════════════════════════════════════════════

def detect_layout(results_dir):
    """Detect whether the results directory uses legacy or flat layout.

    Legacy: has subdirectories matching N-clients/ pattern.
    Flat: has .ndjson files directly in the directory (no subdirs with client counts).

    Returns 'legacy' or 'flat'.
    """
    has_client_subdirs = False
    has_flat_ndjson = False

    for item in results_dir.iterdir():
        if item.is_dir() and parse_client_count(item.name) is not None:
            has_client_subdirs = True
        if item.is_file() and item.name.endswith(".ndjson") and not item.name.startswith("_"):
            if not item.name.endswith(".system.ndjson"):
                has_flat_ndjson = True

    if has_client_subdirs:
        return "legacy"
    if has_flat_ndjson:
        return "flat"
    return "legacy"  # default fallback


def load_manifest(results_dir):
    """Load _manifest.json from results directory if it exists.

    Returns manifest dict or None.
    """
    manifest_path = results_dir / "_manifest.json"
    if manifest_path.exists():
        try:
            with open(manifest_path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Failed to load manifest: {e}", file=sys.stderr)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Flat Layout Data Loading
# ═══════════════════════════════════════════════════════════════════════════════

def load_all_data_flat(results_dir):
    """Load benchmark data from flat layout (one NDJSON per series, all client counts inside).

    Groups STEADY records by phase.connections to build per-client-count data points.

    Returns same format as load_all_data():
        data[series_label] = [(client_count, avg_rps), ...] sorted by client_count
        outlier_stats: dict with filtering statistics
        rps_outlier_map: dict[(series_label, client_count)] = set of outlier run indices
    """
    data = defaultdict(list)
    total_runs = 0
    discarded_runs = 0
    rps_outlier_map = {}

    for ndjson_file in sorted(results_dir.glob("*.ndjson")):
        if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
            continue
        if ndjson_file.name.startswith("_"):
            continue

        series_label = ndjson_file.stem
        records = load_steady_records(ndjson_file)
        if not records:
            continue

        # Group records by connection count
        by_connections = defaultdict(list)
        for rec in records:
            cc = rec.get("phase", {}).get("connections")
            if cc is not None:
                by_connections[cc].append(rec)

        for client_count, cc_records in sorted(by_connections.items()):
            rps_values = extract_total_rps(cc_records)
            if not rps_values:
                continue

            total_runs += len(rps_values)

            outlier_indices = find_consensus_outliers(rps_values)
            discarded_runs += len(outlier_indices)

            rps_outlier_map[(series_label, client_count)] = outlier_indices

            clean_values = [v for i, v in enumerate(rps_values) if i not in outlier_indices]
            if clean_values:
                data[series_label].append((client_count, mean(clean_values)))

    for label in data:
        data[label].sort(key=lambda x: x[0])

    stats = {
        "total_runs": total_runs,
        "discarded_runs": discarded_runs,
        "kept_runs": total_runs - discarded_runs,
    }
    return data, stats, rps_outlier_map


def load_latency_data_flat(results_dir, rps_outlier_map=None):
    """Load latency data from flat layout."""
    if rps_outlier_map is None:
        rps_outlier_map = {}

    latency_data = defaultdict(list)
    commands_found = set()

    for ndjson_file in sorted(results_dir.glob("*.ndjson")):
        if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
            continue
        if ndjson_file.name.startswith("_"):
            continue

        series_label = ndjson_file.stem
        records = load_steady_records(ndjson_file)
        if not records:
            continue

        by_connections = defaultdict(list)
        for rec in records:
            cc = rec.get("phase", {}).get("connections")
            if cc is not None:
                by_connections[cc].append(rec)

        for client_count, cc_records in sorted(by_connections.items()):
            pct_data = extract_latency_percentiles(cc_records)
            if not pct_data:
                continue

            outlier_indices = rps_outlier_map.get((series_label, client_count), set())

            for (cmd, pct), values in pct_data.items():
                commands_found.add(cmd)
                clean = [v for i, v in enumerate(values) if i not in outlier_indices]
                if clean:
                    latency_data[(series_label, cmd, pct)].append((client_count, mean(clean)))

    for key in latency_data:
        latency_data[key].sort(key=lambda x: x[0])

    return latency_data, sorted(commands_found)


def extract_driver_versions_flat(results_dir):
    """Extract driver versions from flat layout NDJSON files."""
    versions = {}

    for ndjson_file in sorted(results_dir.glob("*.ndjson")):
        if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
            continue
        if ndjson_file.name.startswith("_"):
            continue

        series_label = ndjson_file.stem
        if series_label in versions:
            continue

        try:
            with open(ndjson_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        if rec.get("phase", {}).get("id") != "STEADY":
                            continue
                        meta = rec.get("metadata", {})
                        primary_ver = meta.get("primary_driver_version")
                        secondary_id = meta.get("secondary_driver_id")
                        secondary_ver = meta.get("secondary_driver_version")

                        if primary_ver:
                            label = f"{series_label} ({primary_ver})"
                            if secondary_id and secondary_ver:
                                label = f"{series_label} ({primary_ver} / {secondary_id} {secondary_ver})"
                            versions[series_label] = {
                                "primary_version": primary_ver,
                                "secondary_id": secondary_id,
                                "secondary_version": secondary_ver,
                                "label": label,
                            }
                        break
                    except json.JSONDecodeError:
                        continue
        except (OSError, IOError):
            continue

    return versions


def load_cpu_data_flat(results_dir, rps_outlier_map=None):
    """Load CPU data from flat layout."""
    if rps_outlier_map is None:
        rps_outlier_map = {}

    cpu_data = defaultdict(list)
    has_data = False

    for ndjson_file in sorted(results_dir.glob("*.ndjson")):
        if ndjson_file.name.endswith((".system.ndjson", ".interval.ndjson")):
            continue
        if ndjson_file.name.startswith("_"):
            continue

        series_label = ndjson_file.stem
        cpu_file = results_dir / f"{series_label}.system.ndjson"
        if not cpu_file.exists():
            continue

        records = load_steady_records(ndjson_file)
        if not records:
            continue

        cpu_samples = load_cpu_samples(cpu_file)
        if not cpu_samples:
            continue

        # Group records by connections
        by_connections = defaultdict(list)
        for rec in records:
            cc = rec.get("phase", {}).get("connections")
            if cc is not None:
                by_connections[cc].append(rec)

        for client_count, cc_records in sorted(by_connections.items()):
            windows = extract_steady_time_windows(cc_records)
            if not windows:
                continue

            cpu_values = []
            for start_e, end_e in windows:
                avg = compute_avg_cpu_for_window(cpu_samples, start_e, end_e)
                if avg is not None:
                    cpu_values.append(avg)

            if not cpu_values:
                continue

            has_data = True

            outlier_indices = rps_outlier_map.get((series_label, client_count), set())
            clean = [v for i, v in enumerate(cpu_values) if i not in outlier_indices]

            if clean:
                cpu_data[series_label].append((client_count, mean(clean)))

    for label in cpu_data:
        cpu_data[label].sort(key=lambda x: x[0])

    return cpu_data, has_data


# ═══════════════════════════════════════════════════════════════════════════════
# HTML Generation with Plotly.js
# ═══════════════════════════════════════════════════════════════════════════════

REFERENCE_DRIVER = "spring-data-valkey-glide"


def _build_traces_for_family(data, drivers, family, family_label,
                              driver_versions=None, hover_value_label="RPS",
                              hover_value_fmt="%{y:,.0f}"):
    """Build Plotly traces for a list of drivers in a family."""
    traces = []
    for driver in sorted(drivers):
        points = data.get(driver, [])
        if not points:
            continue
        x = [p[0] for p in points]
        y = [p[1] for p in points]
        color = get_driver_color(driver)
        label = get_driver_label(driver, driver_versions)

        traces.append({
            "x": x,
            "y": y,
            "type": "scatter",
            "mode": "lines+markers",
            "name": label,
            "legendgroup": family,
            "legendgrouptitle": {"text": family_label},
            "line": {"color": color, "width": 2.5},
            "marker": {"size": 8, "color": color},
            "hovertemplate": (
                f"<b>{label}</b><br>"
                "Clients: %{x}<br>"
                f"{hover_value_label}: {hover_value_fmt}<br>"
                "<extra></extra>"
            ),
        })
    return traces


def _build_family_grouped_traces(data, driver_versions=None,
                                  hover_value_label="RPS",
                                  hover_value_fmt="%{y:,.0f}"):
    """Build Plotly traces grouped by driver family."""
    traces = []

    family_drivers = defaultdict(list)
    for driver in data:
        family = get_driver_family(driver)
        family_drivers[family].append(driver)

    for family in FAMILY_ORDER:
        family_label = FAMILY_LABELS.get(family, family)
        traces.extend(_build_traces_for_family(
            data, family_drivers.get(family, []), family, family_label,
            driver_versions=driver_versions,
            hover_value_label=hover_value_label,
            hover_value_fmt=hover_value_fmt))

    # Also include "other" family if present
    traces.extend(_build_traces_for_family(
        data, family_drivers.get("other", []), "other", "Other",
        driver_versions=driver_versions,
        hover_value_label=hover_value_label,
        hover_value_fmt=hover_value_fmt))

    return traces


def build_scalability_traces(data, driver_versions=None):
    """Build Plotly traces for the scalability chart (total RPS vs client count)."""
    return _build_family_grouped_traces(
        data, driver_versions=driver_versions,
        hover_value_label="RPS", hover_value_fmt="%{y:,.0f}")


def build_latency_traces(latency_data, command, percentile, driver_versions=None):
    """Build Plotly traces for a latency percentile chart.

    Reshapes latency_data[(driver, command, percentile)] into
    a driver -> [(client_count, value)] dict, then uses the standard
    family-grouped trace builder.

    Args:
        latency_data: dict from load_latency_data()
        command: e.g. 'GET' or 'SET'
        percentile: e.g. 'p50', 'p95', 'p99', 'p999'
        driver_versions: dict from extract_driver_versions()

    Returns:
        list of Plotly trace dicts
    """
    # Reshape: driver -> [(client_count, avg_latency_us)]
    per_driver = {}
    for (drv, cmd, pct), points in latency_data.items():
        if cmd == command and pct == percentile:
            per_driver[drv] = points

    if not per_driver:
        return []

    pct_label = percentile.upper().replace("P", "p")  # p50, p95, p99, p999
    return _build_family_grouped_traces(
        per_driver, driver_versions=driver_versions,
        hover_value_label=f"{command} {pct_label} Latency (µs)",
        hover_value_fmt="%{y:,.0f}")


def build_delta_traces(data, driver_versions=None):
    """Build Plotly traces for delta chart (% glide advantage vs other drivers).

    Delta = ((glide_rps - other_rps) / other_rps) * 100
    Positive = glide is faster.
    """
    traces = []

    # Get reference driver data as a lookup: client_count -> rps
    ref_points = data.get(REFERENCE_DRIVER, [])
    if not ref_points:
        return traces
    ref_rps = {cc: rps for cc, rps in ref_points}

    # Group drivers by family
    family_drivers = defaultdict(list)
    for driver in data:
        if driver == REFERENCE_DRIVER:
            continue
        family = get_driver_family(driver)
        family_drivers[family].append(driver)

    def _delta_traces_for_drivers(drivers, family, family_label):
        result = []
        for driver in sorted(drivers):
            points = data.get(driver, [])
            if not points:
                continue

            x = []
            y = []
            for cc, other_rps in points:
                if cc in ref_rps and other_rps > 0:
                    delta_pct = ((ref_rps[cc] - other_rps) / other_rps) * 100.0
                    x.append(cc)
                    y.append(round(delta_pct, 2))

            if not x:
                continue

            color = get_driver_color(driver)
            label = get_driver_label(driver, driver_versions)

            result.append({
                "x": x,
                "y": y,
                "type": "scatter",
                "mode": "lines+markers",
                "name": label,
                "legendgroup": family,
                "legendgrouptitle": {"text": family_label},
                "line": {"color": color, "width": 2.5},
                "marker": {"size": 8, "color": color},
                "hovertemplate": (
                    f"<b>{label}</b><br>"
                    "Clients: %{x}<br>"
                    "Delta: %{y:+.1f}%<br>"
                    "<extra></extra>"
                ),
            })
        return result

    for family in FAMILY_ORDER:
        family_label = FAMILY_LABELS.get(family, family)
        traces.extend(_delta_traces_for_drivers(
            family_drivers.get(family, []), family, family_label))

    # Also "other" family
    traces.extend(_delta_traces_for_drivers(
        family_drivers.get("other", []), "other", "Other"))

    return traces


def _build_cpu_traces(cpu_data, hover_fmt):
    """Build Plotly traces for CPU-based charts using family grouping."""
    traces = []
    family_drivers = defaultdict(list)
    for driver in cpu_data:
        family = get_driver_family(driver)
        family_drivers[family].append(driver)

    for family in FAMILY_ORDER:
        family_label = FAMILY_LABELS.get(family, family)
        for driver in sorted(family_drivers.get(family, [])):
            points = cpu_data.get(driver, [])
            if not points:
                continue
            x = [p[0] for p in points]
            y = [p[1] for p in points]
            color = get_driver_color(driver)
            traces.append({
                "x": x,
                "y": y,
                "type": "scatter",
                "mode": "lines+markers",
                "name": driver,
                "legendgroup": family,
                "legendgrouptitle": {"text": family_label},
                "line": {"color": color, "width": 2.5},
                "marker": {"size": 8, "color": color},
                "hovertemplate": (
                    f"<b>{driver}</b><br>"
                    "Clients: %{x}<br>"
                    f"{hover_fmt}<br>"
                    "<extra></extra>"
                ),
            })

    # "other" family
    for driver in sorted(family_drivers.get("other", [])):
        points = cpu_data.get(driver, [])
        if not points:
            continue
        x = [p[0] for p in points]
        y = [p[1] for p in points]
        color = get_driver_color(driver)
        traces.append({
            "x": x, "y": y, "type": "scatter", "mode": "lines+markers",
            "name": driver, "legendgroup": "other",
            "legendgrouptitle": {"text": "Other"},
            "line": {"color": color, "width": 2.5},
            "marker": {"size": 8, "color": color},
            "hovertemplate": f"<b>{driver}</b><br>Clients: %{{x}}<br>{hover_fmt}<br><extra></extra>",
        })

    return traces


def build_efficiency_data(rps_data, cpu_data):
    """Compute RPS per CPU% for each driver and client count.

    Returns efficiency_data[driver] = [(client_count, rps_per_cpu_pct), ...]
    """
    efficiency = defaultdict(list)

    # Build lookups
    for driver in rps_data:
        rps_map = {cc: rps for cc, rps in rps_data[driver]}
        cpu_map = {cc: cpu for cc, cpu in cpu_data.get(driver, [])}

        for cc in sorted(set(rps_map) & set(cpu_map)):
            cpu_pct = cpu_map[cc]
            if cpu_pct > 0:
                efficiency[driver].append((cc, rps_map[cc] / cpu_pct))

    for driver in efficiency:
        efficiency[driver].sort(key=lambda x: x[0])

    return efficiency


def build_efficiency_delta_traces(eff_data):
    """Build Plotly traces for CPU efficiency delta chart.

    Shows % advantage of spring-data-valkey-glide in RPS/CPU% vs each other driver.
    Delta = ((glide_efficiency - other_efficiency) / other_efficiency) * 100
    Positive = glide gets more RPS per CPU% consumed.
    """
    traces = []

    ref_points = eff_data.get(REFERENCE_DRIVER, [])
    if not ref_points:
        return traces
    ref_eff = {cc: eff for cc, eff in ref_points}

    # Group drivers by family
    family_drivers = defaultdict(list)
    for driver in eff_data:
        if driver == REFERENCE_DRIVER:
            continue
        family = get_driver_family(driver)
        family_drivers[family].append(driver)

    def _delta_traces(drivers, family, family_label):
        result = []
        for driver in sorted(drivers):
            points = eff_data.get(driver, [])
            if not points:
                continue

            x = []
            y = []
            for cc, other_eff in points:
                if cc in ref_eff and other_eff > 0:
                    delta_pct = ((ref_eff[cc] - other_eff) / other_eff) * 100.0
                    x.append(cc)
                    y.append(round(delta_pct, 2))

            if not x:
                continue

            color = get_driver_color(driver)

            result.append({
                "x": x,
                "y": y,
                "type": "scatter",
                "mode": "lines+markers",
                "name": driver,
                "legendgroup": family,
                "legendgrouptitle": {"text": family_label},
                "line": {"color": color, "width": 2.5},
                "marker": {"size": 8, "color": color},
                "hovertemplate": (
                    f"<b>{driver}</b><br>"
                    "Clients: %{x}<br>"
                    "Delta: %{y:+.1f}%<br>"
                    "<extra></extra>"
                ),
            })
        return result

    for family in FAMILY_ORDER:
        family_label = FAMILY_LABELS.get(family, family)
        traces.extend(_delta_traces(
            family_drivers.get(family, []), family, family_label))

    # Also "other" family
    traces.extend(_delta_traces(
        family_drivers.get("other", []), "other", "Other"))

    return traces


def generate_html(data, stats, title_prefix, output_path, cpu_data=None,
                  latency_data=None, latency_commands=None, driver_versions=None):
    """Generate a self-contained interactive HTML file with scalability + delta + latency + CPU charts."""
    all_client_counts = sorted(set(
        cc for points in data.values()
        for cc, _ in points
    ))

    # Build chart data
    charts = []

    # Scalability chart
    scal_traces = build_scalability_traces(data, driver_versions=driver_versions)
    scal_layout = {
        "title": {
            "text": "RPS Scalability — Total Workload Throughput",
            "font": {"size": 18},
        },
        "xaxis": {
            "title": "Client Count",
            "type": "log",
            "tickvals": all_client_counts,
            "ticktext": [str(c) for c in all_client_counts],
        },
        "yaxis": {
            "title": "Requests per Second (RPS)",
            "rangemode": "tozero",
            "separatethousands": True,
        },
        "legend": {
            "groupclick": "toggleitem",
            "tracegroupgap": 10,
        },
        "hovermode": "closest",
        "template": "plotly_white",
        "height": 600,
    }
    charts.append({"traces": scal_traces, "layout": scal_layout, "id": "scalability-total"})

    # Delta chart
    delta_traces = build_delta_traces(data, driver_versions=driver_versions)
    delta_layout = {
        "title": {
            "text": f"{REFERENCE_DRIVER} Advantage — Total Workload Throughput",
            "font": {"size": 18},
        },
        "xaxis": {
            "title": "Client Count",
            "type": "log",
            "tickvals": all_client_counts,
            "ticktext": [str(c) for c in all_client_counts],
        },
        "yaxis": {
            "title": f"% Advantage of {REFERENCE_DRIVER}",
            "zeroline": True,
            "zerolinecolor": "#888",
            "zerolinewidth": 2,
        },
        "legend": {
            "groupclick": "toggleitem",
            "tracegroupgap": 10,
        },
        "hovermode": "closest",
        "template": "plotly_white",
        "height": 600,
        "shapes": [{
            "type": "line",
            "x0": 0, "x1": 1,
            "y0": 0, "y1": 0,
            "xref": "paper", "yref": "y",
            "line": {"color": "#888", "width": 1.5, "dash": "dash"},
        }],
    }
    charts.append({"traces": delta_traces, "layout": delta_layout, "id": "delta-total"})

    # Latency charts (per command × percentile) — after RPS, before CPU
    if latency_data and latency_commands:
        PERCENTILE_LABELS = {"p50": "p50 (Median)", "p95": "p95", "p99": "p99", "p999": "p99.9"}
        for cmd in latency_commands:
            for pct in LATENCY_PERCENTILES:
                lat_traces = build_latency_traces(
                    latency_data, cmd, pct, driver_versions=driver_versions)
                if not lat_traces:
                    continue
                pct_display = PERCENTILE_LABELS.get(pct, pct)
                chart_id = f"latency-{cmd.lower()}-{pct}"
                lat_layout = {
                    "title": {
                        "text": f"Latency {pct_display} — {cmd} Command",
                        "font": {"size": 18},
                    },
                    "xaxis": {
                        "title": "Client Count",
                        "type": "log",
                        "tickvals": all_client_counts,
                        "ticktext": [str(c) for c in all_client_counts],
                    },
                    "yaxis": {
                        "title": "Latency (µs)",
                        "rangemode": "tozero",
                        "separatethousands": True,
                    },
                    "legend": {
                        "groupclick": "toggleitem",
                        "tracegroupgap": 10,
                    },
                    "hovermode": "closest",
                    "template": "plotly_white",
                    "height": 600,
                }
                charts.append({"traces": lat_traces, "layout": lat_layout, "id": chart_id})

    # CPU charts (only if CPU data is available)
    if cpu_data:
        cpu_client_counts = sorted(set(
            cc for points in cpu_data.values() for cc, _ in points
        ))

        # CPU Scalability chart
        cpu_traces = _build_cpu_traces(cpu_data, "CPU: %{y:.1f}%")
        cpu_layout = {
            "title": {
                "text": "Client-Side CPU Usage — STEADY Phase Average",
                "font": {"size": 18},
            },
            "xaxis": {
                "title": "Client Count",
                "type": "log",
                "tickvals": cpu_client_counts,
                "ticktext": [str(c) for c in cpu_client_counts],
            },
            "yaxis": {
                "title": "CPU Usage (%)",
                "rangemode": "tozero",
            },
            "legend": {"groupclick": "toggleitem", "tracegroupgap": 10},
            "hovermode": "closest",
            "template": "plotly_white",
            "height": 600,
        }
        charts.append({"traces": cpu_traces, "layout": cpu_layout, "id": "cpu-scalability"})

        # Efficiency chart (RPS per CPU%)
        eff_data = build_efficiency_data(data, cpu_data)
        if eff_data:
            eff_traces = _build_cpu_traces(eff_data, "RPS/CPU%%: %{y:,.0f}")
            eff_layout = {
                "title": {
                    "text": "CPU Efficiency — RPS per CPU%",
                    "font": {"size": 18},
                },
                "xaxis": {
                    "title": "Client Count",
                    "type": "log",
                    "tickvals": cpu_client_counts,
                    "ticktext": [str(c) for c in cpu_client_counts],
                },
                "yaxis": {
                    "title": "RPS / CPU%",
                    "rangemode": "tozero",
                    "separatethousands": True,
                },
                "legend": {"groupclick": "toggleitem", "tracegroupgap": 10},
                "hovermode": "closest",
                "template": "plotly_white",
                "height": 600,
            }
            charts.append({"traces": eff_traces, "layout": eff_layout, "id": "cpu-efficiency"})

            # CPU Efficiency Delta chart (RPS/CPU% advantage)
            eff_delta_traces = build_efficiency_delta_traces(eff_data)
            if eff_delta_traces:
                eff_delta_layout = {
                    "title": {
                        "text": f"{REFERENCE_DRIVER} Advantage — CPU Efficiency (RPS per CPU%)",
                        "font": {"size": 18},
                    },
                    "xaxis": {
                        "title": "Client Count",
                        "type": "log",
                        "tickvals": cpu_client_counts,
                        "ticktext": [str(c) for c in cpu_client_counts],
                    },
                    "yaxis": {
                        "title": f"% Advantage of {REFERENCE_DRIVER} (RPS/CPU%)",
                        "zeroline": True,
                        "zerolinecolor": "#888",
                        "zerolinewidth": 2,
                    },
                    "legend": {"groupclick": "toggleitem", "tracegroupgap": 10},
                    "hovermode": "closest",
                    "template": "plotly_white",
                    "height": 600,
                    "shapes": [{
                        "type": "line",
                        "x0": 0, "x1": 1,
                        "y0": 0, "y1": 0,
                        "xref": "paper", "yref": "y",
                        "line": {"color": "#888", "width": 1.5, "dash": "dash"},
                    }],
                }
                charts.append({"traces": eff_delta_traces, "layout": eff_delta_layout, "id": "cpu-efficiency-delta"})

    # Build HTML
    title = "Interactive Benchmark Results"
    if title_prefix:
        title = f"{title_prefix} — {title}"

    discard_pct = (stats['discarded_runs'] / stats['total_runs'] * 100) if stats['total_runs'] > 0 else 0

    chart_scripts = ""
    for chart in charts:
        traces_json = json.dumps(chart["traces"], indent=None)
        layout_json = json.dumps(chart["layout"], indent=None)
        chart_scripts += f'    Plotly.newPlot("{chart["id"]}", {traces_json}, {layout_json}, {{responsive: true}});\n'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #fafafa;
            color: #333;
        }}
        .header {{
            max-width: 1400px;
            margin: 0 auto 20px;
            padding: 20px 30px;
            background: #fff;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px;
            font-size: 24px;
            color: #1a1a1a;
        }}
        .header .meta {{
            font-size: 14px;
            color: #666;
            line-height: 1.6;
        }}
        .header .meta .stat {{
            display: inline-block;
            background: #f0f4f8;
            padding: 2px 10px;
            border-radius: 4px;
            margin-right: 8px;
            font-family: monospace;
        }}
        .chart-container {{
            max-width: 1400px;
            margin: 0 auto 20px;
            padding: 15px;
            background: #fff;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .legend-help {{
            max-width: 1400px;
            margin: 0 auto 20px;
            padding: 12px 20px;
            background: #e8f5e9;
            border-radius: 6px;
            font-size: 13px;
            color: #2e7d32;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <div class="meta">
            <p>
                <span class="stat">Total runs: {stats['total_runs']}</span>
                <span class="stat">Discarded outliers: {stats['discarded_runs']} ({discard_pct:.1f}%)</span>
                <span class="stat">Kept runs: {stats['kept_runs']}</span>
            </p>
            <p>
                Outlier detection: 4-method consensus (Modified Z-Score + IQR + % Deviation + Grubbs' Test, ≥2 agree).
                RPS is total workload throughput (all commands combined).
                Delta charts show <b>{REFERENCE_DRIVER}</b> advantage: positive = glide is faster.
            </p>
        </div>
    </div>

    <div class="legend-help">
        💡 <b>Tip:</b> Click any driver in the legend to toggle it on/off. Double-click to isolate a single driver.
        Drivers are grouped by family for easy reference. Use the toolbar to zoom, pan, and download as PNG.
    </div>

{chr(10).join(f'    <div class="chart-container">' + chr(10) + f'        <div id="{chart["id"]}" style="width:100%;"></div>' + chr(10) + '    </div>' for chart in charts)}

    <script>
{chart_scripts}
    </script>
</body>
</html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(html)

    print(f"Generated: {output_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate interactive HTML benchmark graphs with Plotly.js",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Supports two directory layouts (auto-detected):

  Legacy layout — subdirectories per client count:
    results/<N>-clients/<driver>.ndjson

  Flat layout — one NDJSON per series (from run_benchmark_matrix.py):
    results/<label>.ndjson
    results/_manifest.json  (optional)

Examples:
  # Legacy layout
  python scripts/generate_interactive_graphs.py results/m5.metal/run1

  # Flat layout with custom reference
  python scripts/generate_interactive_graphs.py results/glide-sweep/ \\
      --reference spring-data-valkey-glide@cb=16,tw=16,pool_size=connections
""",
    )
    parser.add_argument(
        "results_dir",
        help="Directory containing benchmark results (legacy or flat layout)",
    )
    parser.add_argument(
        "--output", "-o",
        default="graphs/interactive/",
        help="Output directory for HTML file (default: graphs/interactive/)",
    )
    parser.add_argument(
        "--title", "-t",
        default="",
        help="Title prefix for the report (e.g. environment name)",
    )
    parser.add_argument(
        "--reference", "-r",
        default=None,
        help="Reference driver/series for delta charts (default: spring-data-valkey-glide). "
             "Use the series label (filename stem) for flat layout results.",
    )
    return parser.parse_args()


def _build_variant_traces(data, variant_colors, variant_dashes, variant_families,
                           variant_display_labels, hover_value_label="RPS",
                           hover_value_fmt="%{y:,.0f}"):
    """Build Plotly traces using auto-shaded variant colors from manifest-aware assignment."""
    traces = []

    # Group by family
    family_labels_map = defaultdict(list)
    for label in data:
        family = variant_families.get(label, "other")
        family_labels_map[family].append(label)

    all_families = list(dict.fromkeys(
        [f for f in FAMILY_ORDER if f in family_labels_map] +
        [f for f in family_labels_map if f not in FAMILY_ORDER]
    ))

    for family in all_families:
        family_display = FAMILY_LABELS.get(family, family.replace("-", " ").title())
        for label in sorted(family_labels_map.get(family, [])):
            points = data.get(label, [])
            if not points:
                continue
            x = [p[0] for p in points]
            y = [p[1] for p in points]
            color = variant_colors.get(label, "#999999")
            dash = variant_dashes.get(label, "solid")
            display = variant_display_labels.get(label, label)

            traces.append({
                "x": x,
                "y": y,
                "type": "scatter",
                "mode": "lines+markers",
                "name": display,
                "legendgroup": family,
                "legendgrouptitle": {"text": family_display},
                "line": {"color": color, "width": 2.5, "dash": dash},
                "marker": {"size": 8, "color": color},
                "hovertemplate": (
                    f"<b>{display}</b><br>"
                    "Clients: %{x}<br>"
                    f"{hover_value_label}: {hover_value_fmt}<br>"
                    "<extra></extra>"
                ),
            })
    return traces


def _build_variant_delta_traces(data, reference_label, variant_colors, variant_dashes,
                                 variant_families, variant_display_labels):
    """Build delta traces for flat/variant mode."""
    traces = []
    ref_points = data.get(reference_label, [])
    if not ref_points:
        return traces
    ref_rps = {cc: rps for cc, rps in ref_points}

    family_labels_map = defaultdict(list)
    for label in data:
        if label == reference_label:
            continue
        family = variant_families.get(label, "other")
        family_labels_map[family].append(label)

    all_families = list(dict.fromkeys(
        [f for f in FAMILY_ORDER if f in family_labels_map] +
        [f for f in family_labels_map if f not in FAMILY_ORDER]
    ))

    for family in all_families:
        family_display = FAMILY_LABELS.get(family, family.replace("-", " ").title())
        for label in sorted(family_labels_map.get(family, [])):
            points = data.get(label, [])
            if not points:
                continue
            x = []
            y = []
            for cc, other_rps in points:
                if cc in ref_rps and other_rps > 0:
                    delta_pct = ((ref_rps[cc] - other_rps) / other_rps) * 100.0
                    x.append(cc)
                    y.append(round(delta_pct, 2))
            if not x:
                continue
            color = variant_colors.get(label, "#999999")
            dash = variant_dashes.get(label, "solid")
            display = variant_display_labels.get(label, label)
            traces.append({
                "x": x, "y": y, "type": "scatter", "mode": "lines+markers",
                "name": display, "legendgroup": family,
                "legendgrouptitle": {"text": family_display},
                "line": {"color": color, "width": 2.5, "dash": dash},
                "marker": {"size": 8, "color": color},
                "hovertemplate": (
                    f"<b>{display}</b><br>"
                    "Clients: %{x}<br>"
                    "Delta: %{y:+.1f}%<br>"
                    "<extra></extra>"
                ),
            })
    return traces


def main():
    args = parse_args()
    results_dir = Path(args.results_dir)

    if not results_dir.is_dir():
        print(f"Error: {results_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Set reference driver
    global REFERENCE_DRIVER
    if args.reference:
        REFERENCE_DRIVER = args.reference

    # Detect layout
    layout = detect_layout(results_dir)
    print(f"Scanning: {results_dir} (layout: {layout})")

    # Load manifest (flat layout)
    manifest = None
    if layout == "flat":
        manifest = load_manifest(results_dir)
        if manifest:
            print(f"Manifest loaded: {manifest.get('description', '')}")
            # Auto-detect reference from first series if not specified
            if not args.reference and manifest.get("variants"):
                first_label = next(iter(manifest["variants"]))
                # Only auto-set if there's a single driver (config comparison mode)
                driver_names = set(v.get("driver_name", "") for v in manifest["variants"].values())
                if len(driver_names) == 1:
                    # First variant as reference for single-driver sweep
                    REFERENCE_DRIVER = first_label
                    print(f"Auto-selected reference series: {REFERENCE_DRIVER}")

    # Load data based on layout
    if layout == "flat":
        data, stats, rps_outlier_map = load_all_data_flat(results_dir)
    else:
        data, stats, rps_outlier_map = load_all_data(results_dir)

    if not data:
        print("Error: No data found", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(data)} series")
    print(f"Total runs: {stats['total_runs']}, discarded: {stats['discarded_runs']}, "
          f"kept: {stats['kept_runs']}")

    if REFERENCE_DRIVER not in data:
        print(f"Warning: reference '{REFERENCE_DRIVER}' not found in data. "
              f"Delta charts will be empty.", file=sys.stderr)

    # For flat layout with manifest: use variant-aware coloring
    use_variant_mode = (layout == "flat")

    if use_variant_mode:
        variant_colors, variant_dashes, variant_families, variant_display_labels = \
            assign_variant_colors(list(data.keys()), manifest)

        # Override DRIVER_COLORS and DRIVER_FAMILIES so existing code paths work too
        for label, color in variant_colors.items():
            DRIVER_COLORS[label] = color
        for label, family in variant_families.items():
            DRIVER_FAMILIES[label] = family

    # Extract driver versions
    if layout == "flat":
        driver_versions = extract_driver_versions_flat(results_dir)
    else:
        driver_versions = extract_driver_versions(results_dir)

    if driver_versions:
        print(f"Driver versions found for {len(driver_versions)} series:")
        for drv, info in sorted(driver_versions.items()):
            print(f"  {info['label']}")
    else:
        print("No driver version info found in NDJSON metadata.")

    # If in variant mode with manifest, override driver_versions labels with display labels
    if use_variant_mode and variant_display_labels:
        for label, display in variant_display_labels.items():
            if label not in driver_versions:
                driver_versions[label] = {"label": display}
            else:
                # Append version info to display label
                ver_info = driver_versions[label]
                if ver_info.get("primary_version"):
                    enriched = f"{display} ({ver_info['primary_version']}"
                    if ver_info.get("secondary_id") and ver_info.get("secondary_version"):
                        enriched += f" / {ver_info['secondary_id']} {ver_info['secondary_version']}"
                    enriched += ")"
                    driver_versions[label]["label"] = enriched
                else:
                    driver_versions[label]["label"] = display

    if not driver_versions:
        driver_versions = None

    # Load latency data
    if layout == "flat":
        latency_data, latency_commands = load_latency_data_flat(results_dir, rps_outlier_map)
    else:
        latency_data, latency_commands = load_latency_data(results_dir, rps_outlier_map)

    if latency_data:
        print(f"Latency data found for commands: {', '.join(latency_commands)}")
    else:
        print("No latency data found. Latency charts will be omitted.")
        latency_data = None
        latency_commands = None

    # Load CPU data
    if layout == "flat":
        cpu_data, has_cpu = load_cpu_data_flat(results_dir, rps_outlier_map)
    else:
        cpu_data, has_cpu = load_cpu_data(results_dir, rps_outlier_map)

    if has_cpu:
        print(f"CPU data found for {len(cpu_data)} series")
    else:
        print("No CPU data found (.system.ndjson files). CPU charts will be omitted.")
        cpu_data = None

    output_dir = Path(args.output)
    output_path = output_dir / "scalability_and_delta.html"

    generate_html(data, stats, args.title, output_path,
                  cpu_data=cpu_data,
                  latency_data=latency_data,
                  latency_commands=latency_commands,
                  driver_versions=driver_versions)
    print(f"\nDone. Open in browser: {output_path}")


if __name__ == "__main__":
    main()
