#!/usr/bin/env python3
"""Plot process-group RSS over time from .system.ndjson files, with a leak verdict.

AD-HOC companion to generate_interactive_graphs.py, which charts only cpu_percent
and ignores the memory_rss_bytes that system_monitor.py already collects. Answers
one question: is memory flat, or does it keep climbing?

Usage:
    python scripts/plot_memory.py results/soak/<run-id>/ --output graphs/memory.html
    python scripts/plot_memory.py results/soak/<run-id>/ --skip-warmup-seconds 60

Verdict method: ordinary least squares on (elapsed_seconds, rss_bytes) over the
samples after --skip-warmup-seconds, reported as MB/hour of drift. A leak shows a
persistent positive slope; a healthy run shows a slope near zero once the runtime
has reached steady state, even though the raw series is sawtoothed by GC.

Exit code is 0 regardless of verdict — this is a reporting tool, not a gate.
"""

import argparse
import json
import sys
from pathlib import Path

# Drift below this is treated as noise rather than growth. A GC'd runtime at
# steady state still wobbles by a few MB between samples; over an hour that
# projects to a small nonzero slope even with no leak at all.
FLAT_THRESHOLD_MB_PER_HOUR = 5.0

# A slope fitted over a few minutes says nothing about an hourly trend: a JIT
# warming up for 30s reads as thousands of MB/h once extrapolated. Below this
# much post-warmup data the verdict is INCONCLUSIVE and the slope is reported
# for information only.
MIN_VERDICT_WINDOW_SECONDS = 1800.0


def load_series(path):
    """Read one .system.ndjson into a list of (epoch, rss_bytes).

    Samples with a null memory_rss_bytes are dropped: that is what the collector
    writes when it could not read the process group (run finished, or /proc
    missing with no fallback).
    """
    samples = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            rss = rec.get("memory_rss_bytes")
            epoch = rec.get("timestamp_epoch")
            if rss is None or epoch is None:
                continue
            samples.append((float(epoch), int(rss)))
    samples.sort(key=lambda s: s[0])
    return samples


def ols_slope(points):
    """Least-squares slope of y over x. Returns 0.0 when undetermined."""
    n = len(points)
    if n < 2:
        return 0.0
    mean_x = sum(p[0] for p in points) / n
    mean_y = sum(p[1] for p in points) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in points)
    den = sum((x - mean_x) ** 2 for x, _ in points)
    return num / den if den else 0.0


def analyze(samples, skip_warmup_seconds):
    """Reduce a raw series to elapsed-time points plus a leak verdict."""
    t0 = samples[0][0]
    elapsed = [(epoch - t0, rss) for epoch, rss in samples]
    steady = [(t, rss) for t, rss in elapsed if t >= skip_warmup_seconds]
    # Falling back to the full series keeps a short run from producing no verdict
    # at all; flagged via `truncated` so the caller can say so.
    truncated = len(steady) < 2
    if truncated:
        steady = elapsed

    slope_bytes_per_sec = ols_slope(steady)
    slope_mb_per_hour = slope_bytes_per_sec * 3600 / (1024 * 1024)
    rss_values = [rss for _, rss in steady]

    window_seconds = steady[-1][0] - steady[0][0]
    if window_seconds < MIN_VERDICT_WINDOW_SECONDS:
        verdict, css = "INCONCLUSIVE", "inconclusive"
    elif abs(slope_mb_per_hour) <= FLAT_THRESHOLD_MB_PER_HOUR:
        verdict, css = "FLAT", "flat"
    elif slope_mb_per_hour > 0:
        verdict, css = "RISING", "rising"
    else:
        verdict, css = "FALLING", "flat"

    return {
        "points": elapsed,
        "verdict": verdict,
        "verdict_css": css,
        "slope_mb_per_hour": slope_mb_per_hour,
        "min_mb": min(rss_values) / (1024 * 1024),
        "max_mb": max(rss_values) / (1024 * 1024),
        "first_mb": steady[0][1] / (1024 * 1024),
        "last_mb": steady[-1][1] / (1024 * 1024),
        "duration_seconds": elapsed[-1][0],
        "window_seconds": window_seconds,
        "sample_count": len(elapsed),
        "truncated": truncated,
    }


def build_html(series, title):
    """One self-contained Plotly page: RSS vs elapsed time, one trace per series."""
    traces = []
    for label, data in sorted(series.items()):
        xs = [round(t, 2) for t, _ in data["points"]]
        ys = [round(rss / (1024 * 1024), 2) for _, rss in data["points"]]
        traces.append({
            "x": xs,
            "y": ys,
            "type": "scatter",
            "mode": "lines",
            "name": f"{label} — {data['verdict']}",
            "hovertemplate": "%{x:.0f}s — %{y:.1f} MB<extra>" + label + "</extra>",
        })

    rows = []
    for label, d in sorted(series.items()):
        if d["verdict"] == "INCONCLUSIVE":
            note = (f" — fitted over {d['window_seconds']:.0f}s, needs "
                    f"{MIN_VERDICT_WINDOW_SECONDS:.0f}s for a verdict")
        elif d["truncated"]:
            note = " (run too short to skip warmup)"
        else:
            note = ""
        rows.append(
            f"<tr><td>{label}</td>"
            f"<td class='{d['verdict_css']}'>{d['verdict']}</td>"
            f"<td>{d['slope_mb_per_hour']:+.2f} MB/h{note}</td>"
            f"<td>{d['first_mb']:.1f} → {d['last_mb']:.1f} MB</td>"
            f"<td>{d['min_mb']:.1f} / {d['max_mb']:.1f} MB</td>"
            f"<td>{d['duration_seconds']:.0f}s / {d['sample_count']}</td></tr>"
        )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title}</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         margin: 24px; color: #1a1a1a; }}
  h1 {{ font-size: 20px; margin-bottom: 4px; }}
  p.sub {{ color: #666; margin-top: 0; font-size: 13px; }}
  table {{ border-collapse: collapse; margin-top: 20px; font-size: 13px; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }}
  th {{ background: #f5f5f5; }}
  .flat {{ color: #0a7b34; font-weight: 600; }}
  .rising {{ color: #c0392b; font-weight: 600; }}
  .inconclusive {{ color: #8a6d00; font-weight: 600; }}
  #chart {{ width: 100%; height: 520px; }}
</style></head>
<body>
<h1>{title}</h1>
<p class="sub">Process-group RSS over elapsed time. Verdict is the OLS slope after
the warmup cut; |slope| &le; {FLAT_THRESHOLD_MB_PER_HOUR:g} MB/h counts as flat.
Series with under {MIN_VERDICT_WINDOW_SECONDS:.0f}s of post-warmup data are
INCONCLUSIVE — an hourly projection from a few minutes is meaningless.</p>
<div id="chart"></div>
<table>
  <tr><th>Series</th><th>Verdict</th><th>Drift</th><th>First → last</th>
      <th>Min / max</th><th>Duration / samples</th></tr>
  {"".join(rows)}
</table>
<script>
Plotly.newPlot("chart", {json.dumps(traces)}, {{
  xaxis: {{ title: "Elapsed time (s)" }},
  yaxis: {{ title: "RSS (MB)", rangemode: "tozero" }},
  legend: {{ orientation: "h", y: -0.18 }},
  margin: {{ t: 20 }},
  hovermode: "x unified"
}}, {{responsive: true}});
</script>
</body></html>
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results_dir", help="directory holding *.system.ndjson files")
    ap.add_argument("--output", default="graphs/memory.html", help="HTML output path")
    ap.add_argument("--title", default="Memory usage over time")
    ap.add_argument("--skip-warmup-seconds", type=float, default=30.0,
                    help="ignore this much of the head of each series when fitting "
                         "the slope (default: 30)")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    files = sorted(results_dir.rglob("*.system.ndjson"))
    if not files:
        print(f"No .system.ndjson files under {results_dir}", file=sys.stderr)
        return 1

    series = {}
    for path in files:
        samples = load_series(path)
        label = path.name[: -len(".system.ndjson")]
        if len(samples) < 2:
            print(f"SKIP {label}: {len(samples)} usable sample(s) — "
                  f"memory_rss_bytes was null (Linux /proc missing and no "
                  f"fallback?)", file=sys.stderr)
            continue
        series[label] = analyze(samples, args.skip_warmup_seconds)

    if not series:
        print("No series had usable memory samples.", file=sys.stderr)
        return 1

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build_html(series, args.title))

    width = max(len(label) for label in series)
    for label, d in sorted(series.items()):
        print(f"{label:<{width}}  {d['verdict']:<7} "
              f"{d['slope_mb_per_hour']:+8.2f} MB/h  "
              f"{d['first_mb']:7.1f} → {d['last_mb']:7.1f} MB  "
              f"({d['sample_count']} samples over {d['duration_seconds']:.0f}s)")
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
