#!/usr/bin/env python3
"""Soak-test time series from <label>.interval.ndjson sidecars (docs/INTERVAL_REPORTING.md).

Companion to plot_memory.py (which charts the runner's external RSS sampler). This
reads the per-window records the engines write themselves and answers, per series:

  * does throughput hold (RPS first hour vs last hour)?
  * does the latency tail drift (p99 first hour vs last hour)?
  * is memory flat — and if not, is it the managed heap or native memory?
  * did errors appear?

Usage:
    python scripts/plot_intervals.py results/soak/<run-id>/ --output graphs/intervals.html
    python scripts/plot_intervals.py results/ --phase STEADY --skip-warmup-seconds 300

Exit code is 0 whenever a report was written (reporting tool, not a gate); 1 when
no interval data was found.
"""

import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_memory import (  # noqa: E402  (shared thresholds + slope fit)
    FLAT_THRESHOLD_MB_PER_HOUR,
    MIN_VERDICT_WINDOW_SECONDS,
    ols_slope,
)

SUFFIX = ".interval.ndjson"
MB = 1024 * 1024

# Managed-heap field per engine, in preference order. Native (GLIDE Rust core)
# memory = rss_bytes minus this.
HEAP_FIELDS = ("heap_used_bytes", "heap_alloc_bytes")

# Drift between the first and last hour beyond this is flagged.
RPS_DRIFT_WARN_PCT = 10.0
P99_DRIFT_WARN_PCT = 25.0


def load(path, phase):
    recs = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("type") != "interval":
                continue
            if phase and rec.get("phase_id") != phase:
                continue
            recs.append(rec)
    recs.sort(key=lambda r: (r.get("interval", {}).get("elapsed_s") or 0))
    return recs


def window_stats(recs, lo, hi, fn):
    vals = [fn(r) for r in recs if lo <= r["interval"]["elapsed_s"] < hi]
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None


def p99_of(rec):
    """Request-weighted p99 across commands for one window (approximation)."""
    num = den = 0
    for m in (rec.get("metrics") or {}).values():
        lat = m.get("latency") or {}
        cnt = lat.get("count") or 0
        p99 = (lat.get("summary") or {}).get("p99")
        if cnt and p99 is not None:
            num += p99 * cnt
            den += cnt
    return num / den if den else None


def pct(a, b):
    if a in (None, 0) or b is None:
        return None
    return (b - a) / a * 100.0


def slope_verdict(points, window):
    slope = ols_slope(points) * 3600 / MB if len(points) >= 2 else 0.0
    if window < MIN_VERDICT_WINDOW_SECONDS:
        return slope, "INCONCLUSIVE"
    if abs(slope) <= FLAT_THRESHOLD_MB_PER_HOUR:
        return slope, "FLAT"
    return slope, "RISING" if slope > 0 else "FALLING"


def analyze(recs, skip):
    t = lambda r: r["interval"]["elapsed_s"]  # noqa: E731
    steady = [r for r in recs if t(r) >= skip] or recs
    end = t(recs[-1])
    window = t(steady[-1]) - t(steady[0])

    mem = lambda r, k: (r.get("memory") or {}).get(k)  # noqa: E731
    heap_field = next((k for k in HEAP_FIELDS if mem(recs[-1], k) is not None), None)

    rss_pts = [(t(r), mem(r, "rss_bytes")) for r in steady if mem(r, "rss_bytes") is not None]
    rss_slope, rss_verdict = slope_verdict(rss_pts, window) if rss_pts else (None, "NO DATA")
    heap_slope = heap_verdict = None
    native_slope = native_verdict = None
    if heap_field:
        heap_pts = [(t(r), mem(r, heap_field)) for r in steady if mem(r, heap_field) is not None]
        heap_slope, heap_verdict = slope_verdict(heap_pts, window)
        nat = [(t(r), mem(r, "rss_bytes") - mem(r, heap_field)) for r in steady
               if mem(r, "rss_bytes") is not None and mem(r, heap_field) is not None]
        if nat:
            native_slope, native_verdict = slope_verdict(nat, window)

    hour = min(3600.0, max(window / 4, 1.0))
    first_lo, last_hi = t(steady[0]), end + 1
    rps_first = window_stats(steady, first_lo, first_lo + hour, lambda r: r.get("rps"))
    rps_last = window_stats(steady, end - hour, last_hi, lambda r: r.get("rps"))
    p99_first = window_stats(steady, first_lo, first_lo + hour, p99_of)
    p99_last = window_stats(steady, end - hour, last_hi, p99_of)

    return {
        "recs": recs,
        "heap_field": heap_field,
        "duration_s": end,
        "window_s": window,
        "intervals": len(recs),
        "requests": sum((r.get("totals") or {}).get("requests", 0) for r in recs),
        "errors": sum((r.get("totals") or {}).get("errors", 0) for r in recs),
        "rss_slope": rss_slope, "rss_verdict": rss_verdict,
        "heap_slope": heap_slope, "heap_verdict": heap_verdict,
        "native_slope": native_slope, "native_verdict": native_verdict,
        "rps_first": rps_first, "rps_last": rps_last, "rps_drift": pct(rps_first, rps_last),
        "p99_first": p99_first, "p99_last": p99_last, "p99_drift": pct(p99_first, p99_last),
        "window_label": f"{hour / 60:.0f} min",
    }


def fmt(v, spec, suffix=""):
    return "—" if v is None else f"{v:{spec}}{suffix}"


def cls(verdict):
    return {"FLAT": "ok", "FALLING": "ok", "RISING": "bad"}.get(verdict, "warn")


def build_html(series, title):
    def traces(fn, unit):
        out = []
        for label, d in sorted(series.items()):
            xs, ys = [], []
            for r in d["recs"]:
                y = fn(r, d)
                if y is not None:
                    xs.append(round(r["interval"]["elapsed_s"] / 3600, 4))
                    ys.append(round(y, 3))
            if xs:
                out.append({"x": xs, "y": ys, "type": "scatter", "mode": "lines", "name": label,
                            "hovertemplate": "%{x:.2f}h — %{y} " + unit + "<extra>" + label + "</extra>"})
        return out

    mem = lambda r, k: (r.get("memory") or {}).get(k)  # noqa: E731
    charts = [
        ("rps", "Throughput (req/s)", traces(lambda r, d: r.get("rps"), "req/s")),
        ("p99", "p99 latency, request-weighted across commands (µs)", traces(lambda r, d: p99_of(r), "µs")),
        ("rss", "Process RSS (MB) — engine-reported", traces(
            lambda r, d: mem(r, "rss_bytes") / MB if mem(r, "rss_bytes") is not None else None, "MB")),
        ("heap", "Managed heap (MB)", traces(
            lambda r, d: mem(r, d["heap_field"]) / MB if d["heap_field"] and mem(r, d["heap_field"]) is not None else None, "MB")),
        ("native", "RSS minus managed heap (MB) — native / GLIDE core", traces(
            lambda r, d: (mem(r, "rss_bytes") - mem(r, d["heap_field"])) / MB
            if d["heap_field"] and mem(r, "rss_bytes") is not None and mem(r, d["heap_field"]) is not None else None, "MB")),
        ("err", "Errors per interval", traces(lambda r, d: (r.get("totals") or {}).get("errors", 0), "errors")),
    ]

    rows = []
    for label, d in sorted(series.items()):
        rps_c = "bad" if d["rps_drift"] is not None and d["rps_drift"] < -RPS_DRIFT_WARN_PCT else "ok"
        p99_c = "bad" if d["p99_drift"] is not None and d["p99_drift"] > P99_DRIFT_WARN_PCT else "ok"
        rows.append(
            f"<tr><td>{label}</td>"
            f"<td>{d['duration_s'] / 3600:.2f}h / {d['intervals']}</td>"
            f"<td class='{cls(d['rss_verdict'])}'>{d['rss_verdict']} {fmt(d['rss_slope'], '+.2f', ' MB/h')}</td>"
            f"<td class='{cls(d['heap_verdict'])}'>{d['heap_verdict'] or '—'} {fmt(d['heap_slope'], '+.2f', ' MB/h')}</td>"
            f"<td class='{cls(d['native_verdict'])}'>{d['native_verdict'] or '—'} {fmt(d['native_slope'], '+.2f', ' MB/h')}</td>"
            f"<td class='{rps_c}'>{fmt(d['rps_first'], ',.0f')} → {fmt(d['rps_last'], ',.0f')} ({fmt(d['rps_drift'], '+.1f', '%')})</td>"
            f"<td class='{p99_c}'>{fmt(d['p99_first'], ',.0f')} → {fmt(d['p99_last'], ',.0f')} µs ({fmt(d['p99_drift'], '+.1f', '%')})</td>"
            f"<td class='{'bad' if d['errors'] else 'ok'}'>{d['errors']:,} / {d['requests']:,}</td></tr>")

    divs = "\n".join(f'<h2>{t}</h2><div id="{i}" class="chart"></div>' for i, t, _ in charts)
    plots = "\n".join(
        f'Plotly.newPlot("{i}", {json.dumps(tr)}, {{xaxis: {{title: "Elapsed (h)"}}, '
        f'yaxis: {{rangemode: "tozero"}}, legend: {{orientation: "h", y: -0.25}}, margin: {{t: 10}}, '
        f'hovermode: "x unified"}}, {{responsive: true}});' for i, _, tr in charts)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title}</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1a1a1a; }}
  h1 {{ font-size: 20px; margin-bottom: 4px; }} h2 {{ font-size: 15px; margin: 28px 0 4px; }}
  p.sub {{ color: #666; margin-top: 0; font-size: 13px; }}
  table {{ border-collapse: collapse; margin-top: 16px; font-size: 13px; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }} th {{ background: #f5f5f5; }}
  .ok {{ color: #0a7b34; font-weight: 600; }} .bad {{ color: #c0392b; font-weight: 600; }}
  .warn {{ color: #8a6d00; font-weight: 600; }} .chart {{ width: 100%; height: 360px; }}
</style></head><body>
<h1>{title}</h1>
<p class="sub">Per-window records written by the engines (docs/INTERVAL_REPORTING.md). Memory verdicts are OLS
slopes after the warmup cut; |slope| &le; {FLAT_THRESHOLD_MB_PER_HOUR:g} MB/h is FLAT; under
{MIN_VERDICT_WINDOW_SECONDS:.0f}s of data is INCONCLUSIVE. "Native" = RSS minus managed heap: growth there with a
flat heap points at the GLIDE Rust core (or other off-heap memory). RPS/p99 compare the median of the first vs last
window of up to an hour; RPS drop &gt; {RPS_DRIFT_WARN_PCT:g}% or p99 rise &gt; {P99_DRIFT_WARN_PCT:g}% is flagged.</p>
<table><tr><th>Series</th><th>Duration / intervals</th><th>RSS</th><th>Managed heap</th><th>Native</th>
<th>RPS first → last</th><th>p99 first → last</th><th>Errors / requests</th></tr>
{"".join(rows)}</table>
{divs}
<script>
{plots}
</script></body></html>
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results_dir", help="directory searched recursively for *.interval.ndjson")
    ap.add_argument("--output", "-o", default="graphs/intervals.html", help="HTML output path")
    ap.add_argument("--title", default="Soak time series")
    ap.add_argument("--phase", default="STEADY", help="phase id to plot ('' = all phases)")
    ap.add_argument("--skip-warmup-seconds", type=float, default=300.0,
                    help="ignore this much of each series' head for slopes/drift (default: 300)")
    args = ap.parse_args()

    files = sorted(Path(args.results_dir).rglob("*" + SUFFIX))
    if not files:
        print(f"No {SUFFIX} files under {args.results_dir}", file=sys.stderr)
        return 1
    series = {}
    for path in files:
        recs = load(path, args.phase)
        label = path.name[: -len(SUFFIX)]
        if len(recs) < 2:
            print(f"SKIP {label}: {len(recs)} interval record(s) for phase {args.phase!r}", file=sys.stderr)
            continue
        series[label] = analyze(recs, args.skip_warmup_seconds)
    if not series:
        print("No series had at least 2 interval records.", file=sys.stderr)
        return 1

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build_html(series, args.title))
    for label, d in sorted(series.items()):
        print(f"{label}: RSS {d['rss_verdict']} {fmt(d['rss_slope'], '+.2f', ' MB/h')} | "
              f"native {d['native_verdict'] or '—'} {fmt(d['native_slope'], '+.2f', ' MB/h')} | "
              f"RPS {fmt(d['rps_drift'], '+.1f', '%')} | p99 {fmt(d['p99_drift'], '+.1f', '%')} | "
              f"errors {d['errors']:,}")
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
