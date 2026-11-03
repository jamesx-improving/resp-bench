#!/usr/bin/env python3
"""Compare two perf sweeps (baseline vs candidate) — a stop-gap for #15.

Typical use: the same perf-<lang> matrix run twice on AWS, once from a branch pinned
to the previous GLIDE release and once pinned to the RC, each on its own instance.

    aws s3 sync s3://valkey-glide-resp-bench/runs/<baseline-job>/results/ /tmp/base/
    aws s3 sync s3://valkey-glide-resp-bench/runs/<rc-job>/results/       /tmp/rc/
    python scripts/compare_runs.py /tmp/base /tmp/rc --glide valkey-glide-node \\
        --baseline-name 2.5.3 --candidate-name 2.6.0-rc2 -o compare-node.html

For each series × connection count it reports median RPS and request-weighted p99
across iterations, and the candidate-vs-baseline delta.

Two instances are never identical hardware, so a raw delta mixes the GLIDE change
with instance noise. When --glide names the GLIDE series and the sweeps also contain
peer drivers (lettuce, ioredis, go-redis, redis-py ...) whose versions did NOT change,
the report also shows the *normalized* delta: change in the GLIDE/peer ratio. If the
peers moved by +5% too, the box was simply faster; the ratio isolates GLIDE.

Results directories may be the run directory itself or any parent (searched
recursively for <label>.ndjson; .system/.interval sidecars are ignored).
"""

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

SKIP_SUFFIXES = (".system.ndjson", ".interval.ndjson")
WARN_RPS_PCT = -5.0   # candidate RPS this much lower than baseline → flagged
WARN_P99_PCT = 10.0   # candidate p99 this much higher → flagged


def load(results_dir, phase):
    """{series: {connections: {"rps": [..], "p99": [..], "errors": [..]}}}"""
    out = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for path in sorted(Path(results_dir).rglob("*.ndjson")):
        if path.name.endswith(SKIP_SUFFIXES) or path.name.startswith("_"):
            continue
        series = path.stem  # the runner's label: driver-config basename [+ "@params"]
        for line in path.open():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            ph = rec.get("phase") or {}
            if ph.get("id") != phase or ph.get("status") != "COMPLETED":
                continue
            dur = (ph.get("duration_ms") or 0) / 1000.0
            req = (rec.get("totals") or {}).get("requests") or 0
            if dur <= 0 or req <= 0:
                continue
            num = den = 0
            for m in (rec.get("metrics") or {}).values():
                lat = m.get("latency") or {}
                cnt, p99 = lat.get("count") or 0, (lat.get("summary") or {}).get("p99")
                if cnt and p99 is not None:
                    num += p99 * cnt
                    den += cnt
            cell = out[series][ph.get("connections")]
            cell["rps"].append(req / dur)
            if den:
                cell["p99"].append(num / den)
            cell["errors"].append((rec.get("totals") or {}).get("errors") or 0)
    return out


def med(xs):
    return statistics.median(xs) if xs else None


def pct(a, b):
    return None if not a or b is None else (b - a) / a * 100.0


def f(v, spec, suf=""):
    return "—" if v is None else f"{v:{spec}}{suf}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("baseline_dir")
    ap.add_argument("candidate_dir")
    ap.add_argument("--glide", help="GLIDE series label (enables peer-normalized deltas)")
    ap.add_argument("--baseline-name", default="baseline")
    ap.add_argument("--candidate-name", default="candidate")
    ap.add_argument("--phase", default="STEADY")
    ap.add_argument("--output", "-o", default="compare.html")
    args = ap.parse_args()

    base, cand = load(args.baseline_dir, args.phase), load(args.candidate_dir, args.phase)
    series = sorted(set(base) & set(cand))
    if not series:
        print(f"No common series. baseline={sorted(base)} candidate={sorted(cand)}", file=sys.stderr)
        return 1

    rows, md = [], []
    hdr = ("Series", "Conns", f"RPS {args.baseline_name}", f"RPS {args.candidate_name}", "ΔRPS",
           f"p99 {args.baseline_name} (µs)", f"p99 {args.candidate_name} (µs)", "Δp99", "Errors b/c", "n b/c")
    md.append("| " + " | ".join(hdr) + " |")
    md.append("|" + "---|" * len(hdr))
    stats = {}
    for s in series:
        for c in sorted(set(base[s]) & set(cand[s]), key=lambda x: (x is None, x)):
            b, k = base[s][c], cand[s][c]
            st = dict(rb=med(b["rps"]), rc=med(k["rps"]), pb=med(b["p99"]), pc=med(k["p99"]),
                      eb=sum(b["errors"]), ec=sum(k["errors"]), nb=len(b["rps"]), nc=len(k["rps"]))
            st["drps"], st["dp99"] = pct(st["rb"], st["rc"]), pct(st["pb"], st["pc"])
            stats[(s, c)] = st
            cells = (s, c, f(st["rb"], ",.0f"), f(st["rc"], ",.0f"), f(st["drps"], "+.1f", "%"),
                     f(st["pb"], ",.0f"), f(st["pc"], ",.0f"), f(st["dp99"], "+.1f", "%"),
                     f"{st['eb']}/{st['ec']}", f"{st['nb']}/{st['nc']}")
            md.append("| " + " | ".join(str(x) for x in cells) + " |")
            bad_r = st["drps"] is not None and st["drps"] < WARN_RPS_PCT
            bad_p = st["dp99"] is not None and st["dp99"] > WARN_P99_PCT
            rows.append("<tr>" + "".join(
                f"<td class='{'bad' if (i == 4 and bad_r) or (i == 7 and bad_p) else ''}'>{x}</td>"
                for i, x in enumerate(cells)) + "</tr>")

    norm_rows, norm_md = [], []
    if args.glide and args.glide in series:
        peers = [s for s in series if s != args.glide]
        norm_md += ["", f"### {args.glide} vs peers — change in GLIDE/peer ratio "
                        f"({args.candidate_name} vs {args.baseline_name})", "",
                    "| Peer | Conns | RPS ratio b → c | ΔRPS ratio | p99 ratio b → c | Δp99 ratio |",
                    "|---|---|---|---|---|---|"]
        for p in peers:
            for (s, c), g in sorted(((k, v) for k, v in stats.items() if k[0] == args.glide),
                                    key=lambda kv: (kv[0][1] is None, kv[0][1])):
                q = stats.get((p, c))
                if not q:
                    continue
                rr = lambda a, b: a / b if a and b else None  # noqa: E731
                rb, rc = rr(g["rb"], q["rb"]), rr(g["rc"], q["rc"])
                pb, pc = rr(g["pb"], q["pb"]), rr(g["pc"], q["pc"])
                cells = (p, c, f"{f(rb, '.3f')} → {f(rc, '.3f')}", f(pct(rb, rc), "+.1f", "%"),
                         f"{f(pb, '.3f')} → {f(pc, '.3f')}", f(pct(pb, pc), "+.1f", "%"))
                norm_md.append("| " + " | ".join(str(x) for x in cells) + " |")
                norm_rows.append("<tr>" + "".join(f"<td>{x}</td>" for x in cells) + "</tr>")

    title = f"{args.candidate_name} vs {args.baseline_name} — phase {args.phase}"
    norm_html = ""
    if norm_rows:
        norm_html = (f"<h2>{args.glide} vs peers — change in GLIDE/peer ratio</h2>"
                     "<p class='sub'>Peer versions are identical in both runs, so this cancels instance-to-instance "
                     "noise. ΔRPS ratio &gt; 0 = GLIDE gained on the peer; Δp99 ratio &lt; 0 = GLIDE's tail improved "
                     "relative to the peer.</p><table><tr><th>Peer</th><th>Conns</th><th>RPS ratio b → c</th>"
                     "<th>ΔRPS ratio</th><th>p99 ratio b → c</th><th>Δp99 ratio</th></tr>" + "".join(norm_rows) +
                     "</table>")
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{title}</title><style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1a1a1a; }}
h1 {{ font-size: 20px; }} h2 {{ font-size: 15px; margin-top: 28px; }} p.sub {{ color: #666; font-size: 13px; }}
table {{ border-collapse: collapse; font-size: 13px; }} th, td {{ border: 1px solid #ddd; padding: 5px 9px; }}
th {{ background: #f5f5f5; }} td.bad {{ color: #c0392b; font-weight: 600; }}
</style></head><body><h1>{title}</h1>
<p class="sub">Median across iterations. p99 is request-weighted across commands. Flagged: RPS &lt; {WARN_RPS_PCT:g}%,
p99 &gt; +{WARN_P99_PCT:g}%. Baseline: {args.baseline_dir} — candidate: {args.candidate_dir}</p>
<table><tr>{"".join(f"<th>{h}</th>" for h in hdr)}</tr>{"".join(rows)}</table>{norm_html}</body></html>"""
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    out.with_suffix(".md").write_text(f"## {title}\n\n" + "\n".join(md + norm_md) + "\n")
    print("\n".join(md + norm_md))
    print(f"\nWrote {out} and {out.with_suffix('.md')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
