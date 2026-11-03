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
# GLIDE series label (driver-config basename) -> binding name for report titles.
BINDINGS = {
    "valkey-glide": "Java",
    "valkey-glide-node": "Node.js",
    "valkey-glide-python": "Python",
    "valkey-glide-go": "Go",
    "valkey-glide-csharp": "C#",
    "valkey-glide-ruby": "Ruby",
    "valkey-glide-php": "PHP",
    "valkey-glide-rust": "Rust",
}
WARN_RPS_PCT = -5.0   # candidate RPS this much lower than baseline → flagged
WARN_P99_PCT = 10.0   # candidate p99 this much higher → flagged


def load(results_dir, phase):
    """({series: {connections: {"rps": [..], "p99": [..], "errors": [..]}}}, {series: driver_version})"""
    out = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    versions = {}
    for path in sorted(Path(results_dir).rglob("*.ndjson")):
        if path.name.endswith(SKIP_SUFFIXES) or path.name.startswith("_"):
            continue
        series = path.stem  # the runner's label: driver-config basename [+ "@params"]
        for line in path.open():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            ver = (rec.get("metadata") or {}).get("primary_driver_version")
            if ver:
                versions[series] = ver.split("/")[-1]  # "go-redis/v9.7.3" -> "v9.7.3"
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
    return out, versions


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
    ap.add_argument("--baseline-name", default="baseline",
                    help="GLIDE version (or name) of the baseline run, e.g. 2.5.3")
    ap.add_argument("--candidate-name", default="candidate",
                    help="GLIDE version (or name) of the candidate run, e.g. 2.6.0-rc2")
    ap.add_argument("--binding", help="binding name for the title (default: inferred from --glide, "
                                      "e.g. valkey-glide-node -> Node.js)")
    ap.add_argument("--phase", default="STEADY")
    ap.add_argument("--output", "-o", default="compare.html")
    args = ap.parse_args()

    (base, vb), (cand, vc) = load(args.baseline_dir, args.phase), load(args.candidate_dir, args.phase)
    series = sorted(set(base) & set(cand))

    # Row label: what was actually measured. Peers are pinned identically in both
    # runs, so they are the noise yardstick, not a result; say so on the row.
    B, C = args.baseline_name, args.candidate_name

    def row_label(s):
        if s == args.glide:
            return f"{s} {B} → {C}"
        b, c = vb.get(s), vc.get(s)
        if b and c and b == c:
            return f"{s} {b} (unchanged)"
        if b or c:
            return f"{s} {b or '?'} → {c or '?'} (CHANGED — not a control)"
        return s

    if args.glide and vb.get(args.glide) and vb.get(args.glide) == vc.get(args.glide):
        print(f"WARNING: both runs' metadata report {args.glide} {vb[args.glide]}; using "
              f"--baseline-name/--candidate-name ({B} / {C}) as the GLIDE versions. Check the build "
              f"logs if unsure (the Go engine hardcoded its version string before 847827f).",
              file=sys.stderr)
    if not series:
        print(f"No common series. baseline={sorted(base)} candidate={sorted(cand)}", file=sys.stderr)
        return 1

    rb_h, rc_h = f"baseline run (GLIDE {B})", f"RC run (GLIDE {C})"
    hdr = ("Driver", "Conns", f"RPS — {rb_h}", f"RPS — {rc_h}", "ΔRPS",
           f"p99 µs — {rb_h}", f"p99 µs — {rc_h}", "Δp99", "Errors b/c", "n b/c")
    split = bool(args.glide and args.glide in series)
    # main = GLIDE rows (or everything, without --glide); drift = peer rows, shown last.
    main_rows, main_md, drift_rows, drift_md = [], [], [], []
    stats = {}
    for s in series:
        for c in sorted(set(base[s]) & set(cand[s]), key=lambda x: (x is None, x)):
            b, k = base[s][c], cand[s][c]
            st = dict(rb=med(b["rps"]), rc=med(k["rps"]), pb=med(b["p99"]), pc=med(k["p99"]),
                      eb=sum(b["errors"]), ec=sum(k["errors"]), nb=len(b["rps"]), nc=len(k["rps"]))
            st["drps"], st["dp99"] = pct(st["rb"], st["rc"]), pct(st["pb"], st["pc"])
            stats[(s, c)] = st
            cells = (row_label(s), c, f(st["rb"], ",.0f"), f(st["rc"], ",.0f"), f(st["drps"], "+.1f", "%"),
                     f(st["pb"], ",.0f"), f(st["pc"], ",.0f"), f(st["dp99"], "+.1f", "%"),
                     f"{st['eb']}/{st['ec']}", f"{st['nb']}/{st['nc']}")
            is_peer = split and s != args.glide
            bad_r = not is_peer and st["drps"] is not None and st["drps"] < WARN_RPS_PCT
            bad_p = not is_peer and st["dp99"] is not None and st["dp99"] > WARN_P99_PCT
            html_row = "<tr>" + "".join(
                f"<td class='{'bad' if (i == 4 and bad_r) or (i == 7 and bad_p) else ''}'>{x}</td>"
                for i, x in enumerate(cells)) + "</tr>"
            md_row = "| " + " | ".join(str(x) for x in cells) + " |"
            (drift_rows if is_peer else main_rows).append(html_row)
            (drift_md if is_peer else main_md).append(md_row)

    md_hdr = ["| " + " | ".join(hdr) + " |", "|" + "---|" * len(hdr)]
    th = "<tr>" + "".join(f"<th>{h}</th>" for h in hdr) + "</tr>"

    norm_rows, norm_md = [], []
    if split:
        peers = [s for s in series if s != args.glide]
        norm_md += ["", f"### {args.glide} vs peers — change in GLIDE/peer ratio "
                        f"(GLIDE {B} → {C}; peers unchanged)", "",
                    "| Peer (version) | Conns | RPS ratio b → c | ΔRPS ratio | p99 ratio b → c | Δp99 ratio |",
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
                cells = (row_label(p), c, f"{f(rb, '.3f')} → {f(rc, '.3f')}", f(pct(rb, rc), "+.1f", "%"),
                         f"{f(pb, '.3f')} → {f(pc, '.3f')}", f(pct(pb, pc), "+.1f", "%"))
                norm_md.append("| " + " | ".join(str(x) for x in cells) + " |")
                norm_rows.append("<tr>" + "".join(f"<td>{x}</td>" for x in cells) + "</tr>")

    binding = args.binding or BINDINGS.get(args.glide or "", "")
    title = (f"{binding}: " if binding else "") + f"GLIDE {C} vs {B} — phase {args.phase}"
    main_title = f"{args.glide}: {B} → {C}" if split else "All drivers"
    norm_html = drift_html = ""
    if norm_rows:
        norm_html = (f"<h2>{args.glide} vs peers — change in GLIDE/peer ratio</h2>"
                     "<p class='sub'>The GLIDE change with instance noise cancelled: peers run the same version in "
                     "both runs, so dividing GLIDE by its peer within each run removes whatever the box itself did. "
                     "ΔRPS ratio &gt; 0 = GLIDE gained on the peer; Δp99 ratio &lt; 0 = GLIDE's tail improved "
                     "relative to the peer.</p><table><tr><th>Peer (version)</th><th>Conns</th><th>RPS ratio b → c</th>"
                     "<th>ΔRPS ratio</th><th>p99 ratio b → c</th><th>Δp99 ratio</th></tr>" + "".join(norm_rows) +
                     "</table>")
    if drift_rows:
        drift_html = ("<h2>Peer drift — instance-to-instance noise</h2>"
                      "<p class='sub'>Third-party drivers, pinned to the same version in both runs. Any change here "
                      "is the two instances differing, not code — read it as the error bar on the GLIDE table above "
                      "(a GLIDE delta smaller than its peer's drift at the same connection count is noise).</p>"
                      f"<table>{th}{''.join(drift_rows)}</table>")
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{title}</title><style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1a1a1a; }}
h1 {{ font-size: 20px; }} h2 {{ font-size: 15px; margin-top: 28px; }} p.sub {{ color: #666; font-size: 13px; }}
table {{ border-collapse: collapse; font-size: 13px; }} th, td {{ border: 1px solid #ddd; padding: 5px 9px; }}
th {{ background: #f5f5f5; }} td.bad {{ color: #c0392b; font-weight: 600; }}
</style></head><body><h1>{title}</h1>
<p class="sub">Two runs on separate instances that differ <b>only in the GLIDE version</b> ({B} in the baseline run,
{C} in the RC run). The raw deltas below include instance noise: the GLIDE/peer ratio table cancels it, and the peer
drift table at the bottom shows how large it was. Median across iterations; p99 request-weighted across commands.
Flagged: RPS &lt; {WARN_RPS_PCT:g}%, p99 &gt; +{WARN_P99_PCT:g}%.<br>Baseline run: {args.baseline_dir}<br>RC run:
{args.candidate_dir}</p>
<h2>{main_title}</h2>
<table>{th}{"".join(main_rows)}</table>{norm_html}{drift_html}</body></html>"""
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    md = [f"### {main_title}", ""] + md_hdr + main_md
    tail = (["", "### Peer drift — instance-to-instance noise (same peer version in both runs)", ""] + md_hdr
            + drift_md) if drift_md else []
    note = (f"Two runs on separate instances that differ only in the GLIDE version ({B} → {C}). Raw deltas include "
            f"instance noise; the GLIDE/peer ratio table cancels it and the peer drift table shows its size.\n\n")
    out.with_suffix(".md").write_text(f"## {title}\n\n" + note + "\n".join(md + norm_md + tail) + "\n")
    md = md + norm_md + tail
    print("\n".join(md))
    print(f"\nWrote {out} and {out.with_suffix('.md')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
