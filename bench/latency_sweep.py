#!/usr/bin/env python3
"""Load/latency sweep: advisory latency from the real feed rate up to the producer's maximum.

Every point replays the same slice of one trading day (default 8 Nov, 09:00-09:30, 3.9 M rows) into a fresh job,
paced by the feed's own clock (column Time) at a speed-up factor: 1 = real time (about 2,200 events/s on average
in the default slice, over 4,000/s at the open), 10 = ten times faster, ... ; 0 = as fast as the producer can.
The achieved input rate is measured, so the x-axis is events/s actually sent.

Statistics leave out the first window close after the job starts (cold JVM, every symbol's initial BUY at
once); it is recorded separately as cold_*. With warm EMAs advisories are rare (about 10-20 per close with every
symbol subscribed), so the window latency over all windows (about 3,500 per close) is the larger sample.

Per point: reset (fresh topic `sweep`, empty tables, `advisories` recreated), start the advisory consumer
(bench/advisory_latency.py), start the job subscribed to every symbol (config/subscriptions-all.txt, so every
BUY/SELL is a sample), replay the slice, wait until the job has processed it, then record
  - advisory latency, window fired -> received by the consumer (p50/p95/p99/max) and per stage
  - window latency, fired -> EMA result, over all windows (from ema_window)
Results go to bench/results/latency_sweep/<time>/: results.jsonl and latency.html (chart + table), both rewritten
after every point, so a long sweep can be watched.

The real-time point takes as long as the slice (30 min by default); the whole default sweep about an hour.

Usage (repo root; the data files are in --data-dir):
  python3 bench/latency_sweep.py                                   # speeds 1,3,10,30,100,300,max
  python3 bench/latency_sweep.py --speeds 10,30,100,300,0          # skip the slow real-time points
  python3 bench/latency_sweep.py --day 10 --start 15:25 --end 15:45  # another slice
  python3 bench/latency_sweep.py --set JDBC_BATCH_SIZE=200 --name batch200   # any job setting from docker-compose.yml
  python3 bench/latency_sweep.py --chart bench/results/latency_sweep/<time>   # only redraw the chart
"""
import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import advisory_latency
from backlog import group_lag, kafka, log, reset, sh, sql, start_job

ROOT = Path(__file__).resolve().parent.parent
TOPIC = "sweep"
SUBSCRIPTIONS = "/config/subscriptions-all.txt"


def advisory_count():
    out = kafka("kafka-get-offsets.sh", "--topic advisories")
    return sum(int(line.rsplit(":", 1)[1]) for line in out.split() if line.count(":") == 2)


def window_latency(cold=False):
    """fired -> emitted in ms, over the windows after the first window close (or, cold=True, of that close)."""
    op = "=" if cold else ">"
    out = sql("SELECT count(*) || '|' || coalesce(array_to_string(percentile_cont(ARRAY[0.5, 0.95, 0.99]) "
              "WITHIN GROUP (ORDER BY ms), ','), '') || '|' || coalesce(max(ms)::text, '') FROM "
              "(SELECT extract(epoch FROM emitted_at - fired_at) * 1000 AS ms FROM ema_window "
              f"WHERE window_end {op} (SELECT min(window_end) FROM ema_window)) t")
    n, qs, mx = out.split("|")
    if int(n) == 0:
        return {"windows": 0}
    p50, p95, p99 = (float(x) for x in qs.split(","))
    return {"windows": int(n), "p50": p50, "p95": p95, "p99": p99, "max": float(mx)}


def run_point(a, speed):
    label = "max" if speed == 0 else f"{speed:g}x"
    group = f"sweep-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    log(f"== {label}: {a.csv}, {a.start}-{a.end}, group {group}")
    kafka("kafka-topics.sh", f"--delete --if-exists --topic {TOPIC}")
    while TOPIC in kafka("kafka-topics.sh", "--list").split():
        time.sleep(1)
    reset()  # also recreates topic `sweep` (KAFKA_TOPIC) and `advisories`
    advisory_latency.start()
    start_job({"KAFKA_TOPIC": TOPIC, "KAFKA_GROUP": group, "SUBSCRIPTIONS_FILE": SUBSCRIPTIONS,
               "TASKMANAGERS": str(a.taskmanagers), "PARALLELISM": str(a.parallelism), **a.settings})

    out = sh("docker compose run --rm --no-deps replay-producer", env={
        "DATA_DIR": a.data_dir, "KAFKA_TOPIC": TOPIC, "CSV_FILE": f"/data/{a.csv}", "RATE": "0",
        "SPEEDUP": str(speed), "START_TIME": a.start, "END_TIME": a.end})
    m = re.search(r"sent (\d+) messages .*first to last row sent: ([\d.]+) s", out)
    if not m:
        sys.exit(f"unexpected producer output:\n{out}")
    rows, active = int(m.group(1)), float(m.group(2))
    rate = rows / active if active > 0 else 0
    log(f"   sent {rows:,} rows in {active:.1f} s: {rate:,.0f} events/s")

    # Done when the job has committed every record and the output has not changed for `stable` seconds (the
    # last windows fire after the idleness timeout, IDLENESS_S = 10 s).
    last, last_change, t0 = None, time.time(), time.time()
    while True:
        time.sleep(3)
        # advisories too, in case a variant of the job does not write ema_window
        state = (sql("SELECT count(*) || ' ' || (SELECT count(*) FROM data_issue) FROM ema_window") + " "
                 + str(advisory_count()))
        if state != last:
            last, last_change = state, time.time()
        if group_lag(group, TOPIC) == 0 and time.time() - last_change >= a.stable:
            break
        if time.time() - t0 > 1800:
            sys.exit(f"timeout waiting for the job: output={state}")

    adv = advisory_latency.stop(argparse.Namespace(label=f"sweep {label} {group}"))
    win, cold = window_latency(), window_latency(cold=True)
    log(f"   windows fired -> emitted after the first close: n={win['windows']:,}"
        + (f", p50 {win['p50']:.0f} ms, p99 {win['p99']:.0f} ms" if win["windows"] else "")
        + (f"; first close: n={cold['windows']:,}, p50 {cold['p50']:.0f} ms" if cold["windows"] else ""))
    return {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "speedup": speed, "label": label,
            "name": a.name, "settings": a.settings, "csv": a.csv, "start": a.start, "end": a.end, "taskmanagers": a.taskmanagers,
            "parallelism": a.parallelism, "rows": rows, "send_s": round(active, 2), "events_per_s": round(rate),
            "advisories": adv["messages"], "advisory_repeats": adv["repeats"], "advisory_ms": adv["ms"],
            "window_ms": win, "cold_advisories": adv["cold_messages"], "cold_advisory_ms": adv["cold_ms"],
            "cold_window_ms": cold}


# --- chart -----------------------------------------------------------------------------------------------------

CHART = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Advisory Latency Sweep</title>
<style>
:root {
  color-scheme: light;
  --surface: #fcfcfb; --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #7a7974;
  --grid: #e6e5e0; --axis: #b9b8b0; --border: #e6e5e0;
  --series-1: #2a78d6; --series-2: #eb6834; --series-3: #1baf7a;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --surface: #1a1a19; --text-primary: #f0efec; --text-secondary: #c3c2b7; --text-muted: #8f8e86;
    --grid: #2c2c2a; --axis: #4a4a46; --border: #2c2c2a;
    --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --surface: #1a1a19; --text-primary: #f0efec; --text-secondary: #c3c2b7; --text-muted: #8f8e86;
  --grid: #2c2c2a; --axis: #4a4a46; --border: #2c2c2a;
  --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--surface); color: var(--text-primary);
       font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 960px; margin: 0 auto; padding: 24px 16px 48px; }
h1 { font-size: 20px; font-weight: 600; margin: 0 0 4px; }
.sub { color: var(--text-secondary); margin: 0 0 20px; }
.legend { display: flex; gap: 16px; flex-wrap: wrap; color: var(--text-secondary); font-size: 13px; margin-bottom: 8px; }
.legend span { display: inline-flex; align-items: center; gap: 6px; }
.legend i { width: 14px; height: 2px; border-radius: 1px; display: inline-block; }
.chart { position: relative; }
svg { display: block; width: 100%; height: auto; overflow: visible; }
svg text { fill: var(--text-muted); font-size: 11px; font-variant-numeric: tabular-nums; }
svg .title { fill: var(--text-secondary); font-size: 12px; }
svg .speed { fill: var(--text-secondary); }
svg .direct { font-size: 12px; fill: var(--text-secondary); }
.tip { position: absolute; pointer-events: none; background: var(--surface); border: 1px solid var(--border);
       border-radius: 6px; padding: 8px 10px; font-size: 12px; box-shadow: 0 2px 8px rgba(0,0,0,.12);
       white-space: nowrap; display: none; font-variant-numeric: tabular-nums; }
.tip b { font-weight: 600; }
.tip .row { display: flex; justify-content: space-between; gap: 14px; color: var(--text-secondary); }
.tip .row span:first-child { display: inline-flex; align-items: center; gap: 6px; }
.tip i { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.notes { color: var(--text-secondary); font-size: 13px; margin: 12px 0 28px; }
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 13px; font-variant-numeric: tabular-nums; }
th, td { padding: 6px 10px; text-align: right; border-bottom: 1px solid var(--border); white-space: nowrap; }
th { color: var(--text-secondary); font-weight: 500; }
th:first-child, td:first-child { text-align: left; }
h2 { font-size: 15px; font-weight: 600; margin: 0 0 8px; }
</style></head>
<body><main>
<h1>Advisory latency vs. input rate</h1>
<p class="sub" id="sub"></p>
<div class="legend" id="legend"></div>
<div class="chart" id="chart"><div class="tip" id="tip"></div></div>
<p class="notes">Latency = window fired (the watermark passed the window's end) → advisory received by a Kafka
consumer. Both axes are logarithmic. Labels above the plot are the replay speed-up (1× = the feed's real pace).
Each point replays the same slice into a fresh job, subscribed to every symbol. The first window close after
the job starts (cold start) is left out; it is in the table.</p>
<h2>All measurements (ms)</h2>
<div class="table-wrap"><table id="table"></table></div>
</main>
<script>
const DATA = __DATA__;
const SERIES = [["p50", "p50", "var(--series-1)"], ["p95", "p95", "var(--series-2)"], ["p99", "p99", "var(--series-3)"]];
const pts = DATA.points.filter(p => p.advisories > 0 && p.events_per_s > 0)
                       .sort((a, b) => a.events_per_s - b.events_per_s);
const fmtRate = v => v >= 1e6 ? (v / 1e6).toFixed(v >= 1e7 ? 0 : 1) + "M" : v >= 1e3 ? (v / 1e3).toFixed(v >= 1e4 ? 0 : 1) + "k" : String(Math.round(v));
const speed = p => p.label === "max" ? "max" : p.label.replace(/x$/, "×");
const fmtMs = v => v == null ? "–" : v >= 1000 ? (v / 1000).toFixed(2) + " s" : Math.round(v) + " ms";
const p0 = DATA.points[0];
document.getElementById("sub").textContent = p0 ?
  `${p0.name ? p0.name + " · " : ""}${Object.entries(p0.settings || {}).map(([k, v]) => k + "=" + v).join(" ")}${Object.keys(p0.settings || {}).length ? " · " : ""}${p0.csv}, ${p0.start}–${p0.end} · ${p0.taskmanagers} TaskManagers, parallelism ${p0.parallelism} · ${DATA.points.length} points` : "no points yet";
document.getElementById("legend").innerHTML = SERIES.map(([, n, c]) => `<span><i style="background:${c}"></i>${n}</span>`).join("");

const W = 900, H = 420, M = {l: 56, r: 48, t: 34, b: 44};
const svgNS = "http://www.w3.org/2000/svg";
const el = (tag, attrs, parent, text) => { const e = document.createElementNS(svgNS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]); if (text != null) e.textContent = text; parent.appendChild(e); return e; };
const svg = el("svg", {viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Advisory latency percentiles against input rate"}, document.getElementById("chart"));
if (pts.length) {
  const xs = pts.map(p => p.events_per_s);
  const ys = pts.flatMap(p => SERIES.map(([k]) => p.advisory_ms.fired_to_received[k])).filter(v => v > 0);
  const lx0 = Math.floor(Math.log10(Math.min(...xs))), lx1 = Math.ceil(Math.log10(Math.max(...xs)));
  const ly0 = Math.floor(Math.log10(Math.max(0.5, Math.min(...ys)))), ly1 = Math.ceil(Math.log10(Math.max(...ys)));
  const X = v => M.l + (Math.log10(v) - lx0) / Math.max(1e-9, lx1 - lx0) * (W - M.l - M.r);
  const Y = v => H - M.b - (Math.log10(Math.max(v, Math.pow(10, ly0))) - ly0) / Math.max(1e-9, ly1 - ly0) * (H - M.t - M.b);
  for (let d = ly0; d <= ly1; d++) {
    const y = Y(Math.pow(10, d));
    el("line", {x1: M.l, x2: W - M.r, y1: y, y2: y, stroke: "var(--grid)", "stroke-width": 1}, svg);
    el("text", {x: M.l - 8, y: y + 4, "text-anchor": "end"}, svg, fmtMs(Math.pow(10, d)).replace(" ", " "));
  }
  for (let d = lx0; d <= lx1; d++) for (const m of (lx1 - lx0 <= 3 ? [1, 2, 5] : [1])) {
    const v = m * Math.pow(10, d);
    if (v > Math.pow(10, lx1)) continue;
    const x = X(v);
    el("line", {x1: x, x2: x, y1: H - M.b, y2: H - M.b + 4, stroke: "var(--axis)"}, svg);
    el("text", {x, y: H - M.b + 18, "text-anchor": "middle"}, svg, fmtRate(v) + "/s");
  }
  el("line", {x1: M.l, x2: W - M.r, y1: H - M.b, y2: H - M.b, stroke: "var(--axis)"}, svg);
  el("text", {x: (M.l + W - M.r) / 2, y: H - 6, "text-anchor": "middle", class: "title"}, svg, "input rate (events/s, achieved)");
  for (const p of pts) el("text", {x: X(p.events_per_s), y: M.t - 14, "text-anchor": "middle", class: "speed"}, svg, speed(p));
  const ends = [];
  const cross = el("line", {y1: M.t - 6, y2: H - M.b, stroke: "var(--axis)", "stroke-dasharray": "3 3", visibility: "hidden"}, svg);
  for (const [k, name, c] of SERIES) {
    const sp = pts.filter(p => p.advisory_ms.fired_to_received[k] != null);
    el("path", {d: sp.map((p, i) => (i ? "L" : "M") + X(p.events_per_s).toFixed(1) + "," + Y(p.advisory_ms.fired_to_received[k]).toFixed(1)).join(""),
                fill: "none", stroke: c, "stroke-width": 2, "stroke-linejoin": "round", "stroke-linecap": "round"}, svg);
    for (const p of sp) el("circle", {cx: X(p.events_per_s), cy: Y(p.advisory_ms.fired_to_received[k]), r: 4,
                                      fill: c, stroke: "var(--surface)", "stroke-width": 2}, svg);
    const last = sp[sp.length - 1];
    if (last) ends.push({name, x: X(last.events_per_s) + 8, y: Y(last.advisory_ms.fired_to_received[k]) + 4});
  }
  // direct labels at the line ends, pushed apart so they never overlap (top to bottom, min 14 px)
  ends.sort((a, b) => a.y - b.y);
  for (let i = 1; i < ends.length; i++) ends[i].y = Math.max(ends[i].y, ends[i - 1].y + 14);
  for (const e of ends) el("text", {x: e.x, y: e.y, class: "direct"}, svg, e.name);
  // hover: nearest point by x, crosshair + tooltip with all percentiles
  const tip = document.getElementById("tip"), box = document.getElementById("chart");
  const hit = el("rect", {x: M.l, y: M.t - 20, width: W - M.l - M.r, height: H - M.t - M.b + 20, fill: "transparent"}, svg);
  hit.addEventListener("mousemove", ev => {
    const r = svg.getBoundingClientRect(), mx = (ev.clientX - r.left) / r.width * W;
    let best = pts[0]; for (const p of pts) if (Math.abs(X(p.events_per_s) - mx) < Math.abs(X(best.events_per_s) - mx)) best = p;
    const x = X(best.events_per_s); cross.setAttribute("x1", x); cross.setAttribute("x2", x); cross.setAttribute("visibility", "visible");
    const t = best.advisory_ms.fired_to_received;
    tip.innerHTML = `<b>${speed(best)} · ${Math.round(best.events_per_s).toLocaleString()} events/s</b>` +
      SERIES.map(([k, n, c]) => `<div class="row"><span><i style="background:${c}"></i>${n}</span><span>${fmtMs(t[k])}</span></div>`).join("") +
      `<div class="row"><span>max</span><span>${fmtMs(t.max)}</span></div><div class="row"><span>advisories</span><span>${best.advisories.toLocaleString()}</span></div>`;
    tip.style.display = "block";
    const px = x / W * r.width, tw = tip.offsetWidth;
    tip.style.left = (px + 14 + tw > r.width ? px - tw - 14 : px + 14) + "px";
    tip.style.top = "40px";
  });
  hit.addEventListener("mouseleave", () => { tip.style.display = "none"; cross.setAttribute("visibility", "hidden"); });
}
const cols = [["speed", p => speed(p)], ["events/s", p => Math.round(p.events_per_s).toLocaleString()],
  ["advisories", p => p.advisories.toLocaleString()],
  ["total p50", p => fmtMs(p.advisory_ms.fired_to_received.p50)], ["total p95", p => fmtMs(p.advisory_ms.fired_to_received.p95)],
  ["total p99", p => fmtMs(p.advisory_ms.fired_to_received.p99)], ["total max", p => fmtMs(p.advisory_ms.fired_to_received.max)],
  ["fired→emitted p50", p => fmtMs(p.advisory_ms.fired_to_emitted.p50)], ["emitted→stored p50", p => fmtMs(p.advisory_ms.emitted_to_stored.p50)],
  ["stored→received p50", p => fmtMs(p.advisory_ms.stored_to_received.p50)],
  ["cold start p50", p => p.cold_advisory_ms ? fmtMs(p.cold_advisory_ms.fired_to_received.p50) : "–"],
  ["windows", p => p.window_ms.windows ? p.window_ms.windows.toLocaleString() : "–"], ["window p50", p => fmtMs(p.window_ms.p50)], ["window p99", p => fmtMs(p.window_ms.p99)]];
const rows = [...DATA.points].sort((a, b) => a.events_per_s - b.events_per_s);
document.getElementById("table").innerHTML = "<thead><tr>" + cols.map(([h]) => `<th>${h}</th>`).join("") + "</tr></thead><tbody>" +
  rows.map(p => "<tr>" + cols.map(([, f]) => `<td>${f(p)}</td>`).join("") + "</tr>").join("") + "</tbody>";
</script>
</body></html>
"""


def write_chart(out_dir):
    points = [json.loads(l) for l in (out_dir / "results.jsonl").read_text().splitlines() if l.strip()]
    html = CHART.replace("__DATA__", json.dumps({"points": points}).replace("</", "<\\/"))
    (out_dir / "latency.html").write_text(html)
    return out_dir / "latency.html"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--speeds", default="1,3,10,30,100,300,0", help="speed-up factors; 0 = producer maximum")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--day", default="08", help="day of Nov 2021")
    ap.add_argument("--start", default="09:00", help="slice start, feed time HH:MM")
    ap.add_argument("--end", default="09:30", help="slice end (exclusive), HH:MM")
    ap.add_argument("--taskmanagers", type=int, default=4)
    ap.add_argument("--parallelism", type=int, default=4)
    ap.add_argument("--stable", type=int, default=15, help="seconds without output change that end a point")
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                    help="job setting from docker-compose.yml, e.g. JDBC_BATCH_SIZE=200 (repeatable)")
    ap.add_argument("--name", default="", help="name of this sweep (stored with the results, in the directory name)")
    ap.add_argument("--chart", metavar="DIR", help="only redraw latency.html from DIR/results.jsonl")
    a = ap.parse_args()
    if a.chart:
        log(f"wrote {write_chart(Path(a.chart))}")
        return
    a.csv = f"debs2022-gc-trading-day-{a.day}-11-21.csv"
    if not (Path(a.data_dir) / a.csv).is_file():
        sys.exit(f"missing {Path(a.data_dir) / a.csv}")
    speeds = [float(s) for s in a.speeds.split(",")]
    a.settings = dict(kv.split("=", 1) for kv in a.set)

    os.environ["KAFKA_TOPIC"] = TOPIC  # for reset(): kafka-setup creates this topic
    out_dir = ROOT / "bench" / "results" / "latency_sweep" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                                                              + (f"-{a.name}" if a.name else ""))
    out_dir.mkdir(parents=True)
    sh("docker compose build kafka-setup jobmanager")
    sh("docker compose up -d --wait kafka timescaledb")
    log(f"== sweep over speeds {a.speeds} ({a.csv} {a.start}-{a.end}); results in {out_dir.relative_to(ROOT)}")
    for speed in speeds:
        point = run_point(a, speed)
        with (out_dir / "results.jsonl").open("a") as f:
            f.write(json.dumps(point) + "\n")
        write_chart(out_dir)
    log(f"== done: {(out_dir / 'latency.html').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
