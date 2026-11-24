#!/usr/bin/env python3
"""Subscriber sweep: the same input with 1 to 1,000 subscribed traders (FR-4, NFR-6).

Every point replays the same slice (bench/workload.py; default 10 Nov 09:00-12:00 at 30x) into a fresh job with a
generated subscription file, and records the deliveries, advisory latency per stage, consumer lag, busy and
backpressured time, CPU and memory (bench/measure.py).

  --mode all (default)     each trader follows every symbol (worst case: every advisory goes to every trader). The
                           first window close alone sends about 3,400 messages per trader, 3.4 M with 1,000.
  --mode sample            each trader follows --symbols symbols, drawn in proportion to their price events, so
                           popular symbols have many followers

Latency percentiles are over the messages after the first close ("steady"), so their number decides what they are worth.
The default slice has 657 steady advisories in 34 closes; the old default (8 Nov 09:00-09:30 at 10x) had 245 in 4. In mode
all there are that many per trader. In mode sample a trader has only a few (about 6 of 50 symbols), so use it with at least 100
traders. A warning marks points with fewer than MIN_STEADY messages. They are not independent: all copies of one
advisory are sent together, and all steady advisories are initial BUYs (a fresh job has no warm EMAs yet, real
crossovers come only after days).

Spill: the first close sends thousands of messages per trader at once. If delivering them takes longer than the time
between closes (5 min / speed-up), the next closes wait behind it and the steady percentiles measure the burst, not the
load. A point where the first close took longer than that is marked "spill!" (and a warning printed): replay it
slower. In real time there are 5 min between closes.

Deliveries are checked: the messages a consumer received, repeats counted once, must be exactly the advisories in
`ema_window` times the traders subscribed to their symbols.

One consumer reads the whole topic. If stored → received or its CPU grows with the traders, the consumer is the
limit, not the pipeline: compare with fired → emitted and emitted → stored.

Results: bench/results/subscriber_sweep/<time>/ results.jsonl and summary.md, updated after every point. The
subscription files go to config/generated/ (mounted into the job, not in git).

Usage (repo root; data files in --data-dir; about 7 min per point at the defaults):
  python3 bench/subscriber_sweep.py                              # 1,10,100 traders, every symbol
  python3 bench/subscriber_sweep.py --mode sample --traders 100,1000        # 50 symbols each
  python3 bench/subscriber_sweep.py --speed 300 --name fast
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import measure
import workload
from backlog import ROOT, log, sql
from measure import busiest, markdown, number, stage_cell

GENERATED = ROOT / "config" / "generated"
MIN_STEADY = 100        # fewer messages after the first close: the percentiles mean little


def write_subscriptions(n, mode, price_rows, per_trader, seed):
    """Writes the file for n traders; returns (path inside the job's container, {trader: [symbols]})."""
    if mode == "all":
        subs = {f"trader-{i + 1:05}": ["*"] for i in range(n)}
        name, note = f"subscribers-{n}-all.txt", f"{n} traders, every symbol"
    else:
        subs = workload.subscriptions(price_rows, n, per_trader, seed)
        name = f"subscribers-{n}-{per_trader}.txt"
        note = f"{n} traders, {per_trader} symbols each, seed {seed}"
    GENERATED.mkdir(exist_ok=True)
    (GENERATED / name).write_text(workload.subscriptions_text(subs, note))
    return f"/config/generated/{name}", subs


def expected_deliveries(advisories, subs):
    """advisories: {symbol: advisories}. One message per advisory and subscribed trader."""
    everything = sum(1 for s in subs.values() if s == ["*"])
    followers = {}
    for symbols in subs.values():
        for s in symbols:
            followers[s] = followers.get(s, 0) + 1
    return sum(n * (everything + followers.get(s, 0)) for s, n in advisories.items())


def advisories_by_symbol():
    out = sql("SELECT symbol, count(*) FROM ema_window WHERE advisory IS NOT NULL GROUP BY 1")
    return {s: int(n) for s, n in (line.split("|") for line in out.splitlines())}


def run_point(a, meta, n):
    path, subs = write_subscriptions(n, a.mode, meta["price_rows"], a.symbols, a.seed)
    point = measure.replay(meta, Path(a.data_dir) / "slices", a.speed, path, a.taskmanagers, a.parallelism,
                           a.settings, a.stable)
    advisories = advisories_by_symbol()
    expected = expected_deliveries(advisories, subs)
    got = point["advisories"]["distinct"]
    log(f"   {n} traders: {sum(advisories.values()):,} advisories, {got:,} deliveries (expected {expected:,}), "
        f"fired → received p50 / p99 / max {stage_cell(point['advisories'])} ms")
    if got != expected:
        log("   WARNING: deliveries differ from the subscriptions")
    steady = point["advisories"].get("steady", {}).get("messages") or 0
    if steady < MIN_STEADY:
        log(f"   WARNING: only {steady} messages after the first close, so the percentiles mean little")
    cold_ms = point["advisories"].get("cold", {}).get("ms", {}).get("fired_to_received", {}).get("max") or 0
    spill = point["speed"] > 0 and cold_ms > 300_000 / point["speed"]
    if spill:
        log(f"   WARNING: the first close took {cold_ms / 1000:.1f} s to deliver, closes are {300 / point['speed']:.1f} s "
            "apart: the steady percentiles include its spill")
    return {**point, "traders": n, "mode": a.mode, "symbols_per_trader": a.symbols if a.mode == "sample" else None,
            "subscriptions": sum(map(len, subs.values())), "advisory_count": sum(advisories.values()),
            "deliveries_expected": expected, "deliveries_ok": got == expected,
            "verdict": measure.verdict(point, a.max_lag_s), "spill": spill}


def summarize(results):
    ok = [r for r in results if "error" not in r]
    text = "# Subscriber sweep\n\n"
    if ok:
        s = ok[0]["slice"]
        text += (f"{s['file']} ({s['rows']:,} rows) at {ok[0]['speed']:g}×, mode {ok[0]['mode']}, "
                 f"{ok[0]['taskmanagers']} TaskManagers, parallelism {ok[0]['parallelism']}. Latency: window fired → "
                 "received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.\n\n")
    rows = []
    for r in results:
        if "error" in r:
            rows.append([r["name"], "failed"] + [None] * 13)
            continue
        adv, steady = r["advisories"], r["advisories"].get("steady", {})
        rows.append([
            r["traders"], number(r["subscriptions"]), number(r["advisory_count"]),
            f"{adv['distinct']:,}" + ("" if r["deliveries_ok"] else f" (expected {r['deliveries_expected']:,})"),
            f"{steady.get('messages', 0):,} ({steady.get('closes', 0)})" + (" few!" if steady.get("messages", 0) < MIN_STEADY else "")
            + (" spill!" if r.get("spill") else ""),
            number(adv.get("max_per_s")), stage_cell(adv), stage_cell(adv, stage="fired_to_emitted"),
            stage_cell(adv, stage="emitted_to_stored"), stage_cell(adv, stage="stored_to_received"),
            stage_cell(adv, part="cold"), busiest(r),
            " / ".join(number(r["cpu_pct"].get(k)) or "–" for k in ("taskmanager", "kafka", "advisory-latency")),
            f"{number(r['lag']['max']) or '–'} / {number(r['lag']['end']) or '–'}",
            " → ".join(f"{s[3]:,}" for s in r.get("stability", {}).get("segments", [])) or "–"])
    return text + markdown(["Traders", "Subscriptions", "Advisories", "Deliveries", "Steady messages (closes)", "Max/s", "Total latency",
                            "fired → emitted", "emitted → stored", "stored → received", "First close (cold)",
                            "Busiest task busy/bp (ms/s)", "CPU % TaskManagers / Kafka / consumer", "Lag max / end (records)",
                            "Window p95 per third"], rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--traders", default="1,10,100")
    ap.add_argument("--mode", choices=("sample", "all"), default="all")
    ap.add_argument("--symbols", type=int, default=50, help="symbols per trader (mode sample)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--day", default="10", help="day of Nov 2021")
    ap.add_argument("--start", default="09:00", help="slice start, feed time HH:MM")
    ap.add_argument("--end", default="12:00", help="slice end (exclusive), HH:MM")
    ap.add_argument("--speed", type=float, default=30, help="replay speed-up; 0 = producer maximum")
    ap.add_argument("--taskmanagers", type=int, default=4)
    ap.add_argument("--parallelism", type=int, default=4)
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                    help="job setting from docker-compose.yml (repeatable)")
    ap.add_argument("--max-lag-s", type=float, default=2, help="consumer lag, in seconds of input, that still keeps up")
    ap.add_argument("--stable", type=int, default=20, help="seconds without output change that end a point")
    ap.add_argument("--name", default="", help="name of this sweep (in the directory name)")
    ap.add_argument("--dry-run", action="store_true", help="only print the points")
    a = ap.parse_args()
    a.settings = measure.parse_settings(a.set)
    a.data_dir = str(Path(a.data_dir).resolve())
    counts = [int(n) for n in a.traders.split(",")]
    if min(counts) < 1:
        sys.exit("--traders: at least 1")
    log(f"== traders {counts}, mode {a.mode}, day {a.day} {a.start}-{a.end} at {a.speed:g}x")
    if a.dry_run:
        return
    profiles = workload.profile(a.data_dir, [a.day], log)
    meta = workload.extract(a.data_dir, profiles[a.day], a.day, a.start, a.end, log=log)
    out_dir = ROOT / "bench" / "results" / "subscriber_sweep" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + (f"-{a.name}" if a.name else ""))
    out_dir.mkdir(parents=True)
    measure.setup()
    failed = measure.run_points([(f"{n} traders", lambda n=n: run_point(a, meta, n)) for n in counts],
                                out_dir, summarize)
    log("\n" + (out_dir / "summary.md").read_text())
    log(f"== done: {out_dir.relative_to(ROOT)}")
    if failed:
        sys.exit(f"{failed} point(s) failed")


if __name__ == "__main__":
    main()
