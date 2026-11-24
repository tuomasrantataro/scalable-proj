#!/usr/bin/env python3
"""Peak test: the busiest minute of the week, overall and per exchange, replayed faster and faster.

1. Finds the busiest minute (rows by column Time) of every scope: all exchanges together, FR, NL and ETR.
   The rates per minute come from one scan of the files (about a minute per day, cached in <data-dir>/slices).
2. Cuts a slice around it (default from 15 min before to 30 min after the 5-min window of the peak). The slice of an
   exchange has only that exchange's rows (--whole-feed: the rows of all of them).
3. Replays every slice at several speed-ups into a fresh job with every symbol subscribed and records the achieved
   rate, consumer lag, busy and backpressured time, CPU, memory, window and advisory latency (bench/measure.py).

Verdict per point, from the consumer lag while rows were sent (Prometheus, every 5 s):
  keeps up          the lag never exceeded --max-lag-s seconds of input
  absorbs           it did, but the job had caught up when the last row was sent
  falls behind      still behind at the end: the rate is above what the job sustains
  producer limited  the producer missed the requested pace, so this rate was not tested
  n/a               fewer than 3 lag samples (the point is too short; use a longer slice)

The speed-up scales the whole slice, so the rate in the peak minute is the speed-up times the real peak, for a
1/speed-up of a minute. Rates are given as slice average (achieved) and peak minute / peak second (nominal).
Advisories from a symbol's first window (EMA = 0 start) inside the slice are counted separately (initial).

Results: bench/results/peak_test/<time>/ peaks.json, results.jsonl and summary.md, updated after every point. The
slices are kept in <data-dir>/slices (not in git; several hundred MB each).

Usage (repo root; the default run takes about an hour):
  python3 bench/peak_test.py                                   # all,FR,NL,ETR at 10x, 30x, 100x and maximum
  python3 bench/peak_test.py --scopes all --speeds 1,10,0      # real time takes as long as the slice (45 min)
  python3 bench/peak_test.py --find                            # only find the peaks, replay nothing
  python3 bench/peak_test.py --scopes all --before 60 --after 60 --speeds 100,300   # longer, for more lag samples
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import measure
import workload
from backlog import ROOT, log
from measure import busiest, markdown, number, stage_cell

SUBSCRIPTIONS = "/config/subscriptions-all.txt"


def slice_bounds(minute, before, after):
    """HH:MM bounds around the 5-min window that contains the minute, within the day."""
    window = workload.hhmm_s(minute) // 300 * 300
    start, end = max(0, window - before * 60), min(24 * 3600 - 60, window + after * 60)
    return workload.s_hhmm(start), workload.s_hhmm(end)


def run_point(a, peak, meta, speed):
    point = measure.replay(meta, Path(a.data_dir) / "slices", speed, SUBSCRIPTIONS, a.taskmanagers, a.parallelism,
                           a.settings, a.stable)
    point.update(scope=peak["scope"], peak=peak, verdict=measure.verdict(point, a.max_lag_s))
    lag = point["lag"]
    log(f"   {point['events_per_s']:,} events/s (peak minute {number(point['peak_minute_per_s']) or '–'}/s): "
        f"{point['verdict']}; lag max {number(lag['max'])} end {number(lag['end'])} ({lag['samples']} samples); "
        f"fired → received p50 / p99 / max {stage_cell(point['advisories'])} ms")
    return point


def summarize(results):
    peaks = {}
    rows = []
    for r in results:
        if "error" in r:
            rows.append([r["name"], "failed"] + [None] * 10)
            continue
        peaks[r["scope"]] = r["peak"]
        seconds = lambda v: "–" if v is None or not r["events_per_s"] else f"{v / r['events_per_s']:.1f}"
        w = r["window_ms"]
        rows.append([
            r["scope"], "max" if r["speed"] == 0 else f"{r['speed']:g}×", number(r["events_per_s"]),
            f"{number(r['peak_minute_per_s']) or '–'} / {number(r['peak_second_per_s']) or '–'}", r["verdict"],
            f"{seconds(r['lag']['max'])} / {seconds(r['lag']['end'])} ({r['lag']['samples']})", busiest(r),
            " / ".join(number(r["cpu_pct"].get(k)) or "–" for k in ("taskmanager", "timescaledb", "kafka", "replay-producer")),
            " / ".join(number(w.get(k)) or "–" for k in ("p50", "p99", "max")), stage_cell(r["advisories"]),
            f"{number(r['advisories'].get('steady', {}).get('messages')) or '–'} ({r['initial_advisories']:,})",
            " → ".join(f"{s[3]:,}" for s in r.get("stability", {}).get("segments", [])) or "–"])
    text = "# Peak test\n\n" + markdown(
        ["Scope", "Busiest minute", "Rows in it", "Real peak (events/s)"],
        [[p["scope"], f"{p['day']} Nov {p['minute']}", number(p["rows"]), number(p["rows"] / 60)] for p in peaks.values()])
    return text + "\nLatencies in ms, without the first window close. Lag in seconds of input at the achieved rate.\n\n" + \
        markdown(["Scope", "Speed", "Events/s (achieved)", "Peak minute / second (nominal/s)", "Verdict",
                  "Lag max / end (s; samples)", "Busiest task busy/bp (ms/s)",
                  "CPU % TaskManagers / TimescaleDB / Kafka / producer", "Window fired → EMA p50 / p99 / max",
                  "Advisory fired → received p50 / p99 / max", "Advisories (initial)",
                  "Window p95 per third of the run"], rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scopes", default="all," + ",".join(workload.EXCHANGES))
    ap.add_argument("--speeds", default="10,30,100,0", help="speed-up factors; 0 = producer maximum")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--days", default="08,09,10,11,12", help="days of Nov 2021 to search")
    ap.add_argument("--from", dest="first", default="08:00", help="search from this feed time")
    ap.add_argument("--to", dest="last", default="18:00", help="... to this one (exclusive)")
    ap.add_argument("--before", type=int, default=15, help="minutes of the slice before the peak's 5-min window")
    ap.add_argument("--after", type=int, default=30, help="minutes of the slice after that window's start")
    ap.add_argument("--whole-feed", action="store_true", help="replay all exchanges also for FR, NL and ETR")
    ap.add_argument("--taskmanagers", type=int, default=4)
    ap.add_argument("--parallelism", type=int, default=4)
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                    help="job setting from docker-compose.yml (repeatable)")
    ap.add_argument("--max-lag-s", type=float, default=2, help="consumer lag, in seconds of input, that still keeps up")
    ap.add_argument("--stable", type=int, default=20, help="seconds without output change that end a point")
    ap.add_argument("--name", default="", help="name of this test (in the directory name)")
    ap.add_argument("--find", action="store_true", help="only find and print the peaks")
    a = ap.parse_args()
    a.settings = measure.parse_settings(a.set)
    a.data_dir = str(Path(a.data_dir).resolve())
    scopes, speeds = a.scopes.split(","), [float(s) for s in a.speeds.split(",")]
    for s in scopes:
        if s not in ("all", *workload.EXCHANGES):
            sys.exit(f"--scopes {s}: expected all, {', '.join(workload.EXCHANGES)}")
    profiles = workload.profile(a.data_dir, a.days.split(","), log)
    peaks = [p for p in (workload.peak(profiles, s, a.first, a.last) for s in scopes) if p]
    for p in peaks:
        log(f"   {p['scope']:>3}: {p['day']} Nov {p['minute']}, {p['rows']:,} rows ({p['rows'] / 60:,.0f} events/s)")
    if a.find:
        return
    if not peaks:
        sys.exit("no rows in the searched range")
    points = []
    for p in peaks:
        start, end = slice_bounds(p["minute"], a.before, a.after)
        meta = workload.extract(a.data_dir, profiles[p["day"]], p["day"], start, end,
                                "all" if a.whole_feed else p["scope"], log)
        log(f"   {meta['file']}: {meta['rows']:,} rows, {meta['dropped_ahead']} ahead of the feed clock left out")
        for speed in speeds:
            points.append((f"{p['scope']} {'max' if speed == 0 else f'{speed:g}x'}",
                           lambda p=p, meta=meta, speed=speed: run_point(a, p, meta, speed)))
    out_dir = ROOT / "bench" / "results" / "peak_test" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + (f"-{a.name}" if a.name else ""))
    out_dir.mkdir(parents=True)
    (out_dir / "peaks.json").write_text(json.dumps(peaks, indent=1))
    measure.setup()
    failed = measure.run_points(points, out_dir, summarize)
    log("\n" + (out_dir / "summary.md").read_text())
    log(f"== done: {out_dir.relative_to(ROOT)}")
    if failed:
        sys.exit(f"{failed} point(s) failed")


if __name__ == "__main__":
    main()
