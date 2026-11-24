#!/usr/bin/env python3
"""Delay from a window's end in market time to the advisory at the trader, for several OUT_OF_ORDERNESS_S values.

bench/advisory_latency.py starts when the window fires, so it leaves out the wait before that: the out-of-orderness
bound, Flink's watermark interval (200 ms) and the wait for the next event of every partition. Here the feed is replayed
in real time, so a window's end has a wall-clock time:

  wall time of market time t = t + offset,  offset = median(record CreateTime - row Time)

(when pacing, the producer stamps a row when it is due, see producer-rs/src/main.rs). Reported, without the first close
(cold start):
  end → fired       per window close: how long after the window's end the job evaluated it
  end → received    per advisory: how long after the window's end the trader's system received it

Only whole-second values are measured correctly. `Time`, which paces the replay, has whole seconds, and the events carry
trading times up to 1 s later: a second's events are sent together at its start. So the event that closes a window
reaches the job at (window end + bound) rounded down to a second. A fractional bound (0.5) therefore looks up to 1 s
too fast; its rows are marked in summary.md.

Results: bench/results/window_delay/<time>/ results.jsonl and summary.md, updated after every value.

Usage (repo root; every value takes the slice's length, 13 min by default, plus about 2 min):
  python3 bench/window_delay.py --values 60,5,1
"""
import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import advisory_latency
import measure
from backlog import ROOT, kafka, log, partition_records, reset, sh, sql, start_job
from measure import markdown, number
from workload import clock_s

SUBSCRIPTIONS = "/config/subscriptions-all.txt"
SAMPLE = 200                    # records per sample for the clock offset
SAMPLE_AT = (0.0, 0.45, 0.9)    # where the samples are in each partition (several, to notice drift)


def stats(values):
    v = sorted(values)
    return {"n": len(v), **{k: advisory_latency.percentile(v, q) and round(advisory_latency.percentile(v, q))
                            for k, q in (("min", 0), ("p50", 50), ("p95", 95), ("p99", 99), ("max", 100))}}


def clock_offset(topic, day_ms):
    """(median, drift, spread) of CreateTime - row time in ms: drift = largest difference between the medians of
    SAMPLE_AT, spread = p90 - p10."""
    parts, medians, every = partition_records(topic), [], []
    for where in SAMPLE_AT:
        diffs = []
        for p, n in enumerate(parts):
            out = kafka("kafka-console-consumer.sh", f"--topic {topic} --partition {p} --offset {int(n * where)} "
                        f"--max-messages {SAMPLE} --timeout-ms 15000 --formatter-property print.timestamp=true "
                        "2>/dev/null")
            for line in out.splitlines():
                m = re.match(r"CreateTime:(\d+)\t(.*)", line)
                t = clock_s(m.group(2).split(",", 4)[3].encode()) if m else None
                if t is not None:
                    diffs.append(int(m.group(1)) - (day_ms + t * 1000))
        medians.append(median(diffs))
        every += diffs
    every.sort()
    return median(every), max(medians) - min(medians), every[int(len(every) * 0.9)] - every[int(len(every) * 0.1)]


def run_value(a, value):
    group = f"delay-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    topic = measure.TOPIC
    kafka("kafka-topics.sh", f"--delete --if-exists --topic {topic}")
    while topic in kafka("kafka-topics.sh", "--list").split():
        time.sleep(1)
    reset()
    advisory_latency.start()
    start_job({"KAFKA_GROUP": group, "SUBSCRIPTIONS_FILE": SUBSCRIPTIONS, "OUT_OF_ORDERNESS_S": value,
               "TASKMANAGERS": "4", "PARALLELISM": "4"})
    out = sh("docker compose run --rm --no-deps replay-producer", env={
        "DATA_DIR": a.data_dir, "CSV_FILE": f"/data/debs2022-gc-trading-day-{a.day}-11-21.csv", "SPEEDUP": "1",
        "RATE": "0", "REPEAT": "1", "START_TIME": a.start, "END_TIME": a.end})
    m = re.search(r"sent (\d+) messages .*first to last row sent: ([\d.]+) s", out)
    if not m:
        raise RuntimeError(f"unexpected producer output:\n{out}")
    measure.wait_processed(group, 20)
    lines = advisory_latency.compose("logs", "--no-log-prefix", "--no-color", advisory_latency.SERVICE).splitlines()
    advisory_latency.compose("rm", "-sf", advisory_latency.SERVICE)

    day_ms = datetime(2021, 11, int(a.day), tzinfo=timezone.utc).timestamp() * 1000
    offset, drift, spread = clock_offset(topic, day_ms)
    # Windows that the watermark closed while the feed ran. Later ones are closed by the idleness timeout at the end.
    sent_ms = sql("SELECT string_agg(e || ',' || f, ' ' ORDER BY e) FROM (SELECT extract(epoch FROM window_end) * 1000 "
                  "AS e, extract(epoch FROM min(fired_at)) * 1000 AS f FROM ema_window GROUP BY 1) t")
    closes = [tuple(float(x) for x in pair.split(",")) for pair in sent_ms.split()]
    feed_end = day_ms + (int(a.end[:2]) * 60 + int(a.end[3:])) * 60_000
    fired = {e: f - (e + offset) for e, f in closes[1:] if f < feed_end + offset}
    received = []
    for line in lines:
        if line.startswith("{") and '"advisory"' in line:
            msg = json.loads(line)
            end = datetime.fromisoformat(msg["advisory"]["window_end"]).replace(tzinfo=timezone.utc).timestamp() * 1000
            if end in fired:
                received.append(msg["received_ms"] - (end + offset))
    issues = dict(l.split("|") for l in sql("SELECT reason, count(*) FROM data_issue GROUP BY 1").splitlines())
    r = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "out_of_orderness_s": value, "group": group,
         "day": a.day, "start": a.start, "end": a.end, "rows": int(m.group(1)), "send_s": float(m.group(2)),
         "clock_drift_ms": round(drift, 1), "clock_spread_ms": round(spread, 1), "issues": issues,
         "end_to_fired_ms": {datetime.fromtimestamp(e / 1000, timezone.utc).strftime("%H:%M"): round(d)
                             for e, d in fired.items()},
         "end_to_received_ms": stats(received)}
    log(f"   OUT_OF_ORDERNESS_S={value}: end → fired {r['end_to_fired_ms']} ms; end → received {r['end_to_received_ms']}; "
        f"issues {issues}; clock drift {drift:.1f} ms, spread {spread:.1f} ms")
    return r


def summarize(results):
    rows = []
    for r in results:
        if "error" in r:
            rows.append([r["name"], "failed"] + [None] * 4)
            continue
        s = r["end_to_received_ms"]
        rows.append([r["out_of_orderness_s"] + ("" if float(r["out_of_orderness_s"]).is_integer() else " (*)"), " / ".join(f"{d:,}" for d in r["end_to_fired_ms"].values()),
                     s["n"], " / ".join(number(s[k]) or "–" for k in ("p50", "p95", "max")),
                     r["issues"].get("LATE", 0), f"{r['clock_drift_ms']} / {r['clock_spread_ms']}"])
    return "# Window delay at real-time pace\n\nms after the window's end in market time, without the first close.\n\n" + \
        markdown(["OUT_OF_ORDERNESS_S", "End → fired, per close", "Advisories", "End → received p50 / p95 / max",
                  "Late events", "Clock drift / spread (ms)"], rows) + \
        "\n\n(*) not valid: the replay sends a second's events together at its start, so a fractional bound looks up to " \
        "1 s too fast.\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--values", default="60,5,1")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--day", default="08")
    ap.add_argument("--start", default="09:04")
    ap.add_argument("--end", default="09:17", help="more than the largest value after the last window to measure")
    ap.add_argument("--name", default="")
    a = ap.parse_args()
    a.data_dir = str(Path(a.data_dir).resolve())
    measure.setup()
    os.environ["LATENCY_OUTPUT"] = "messages"
    out_dir = ROOT / "bench" / "results" / "window_delay" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + (f"-{a.name}" if a.name else ""))
    out_dir.mkdir(parents=True)
    failed = measure.run_points([(f"OUT_OF_ORDERNESS_S={v}", lambda v=v: run_value(a, v)) for v in a.values.split(",")],
                                out_dir, summarize)
    log("\n" + (out_dir / "summary.md").read_text())
    if failed:
        sys.exit(f"{failed} value(s) failed")


if __name__ == "__main__":
    main()
