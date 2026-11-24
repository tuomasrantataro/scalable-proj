#!/usr/bin/env python3
"""Advisory latency: from the end of a 5-min window to a trader's system receiving the BUY/SELL advisory.

The `advisory-latency` service (producer-rs, mode `latency`) reads topic `advisories` like a trader's system and
prints one line per message with its receive time. Each advisory carries `fired_at` (the window fired: the
watermark passed its end) and `emitted_at` (the EMA function produced the result); its Kafka timestamp is the
broker's append time. All clocks are the same host clock (every container runs on one machine).

  fired → emitted     Flink: window result → EMA function
  emitted → stored    Flink Kafka sink → broker (stored in the log)
  stored → received   broker → consumer
  fired → received    total, from the end of the window to the trader's system

`fired_at` is when the job saw the window as ended. It is later than the window's end in market time by the
out-of-orderness bound (OUT_OF_ORDERNESS_S) plus however long the input takes to get past that point; that delay is
measured by window_delay.py.

  start   Starts the consumer; call it after the topic `advisories` is recreated and before the job starts.
  stop    Stops it, prints p50/p95/p99/max per stage and appends a summary to bench/results/advisory_latency.jsonl.
          Repeats (at-least-once delivery after a restart) count once, by (trader, symbol, window_end).

The first window close after the job starts is reported separately ("cold start"): the job has just started
(code not yet compiled by the JIT, connections being opened) and, with empty EMA state, every symbol's first
window gives an initial BUY, so nearly all of that close's windows are advisories at once. Mixed in, it hides the
steady state: in one run its windows took p50 541 ms (fired → emitted), the next close's 7 ms.

Usage (repo root):
  python3 bench/advisory_latency.py start
  python3 bench/advisory_latency.py stop --label my-run
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results" / "advisory_latency.jsonl"
COMPOSE = ["docker", "compose", "--profile", "latency"]
SERVICE = "advisory-latency"
STAGES = [("fired_to_emitted", "fired → emitted"), ("emitted_to_stored", "emitted → stored"),
          ("stored_to_received", "stored → received"), ("fired_to_received", "fired → received (total)")]


def compose(*args):
    r = subprocess.run(COMPOSE + list(args), cwd=ROOT, text=True, capture_output=True)
    if r.returncode != 0:
        sys.exit(f"docker compose {' '.join(args)} failed:\n{r.stdout}{r.stderr}")
    return r.stdout


def start(_=None):
    compose("rm", "-sf", SERVICE)
    compose("up", "-d", SERVICE)
    print(f"   {SERVICE} started", flush=True)


def epoch_ms(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000


def percentile(sorted_vals, q):
    if not sorted_vals:
        return None
    i = min(len(sorted_vals) - 1, max(0, round(q / 100 * len(sorted_vals) + 0.5) - 1))  # nearest rank
    return sorted_vals[i]


def summarize(lines):
    ready, seen, rows = None, set(), []
    before_ready = repeats = 0
    ts_types = set()
    for line in lines:
        if not line.startswith("{"):
            continue
        m = json.loads(line)
        if "ready_ms" in m:
            ready = m["ready_ms"]
            continue
        a = m["advisory"]
        ts_types.add(m["ts_type"])
        key = (a["trader"], a["symbol"], a["window_end"])
        if key in seen:
            repeats += 1
            continue
        seen.add(key)
        if ready is not None and m["kafka_ts"] < ready:
            before_ready += 1  # stored before the consumer was reading: not received live
            continue
        fired, emitted, stored, received = epoch_ms(a["fired_at"]), epoch_ms(a["emitted_at"]), m["kafka_ts"], m["received_ms"]
        rows.append({"window_end": a["window_end"], "fired_to_emitted": emitted - fired,
                     "emitted_to_stored": stored - emitted, "stored_to_received": received - stored,
                     "fired_to_received": received - fired})
    first_close = min((r["window_end"] for r in rows), default=None)  # ISO local times sort as strings
    steady = [r for r in rows if r["window_end"] != first_close]
    cold = [r for r in rows if r["window_end"] == first_close]
    return {"messages": len(steady), "repeats": repeats, "before_ready": before_ready,
            "ts_types": sorted(ts_types), "ms": stage_stats(steady),
            "cold_close": first_close, "cold_messages": len(cold), "cold_ms": stage_stats(cold)}


def stage_stats(rows):
    stats = {}
    for key, _ in STAGES:
        vals = sorted(r[key] for r in rows)
        stats[key] = {q: percentile(vals, n) for q, n in (("p50", 50), ("p95", 95), ("p99", 99), ("max", 100))}
    return stats


def stop(a):
    """Stops the consumer, prints and stores the summary; returns it."""
    out = compose("logs", "--no-log-prefix", "--no-color", SERVICE)
    compose("rm", "-sf", SERVICE)
    s = summarize(out.splitlines())
    print(f"== advisory latency: {s['messages']} messages received live after the first window close"
          f" ({s['repeats']} repeats, {s['before_ready']} stored before the consumer started)", flush=True)
    if s["ts_types"] and s["ts_types"] != ["LogAppendTime"]:
        print(f"   WARNING: Kafka timestamp types {s['ts_types']}: the topic needs message.timestamp.type="
              "LogAppendTime (rerun kafka-setup); emitted → stored is meaningless", flush=True)
    if s["messages"]:
        print(f"   {'ms':26} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}")
        for key, name in STAGES:
            v = s["ms"][key]
            print(f"   {name:26} " + " ".join(f"{v[q]:8.0f}" for q in ("p50", "p95", "p99", "max")), flush=True)
    if s["cold_messages"]:
        c = s["cold_ms"]["fired_to_received"]
        print(f"   cold start (first close, {s['cold_close']}): {s['cold_messages']} messages, fired → received "
              f"p50 {c['p50']:.0f} ms, max {c['max']:.0f} ms (not included above)", flush=True)
    result = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "label": a.label, **s}
    RESULTS.parent.mkdir(exist_ok=True)
    with RESULTS.open("a") as f:
        f.write(json.dumps(result) + "\n")
    print(f"   appended to {RESULTS.relative_to(ROOT)}", flush=True)
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("start", help="start the consumer (before the job starts)")
    s = sub.add_parser("stop", help="stop the consumer and report")
    s.add_argument("--label", default="", help="stored with the result, e.g. the run's consumer group")
    a = ap.parse_args()
    {"start": start, "stop": stop}[a.cmd](a)


if __name__ == "__main__":
    main()
