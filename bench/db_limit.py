#!/usr/bin/env python3
"""Is the single TimescaleDB a problem, and at what traffic does it become the limit?

What loads the database is not the event rate but the rows of `ema_window`: one per symbol with a price event and 5-min
window. The real feed has 4,970 such symbols, about 3,100-3,700 rows per close. So traffic is described by events/s AND by
rows per close (symbols with a price).

Three measurements (each also usable alone):
  realistic   the real mix, at real-time pace around the busiest minute of the week and faster: writer load, rows/s, time
              from the window's end (fired) to the row being written, and the EMA task's backpressure
  bursts      the same rows at 10x, but with N equally frequent symbols (uniform workload): the rows per close grow to N, the
              event rate stays. Shows how many symbols per close the database stores at once, and when EMA results
              wait for it.
  saturation  a stored backlog read as fast as possible, with the database (EMA_SINK=jdbc) and without it
              (EMA_SINK=discard: the job does everything except writing ema_window). If the run without the database is clearly
              faster, the database is the limit; rows/s with it is its capacity. With more symbols there are more rows per
              event, so the limit moves to fewer events/s.

`written_at` is the start of the insert transaction (now()), so a row is visible up to one batch later.

Results: bench/results/db_limit/<time>-<name>/ results.jsonl and summary.md, updated after every point.

Usage (repo root; needs ./data):
  python3 bench/db_limit.py realistic
  python3 bench/db_limit.py bursts --symbols 5000,20000,80000
  python3 bench/db_limit.py saturation          # recreates topic ticks-uniform (one day) for every --symbols value
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import backlog
import measure
import workload
from backlog import ROOT, log, sh, sql
from measure import markdown, number

SUBSCRIPTIONS = "/config/subscriptions.txt"     # the default: the database is the subject, not the fan-out
WRITER = "timescaledb_ema:_Writer"


def db_point(start_end=None):
    """What the run in `ema_window` says about the database. Cold close left out, as elsewhere."""
    out = sql("WITH w AS (SELECT * FROM ema_window WHERE window_end > (SELECT min(window_end) FROM ema_window)), "
              "c AS (SELECT window_end, count(*) AS n, extract(epoch FROM max(written_at) - min(fired_at)) * 1000 AS drain "
              "FROM w GROUP BY 1) SELECT "
              "(SELECT array_to_string(percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM emitted_at - fired_at) * 1000), ',') || ',' || max(extract(epoch FROM emitted_at - fired_at) * 1000) FROM w) || '|' || "
              "(SELECT array_to_string(percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM written_at - fired_at) * 1000), ',') || ',' || max(extract(epoch FROM written_at - fired_at) * 1000) FROM w) || '|' || "
              "(SELECT array_to_string(percentile_cont(ARRAY[0.5,0.95]) WITHIN GROUP (ORDER BY n), ',') || ',' || max(n) FROM c) || '|' || "
              "(SELECT array_to_string(percentile_cont(ARRAY[0.5,0.95]) WITHIN GROUP (ORDER BY drain), ',') || ',' || max(drain) FROM c) || '|' || "
              "(SELECT count(*) FROM c)")
    e, w, n, d, closes = out.split("|")
    f = lambda t, keys: dict(zip(keys, (round(float(x)) for x in t.split(","))))
    return {"fired_to_emitted_ms": f(e, ("p50", "p95", "p99", "max")), "fired_to_written_ms": f(w, ("p50", "p95", "p99", "max")),
            "rows_per_close": f(n, ("p50", "p95", "max")), "close_drain_ms": f(d, ("p50", "p95", "max")), "closes": int(closes),
            "rows_total": int(sql("SELECT count(*) FROM ema_window"))}


def load_of(tasks, prefix):
    for name, t in tasks.items():
        if name.startswith(prefix):
            return t
    return {}


def paced(meta, data_dir, speed, producer_env=None, settings=None, stable=20):
    p = measure.replay(meta, Path(data_dir) / "slices", speed, SUBSCRIPTIONS, 4, 4, settings or {}, stable, producer_env)
    p["db"] = db_point()
    p["rows_per_s"] = round(p["db"]["rows_total"] / p["send_s"], 1) if p["send_s"] else None
    return p


def row_of(r):
    w = load_of(r["tasks"], WRITER)
    e = next((t for n, t in r["tasks"].items() if n.startswith("ema____")), {})
    d = r["db"]
    q = lambda m, ks=("p50", "p99", "max"): " / ".join(f"{m[k]:,}" for k in ks)
    return [r["name"], number(r["events_per_s"]), number(d["rows_per_close"]["p50"]), number(r["rows_per_s"]),
            f"{w.get('busy_avg', 0) / 10:.1f}% / {w.get('busy_max', 0) / 10:.1f}%", f"{e.get('backpressured_avg', 0) / 10:.1f}%",
            number(r["cpu_pct"].get("timescaledb")), number(r["cpu_max_pct"].get("timescaledb")),
            q(d["fired_to_emitted_ms"]), q(d["fired_to_written_ms"]), q(d["close_drain_ms"], ("p50", "max"))]


def summarize_paced(results):
    rows = [row_of(r) if "error" not in r else [r["name"], "failed"] + [None] * 9 for r in results]
    return ("# Database load at paced traffic\n\nms, without the first close. Writer busy and EMA backpressured as a share of time.\n\n"
            + markdown(["Point", "Events/s", "Rows per close (p50)", "Rows/s (average over the run)", "Writer busy avg / max subtask", "EMA backpressured",
                        "DB CPU % avg", "DB CPU % max", "fired → EMA p50 / p99 / max", "fired → written p50 / p99 / max", "Close stored, p50 / max (first fired → last row written)"], rows))


def out_dir_for(name):
    d = ROOT / "bench" / "results" / "db_limit" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + (f"-{name}" if name else ""))
    d.mkdir(parents=True)
    return d


def slice_of(a, day, start, end):
    return workload.extract(a.data_dir, workload.profile(a.data_dir, [day], log)[day], day, start, end, "all", log)


def realistic(a):
    peak = slice_of(a, "10", "15:15", "15:45")      # 15:30 is the busiest minute of the week
    session = slice_of(a, "10", "09:00", "17:30")
    points = [(f"peak half hour {s:g}x", peak, s) for s in (float(x) for x in a.peak_speeds.split(","))]
    points += [(f"session {'max' if s == 0 else f'{s:g}x'}", session, s) for s in (float(x) for x in a.session_speeds.split(","))]
    measure.setup()
    out = out_dir_for(a.name or "realistic")
    failed = measure.run_points([(n, lambda m=m, s=s: paced(m, a.data_dir, s, stable=a.stable)) for n, m, s in points],
                                out, summarize_paced)
    log("\n" + (out / "summary.md").read_text())
    return failed


def bursts(a):
    meta = slice_of(a, "10", "09:00", "09:30")
    measure.setup()
    out = out_dir_for(a.name or "bursts")
    def point(n):
        # every symbol is new to the job, so price jumps would be reported for each: not wanted here
        return paced(meta, a.data_dir, a.speed, {"UNIFORM_SYMBOLS": str(n)}, {"PRICE_JUMP_REPORT": "1e18"}, a.stable)
    pts = [("real symbols", lambda: paced(meta, a.data_dir, a.speed, stable=a.stable))]
    pts += [(f"{n:,} symbols", lambda n=n: point(n)) for n in (int(x) for x in a.symbols.split(","))]
    failed = measure.run_points(pts, out, summarize_paced)
    log("\n" + (out / "summary.md").read_text())
    return failed


# --- saturation -------------------------------------------------------------------------------------------------------

def consumed_at(job_id, total, start, end):
    """Epoch second of the first scrape at which the readers had consumed `total` records (5 s resolution), or None."""
    expr = f'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total{{job_id="{job_id}"}})'
    for r in backlog.prom("query_range", query=expr, start=start, end=end, step=1):
        for t, v in r["values"]:
            if float(v) >= total:
                return float(t)
    return None


def drain(topic, sink, label, timeout=3600, stable=20):
    os.environ["KAFKA_TOPIC"] = topic
    total = backlog.topic_records(topic)
    if not total:
        raise RuntimeError(f"topic {topic} is empty")
    group = "dblimit-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    backlog.reset()
    sh("docker compose up -d --wait prometheus cadvisor")
    env = {"KAFKA_GROUP": group, "TASKMANAGERS": "4", "TASK_SLOTS": "1", "PARALLELISM": "4", "EMA_SINK": sink,
           "PRICE_JUMP_REPORT": "1e18", "SUBSCRIPTIONS_FILE": SUBSCRIPTIONS, "KAFKA_TOPIC": topic}
    t0 = backlog.start_job(env)
    job = backlog.flink_job()["jid"]
    last, changed = None, time.time()
    while True:
        time.sleep(5)
        state = sql("SELECT count(*) FROM ema_window")
        if state != last:
            last, changed = state, time.time()
        if backlog.group_lag(group, topic) == 0 and time.time() - changed >= stable:
            break
        if time.time() - t0 > timeout:
            raise RuntimeError("timeout")
    now = int(time.time())
    read_end = consumed_at(job, total, int(t0), now)
    rows = int(sql("SELECT count(*) FROM ema_window"))
    out_end = float(sql("SELECT coalesce(extract(epoch FROM max(emitted_at)), 0) FROM ema_window")) or None
    start, end = int(t0), int(max(out_end or 0, read_end or 0)) + 5
    times = backlog.task_times(job, start, end, from_start=True)
    cpu = backlog.prom_series("sum by (container_env_SERVICE) (rate(container_cpu_usage_seconds_total[15s]))", start, end)
    avg = lambda v: sum(v) / len(v) if v else None
    r = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "name": label, "topic": topic, "sink": sink,
         "records": total, "read_s": round(read_end - t0, 1) if read_end else None,
         "drain_s": round(out_end - t0, 1) if out_end else None, "rows": rows,
         "busy": {n.split("____")[0]: {k: round(avg(v)) for k, v in t.items()} for n, t in times.items()},
         "cpu_pct": {dict(k).get("container_env_SERVICE", "?"): round(avg(v) * 100) for k, v in cpu.items() if v}}
    if sink == "jdbc":
        r["rows_per_s"] = round(rows / (out_end - t0), 1)
    r["records_per_s"] = round(total / (read_end - t0)) if read_end else None
    log(f"   {label}, {sink}: read {total:,} records in {r['read_s']} s ({number(r['records_per_s'])}/s), "
        f"{rows:,} rows" + (f", {r['rows_per_s']:,} rows/s to the last row" if sink == 'jdbc' else ""))
    return r


def summarize_saturation(results):
    rows = []
    by = {}
    for r in results:
        if "error" in r:
            rows.append([r["name"], None, "failed"] + [None] * 6)
            continue
        by.setdefault(r["name"].rsplit(" ", 1)[0], {}).setdefault(r["sink"], []).append(r)
    for name, s in by.items():
        for sink, rs in s.items():
            for r in rs:
                b = r["busy"]
                w = next((t for n, t in b.items() if n.startswith("timescaledb_ema") and n.endswith("Writer")), {})
                ema = next((t for n, t in b.items() if n == "ema"), {})
                rows.append([name, sink, number(r["records"]), r["read_s"], number(r["records_per_s"]),
                             number(r["rows"]) if sink == "jdbc" else "–", number(r.get("rows_per_s")),
                             f"{w.get('busy', 0) / 10:.0f}%" if sink == "jdbc" else "–", f"{ema.get('backpressured', 0) / 10:.0f}%",
                             number(r["cpu_pct"].get("timescaledb")), number(r["cpu_pct"].get("taskmanager"))])
    return ("# Database capacity: stored backlog with and without the database\n\nRead = until the last record was consumed (Prometheus, 5 s "
            "resolution).\n\n" + markdown(["Workload", "ema_window", "Records", "Read (s)", "Records/s", "Rows", "Rows/s (to the last row)", "Writer busy",
                                           "EMA backpressured", "DB CPU %", "TaskManager CPU %"], rows))


def saturation(a):
    sh("docker compose up -d --wait kafka timescaledb prometheus cadvisor")
    sh("docker compose build kafka-setup jobmanager")
    out = out_dir_for(a.name or "saturation")
    pts = [("real week", "ticks", None)]
    pts = [p for p in pts if a.week]
    for n in [int(x) for x in a.symbols.split(",") if x]:
        pts.append((f"uniform {n:,} symbols, one day", backlog.WORKLOADS["uniform"], n))
    results, failed = [], 0
    for label, topic, n in pts:
        if n is not None:
            r = subprocess.run(["python3", "bench/backlog.py", "load", "--data-dir", a.data_dir, "--days", a.day, "--workload", "uniform",
                                "--uniform-symbols", str(n)], cwd=ROOT, capture_output=True, text=True)
            log(r.stdout.strip().splitlines()[-1] if r.stdout else r.stderr[-300:])
        for sink in ["jdbc", "discard"] * (1 if n is not None else 2):
            name = f"{label} {sink}"
            log(f"== {name}")
            try:
                p = drain(topic, sink, label)
                p["name"] = name
            except (RuntimeError, OSError, ValueError, KeyError, SystemExit) as e:
                failed += 1
                log(f"   FAILED: {e}")
                p = {"name": name, "error": str(e)[:2000]}
            results.append(p)
            with (out / "results.jsonl").open("a") as f:
                f.write(json.dumps(p) + "\n")
            (out / "summary.md").write_text(summarize_saturation(results))
    log("\n" + (out / "summary.md").read_text())
    return failed


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("realistic", "bursts", "saturation"):
        p = sub.add_parser(name)
        p.add_argument("--data-dir", default="./data")
        p.add_argument("--name", default="")
        p.add_argument("--stable", type=int, default=20)
        if name == "realistic":
            p.add_argument("--peak-speeds", default="1,10", help="10 Nov 15:15-15:45, the half hour around the busiest minute")
            p.add_argument("--session-speeds", default="100,300,0", help="10 Nov 09:00-17:30; 0 = producer maximum")
        elif name == "bursts":
            p.add_argument("--symbols", default="5000,10000,20000,40000,80000")
            p.add_argument("--speed", type=float, default=10)
        else:
            p.add_argument("--symbols", default="10000,40000", help="uniform workload, one day each; empty: none")
            p.add_argument("--day", default="08")
            p.add_argument("--no-week", dest="week", action="store_false")
    a = ap.parse_args()
    a.data_dir = str(Path(a.data_dir).resolve())
    failed = {"realistic": realistic, "bursts": bursts, "saturation": saturation}[a.cmd](a)
    if failed:
        sys.exit(f"{failed} point(s) failed")


if __name__ == "__main__":
    main()
