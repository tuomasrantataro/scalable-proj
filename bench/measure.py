"""One measured replay of a slice (bench/workload.py) into a fresh job: input rate, consumer lag, busy and
backpressured time, CPU, memory, window latency and advisory latency. Used by peak_test.py and subscriber_sweep.py.

The advisory consumer runs in summary mode (LATENCY_OUTPUT=summary): it aggregates itself, because writing and
parsing a log line per message would make it the bottleneck with many subscribers.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import advisory_latency
from backlog import flink_job, group_lag, kafka, log, memory, prom_series, reset, sh, sql, start_job, subtask_skew, task_times
from latency_sweep import advisory_count, window_latency
import stability

TOPIC = "bench-replay"
SCRAPE_S = 5            # Prometheus scrape interval (config/prometheus/prometheus.yml)
SETTLE_S = 25           # new TaskManagers are discovered (10 s) and scraped (5 s) before the replay starts
# Sum over the partitions of every reader. The Kafka client's records_lag_max is not usable: it stays at an old value.
LAG = "sum(flink_taskmanager_job_task_operator_pendingRecords)"


def setup():
    """Once per script: images, services and the settings that every helper reads from the environment."""
    os.environ["KAFKA_TOPIC"] = TOPIC                # for reset(): kafka-setup creates this topic; `ticks` is kept
    os.environ["LATENCY_OUTPUT"] = "summary"
    sh("docker compose build kafka-setup jobmanager")
    sh("docker compose up -d --wait kafka timescaledb")
    sh("docker compose up -d --wait prometheus cadvisor")


def mean(v):
    return sum(v) / len(v) if v else None


def rounded(v, digits=1):
    return None if v is None else round(v, digits)


def label(key, name):
    return dict(key).get(name, "?")


def collect_metrics(start, end):
    """Prometheus metrics between two epoch seconds. Values are None where there are no samples."""
    values = next(iter(prom_series(LAG, start, end, SCRAPE_S).values()), [])
    lag = {"samples": len(values), "max": round(max(values)) if values else None,
           "end": round(values[-1]) if values else None}
    job_id = flink_job()["jid"]
    tasks = {name: {f"{k}_{agg}": round(fn(v)) for k, v in t.items() for agg, fn in (("avg", mean), ("max", max))}
             for name, t in task_times(job_id, start, end, from_start=False).items()}
    cpu = prom_series("sum by (container_env_SERVICE) (rate(container_cpu_usage_seconds_total[15s]))", start, end, SCRAPE_S)
    return {
        "lag": lag, "tasks": tasks,
        "cpu_pct": {label(k, "container_env_SERVICE"): rounded(100 * mean(v), 0) for k, v in cpu.items() if v},
        "cpu_max_pct": {label(k, "container_env_SERVICE"): round(100 * max(v)) for k, v in cpu.items() if v},
        "mem_mb": memory(start, end), "records_per_subtask": subtask_skew(job_id, start, int(time.time())),
    }


def verdict(point, max_lag_s, min_samples=3):
    """How the job coped with the input rate, from the consumer lag while the rows were sent:
    keeps up (lag never above max_lag_s of input), absorbs (above, but caught up by the end), falls behind,
    producer limited (the producer missed the requested pace, so the rate was not tested), n/a."""
    lag, rate = point["lag"], point["events_per_s"]
    if point["speed"] > 0 and not point["producer_on_pace"]:
        return "producer limited"
    if lag["samples"] < min_samples or not rate:
        return "n/a"
    if lag["max"] / rate <= max_lag_s:
        return "keeps up"
    return "absorbs" if lag["end"] / rate <= max_lag_s else "falls behind"


def last_summary(lines):
    for line in reversed(lines):
        if line.startswith('{"summary"'):
            return json.loads(line)["summary"]
    return None


def stop_consumer(timeout=180):
    """Waits until the consumer has read every advisory, stops it and returns its summary."""
    expected, t0, summary = advisory_count(), time.time(), None
    while time.time() - t0 < timeout:
        out = advisory_latency.compose("logs", "--no-log-prefix", "--no-color", advisory_latency.SERVICE)
        summary = last_summary(out.splitlines())
        if expected == 0 or (summary and summary["read"] >= expected):
            break
        time.sleep(3)
    advisory_latency.compose("rm", "-sf", advisory_latency.SERVICE)
    if summary is None:
        summary = {"read": 0, "distinct": 0}
    if summary["read"] < expected:
        log(f"   WARNING: the consumer read {summary['read']:,} of {expected:,} advisory messages")
    if summary.get("ts_types") not in (None, [], ["LogAppendTime"]):
        log(f"   WARNING: Kafka timestamp types {summary['ts_types']}: emitted → stored is meaningless")
    return {**summary, "topic_messages": expected}


def wait_processed(group, stable, timeout=1800):
    """Until the job has committed every record and its output has not changed for `stable` seconds (the last
    windows fire after the idleness timeout, IDLENESS_S = 10 s)."""
    last, last_change, t0 = None, time.time(), time.time()
    while True:
        time.sleep(3)
        state = (sql("SELECT count(*) || ' ' || (SELECT count(*) FROM data_issue) FROM ema_window")
                 + f" {advisory_count()}")
        if state != last:
            last, last_change = state, time.time()
        if group_lag(group, TOPIC) == 0 and time.time() - last_change >= stable:
            return
        if time.time() - t0 > timeout:
            raise RuntimeError(f"timeout waiting for the job: output={state}")


def replay(slice_meta, slices_dir, speed, subscriptions_file, taskmanagers, parallelism, settings, stable,
           producer_env=None):
    """Resets the system and replays the slice at `speed` times the feed's pace (0 = producer maximum)."""
    group = f"replay-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    kafka("kafka-topics.sh", f"--delete --if-exists --topic {TOPIC}")
    for _ in range(60):  # deletion is asynchronous
        if TOPIC not in kafka("kafka-topics.sh", "--list").split():
            break
        time.sleep(1)
    reset()
    advisory_latency.start()
    start_job({"KAFKA_GROUP": group, "SUBSCRIPTIONS_FILE": subscriptions_file, "TASKMANAGERS": str(taskmanagers),
               "PARALLELISM": str(parallelism), **settings})
    time.sleep(SETTLE_S)

    out = sh("docker compose run --rm --no-deps replay-producer", env={
        "DATA_DIR": str(slices_dir), "CSV_FILE": f"/data/{slice_meta['file']}", "SPEEDUP": str(speed),
        "RATE": "0", "REPEAT": "1", "START_TIME": "", "END_TIME": "", **(producer_env or {})})
    t_end = time.time()
    m = re.search(r"sent (\d+) messages .*first to last row sent: ([\d.]+) s", out)
    if not m:
        raise RuntimeError(f"unexpected producer output:\n{out}")
    rows, send_s = int(m.group(1)), float(m.group(2))
    if rows != slice_meta["rows"]:
        raise RuntimeError(f"the producer sent {rows} rows, the slice has {slice_meta['rows']}")
    wait_processed(group, stable)
    requested_s = slice_meta["span_s"] / speed if speed > 0 else None
    point = {
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "group": group,
        "slice": {k: v for k, v in slice_meta.items() if k != "price_rows"}, "speed": speed,
        "taskmanagers": taskmanagers, "parallelism": parallelism, "settings": settings,
        "rows": rows, "send_s": send_s, "requested_send_s": rounded(requested_s, 2),
        "producer_on_pace": requested_s is None or send_s <= 1.1 * requested_s,
        "events_per_s": round(rows / send_s) if send_s > 0 else 0,
        # nominal rates in the slice's busiest minute and second at this speed (None at the producer's maximum)
        "peak_minute_per_s": round(slice_meta["peak_minute_rows"] / 60 * speed) if speed > 0 else None,
        "peak_second_per_s": round(slice_meta["peak_second_rows"] * speed) if speed > 0 else None,
        **collect_metrics(int(t_end - send_s), int(t_end)),
        "advisories": stop_consumer(), "window_ms": window_latency(), "cold_window_ms": window_latency(cold=True),
        "initial_advisories": int(sql("SELECT count(*) FROM ema_window WHERE initial AND advisory IS NOT NULL AND "
                                      "window_end > (SELECT min(window_end) FROM ema_window)")),
        "stability": stability.collect(),
    }
    return point


def run_points(points, out_dir, summarize):
    """Runs (description, function) pairs; a failing point is recorded and the others still run. Returns the number
    of failed points."""
    failed = 0
    for i, (name, fn) in enumerate(points, 1):
        log(f"== point {i}/{len(points)}: {name}")
        try:
            point = fn()
        except (RuntimeError, OSError, ValueError, KeyError, SystemExit) as e:
            failed += 1
            log(f"   FAILED: {e}")
            point = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "error": str(e)[:2000]}
        with (out_dir / "results.jsonl").open("a") as f:
            f.write(json.dumps({"name": name, **point}) + "\n")
        (out_dir / "summary.md").write_text(summarize(read_results(out_dir)))
    return failed


def read_results(out_dir):
    return [json.loads(l) for l in (out_dir / "results.jsonl").read_text().splitlines() if l.strip()]


def markdown(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    return "\n".join(lines + ["| " + " | ".join("–" if c is None else str(c) for c in r) + " |" for r in rows]) + "\n"


def number(v):
    return None if v is None else f"{v:,.0f}"


def stage_cell(summary, part="steady", stage="fired_to_received", keys=("p50", "p99", "max")):
    ms = summary.get(part, {}).get("ms", {}).get(stage, {})
    return " / ".join("–" if ms.get(k) is None else f"{ms[k]:,.0f}" for k in keys)


def busiest(point):
    """The task whose most loaded subtask was busiest: "name busy/backpressured ms/s"."""
    tasks = [(t.get("busy_max") or 0, t.get("backpressured_max") or 0, name) for name, t in point["tasks"].items()]
    if not tasks:
        return None
    busy, bp, name = max(tasks)
    return f"{name.split('____')[0][:28]} {busy:.0f}/{bp:.0f}"


def parse_settings(pairs):
    for kv in pairs:
        if "=" not in kv:
            sys.exit(f"--set {kv}: expected KEY=VALUE")
    return dict(kv.split("=", 1) for kv in pairs)
