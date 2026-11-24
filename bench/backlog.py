#!/usr/bin/env python3
"""Load the input once into Kafka, then replay it into the Flink job as often as needed.

The producer is then out of the measurement: each run reads the stored backlog with a fresh consumer group, at
the job's own speed. Non-price rows are still sent and filtered in Flink, as in a live run.

  load   Recreates topic `ticks` and fills it with the CSV files while Flink is stopped. Kafka keeps the
         records until the topic is deleted (KAFKA_RETENTION_MS=-1), also across `docker compose down`
         (but not `down -v`, which deletes the kafka-data volume).
  reset  Stops the job and removes all results: the tables ema_window and data_issue, the topic
         `advisories` (recreated empty) and the Flink checkpoints. The loaded events (topic `ticks`) are kept.
  run    Resets (as above), starts the job with a fresh consumer group (so it reads the backlog from offset 0)
         and the given scaling, waits until all of it is read and the output is stable, and reports times, read
         rate, busy time per task, CPU per container and a digest of the results. Each run is also appended to
         bench/results/backlog_runs.jsonl. Also measures advisory latency (bench/advisory_latency.py).

Usage (repo root):
  python3 bench/backlog.py load --data-dir ./data                      # the whole week (default)
  python3 bench/backlog.py load --data-dir ./data --days 08            # one trading day
  python3 bench/backlog.py run                                         # 4 TaskManagers, parallelism 4
  python3 bench/backlog.py run --taskmanagers 8 --parallelism 8
  python3 bench/backlog.py run --taskmanagers 2 --slots 4 --parallelism 8
  python3 bench/backlog.py reset
  python3 bench/backlog.py status                                      # what is loaded, is a job running
  python3 bench/backlog.py run --set WATERMARK_ALIGNMENT_S=0          # any job setting from docker-compose.yml

Long tail: each run also records how evenly the load is spread (`skew`: records per partition, records and busy time
per subtask, each with max/mean). For comparison, the same rows with evenly spread symbols can be loaded into a topic of
their own and run the same way (its results are meaningless as market data, so they have no reference digest):
  python3 bench/backlog.py load --data-dir ./data --workload uniform
  python3 bench/backlog.py run --workload uniform
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import advisory_latency

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results" / "backlog_runs.jsonl"
TOPIC = "ticks"
WORKLOADS = {"real": TOPIC, "uniform": "ticks-uniform"}
# Symbols of the uniform workload: with 2,047, evenly spread price events fill as many (symbol, 5-min window) pairs in
# the whole week as the real ones (1,855,738 vs 1,855,645), so the EMA and database load is the same.
UNIFORM_SYMBOLS = 2047
# md5 of the ema_window export below for the whole week; the same with Iggy (1 partition), Kafka (8 partitions) and the
# pandas reference (reference/compare.py)
WEEK_DIGEST = "19413a509d53044973e3e6b76c177af1"
EMA_EXPORT = ("\\copy (SELECT symbol, window_end, close, ticks, ema38, ema100, advisory, initial FROM ema_window "
              "ORDER BY symbol, window_end) TO STDOUT CSV")


def log(*a):
    print(*a, flush=True)


def sh(cmd, env=None, check=True):
    r = subprocess.run(cmd, shell=True, cwd=ROOT, env=dict(os.environ, **(env or {})), text=True,
                       capture_output=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd} failed:\n{r.stdout}\n{r.stderr}")
    return r.stdout


def sql(q):
    return sh(f'docker compose exec -T timescaledb psql -U postgres -d trading -tA -c "{q}"').strip()


def kafka(tool, args):
    return sh(f"docker compose exec -T kafka /opt/kafka/bin/{tool} --bootstrap-server localhost:9092 {args}")


def partition_records(topic=TOPIC):
    """Records per partition; empty if the topic does not exist."""
    out = sh(f"docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 "
             f"--topic {topic}", check=False)
    parts = [line.rsplit(":", 2) for line in out.split() if line.count(":") == 2]
    return [int(n) for _, _, n in sorted(parts, key=lambda p: int(p[1])) if n.isdigit()]


def topic_records(topic=TOPIC):
    return sum(partition_records(topic))


def group_lag(group, topic=TOPIC):
    """Records of `topic` not yet committed by the group (offsets are committed at each checkpoint)."""
    out = sh(f"docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "
             f"localhost:9092 --describe --group {group}", check=False)
    lag, seen = 0, False
    for line in out.splitlines():
        f = line.split()
        if len(f) >= 6 and f[1] == topic:
            seen = True
            lag += int(f[5]) if f[5].isdigit() else 0
            if not f[3].isdigit():  # no committed offset yet for this partition
                lag += int(f[4])
    return lag if seen else None


def prom(path, **params):
    url = f"http://localhost:9090/api/v1/{path}?" + urllib.parse.urlencode(params)
    return json.load(urllib.request.urlopen(url))["data"]["result"]


def prom_series(expr, start, end, step=5):
    """{label tuple: [values]} over [start, end] (epoch seconds)."""
    out = {}
    for r in prom("query_range", query=expr, start=start, end=end, step=step):
        vals = [float(v) for _, v in r["values"] if v not in ("NaN", "+Inf", "-Inf")]
        out[tuple(sorted(r["metric"].items()))] = vals
    return out


def balance(values):
    """How evenly `values` (one per partition or subtask) are spread; max_over_mean 1 = perfectly even."""
    mean = sum(values) / len(values) if values else 0
    return {"values": [round(v) for v in values], "max_over_mean": round(max(values) / mean, 3) if mean else None}


def task_times(job_id, start, end, from_start):
    """{task: {"busy": [ms/s per subtask], "backpressured": [...]}} of one job, over its whole life so far
    (from_start) or between the first and last sample in [start, end].

    From Flink's accumulated counters, as shares of busy + backpressured + idle time. The ...TimeMsPerSecond gauges
    are not used: they are moving averages over a minute, which smears short runs, and read 1000 in the first sample
    after a job start (an idle task then averages 60-110 ms/s)."""
    raw = {}
    for kind in ("Busy", "BackPressured", "Idle"):
        series = prom_series("max by (task_name, subtask_index) "
                             f'(flink_taskmanager_job_task_accumulate{kind}TimeMs{{job_id="{job_id}"}})', start, end)
        for k, v in series.items():
            if len(v) >= (1 if from_start else 2):
                d = dict(k)
                raw.setdefault(d["task_name"], {}).setdefault(int(d["subtask_index"]), {})[kind] = \
                    max(0, v[-1] - (0 if from_start else v[0]))
    out = {}
    for task, subtasks in raw.items():
        times = [subtasks[i] for i in sorted(subtasks) if len(subtasks[i]) == 3 and sum(subtasks[i].values()) > 0]
        share = lambda kind: [1000 * t[kind] / sum(t.values()) for t in times]
        if times:
            out[task] = {"busy": share("Busy"), "backpressured": share("BackPressured")}
    return out


def subtask_skew(job_id, start, end):
    """Records per subtask of one job (balance() each), from Prometheus."""
    job = f'job_id="{job_id}"'
    reads = "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total"
    records_in = "flink_taskmanager_job_task_operator_numRecordsIn"
    counters = {"read from Kafka": f"{reads}{{{job}}}",
                "price events into window": f'{records_in}{{{job},operator_name="window_5min"}}',
                "windows into ema": f'{records_in}{{{job},operator_name="ema"}}'}
    records = {}
    for name, expr in counters.items():     # the last sample of a counter is its total
        series = prom_series(f"max by (subtask_index) ({expr})", start, end)
        totals = {int(dict(k)["subtask_index"]): max(v) for k, v in series.items() if v}
        if totals:
            records[name] = balance([totals[i] for i in sorted(totals)])
    return records


def memory(start, end):
    """Memory in MB over [start, end]: {service: {avg, max}} (working set, all containers of a service together) and
    the JVM heap used by the TaskManagers. Prometheus keeps the last value of a removed container (of the run before)
    for five minutes, so only containers cAdvisor has just seen are counted."""
    seen = "container_memory_working_set_bytes and on (name) (time() - container_last_seen < 12)"
    stats = lambda v: {"avg": round(sum(v) / len(v) / 1e6), "max": round(max(v) / 1e6)}
    series = prom_series(f"sum by (container_env_SERVICE) ({seen})", start, end)
    out = {dict(k).get("container_env_SERVICE", "?"): stats(v) for k, v in series.items() if v}
    for v in prom_series("sum(flink_taskmanager_Status_JVM_Memory_Heap_Used)", start, end).values():
        if v:
            out["taskmanager heap"] = stats(v)
    return out


def flink_job():
    jobs = json.load(urllib.request.urlopen("http://localhost:8081/jobs/overview"))["jobs"]
    running = [j for j in jobs if j["state"] == "RUNNING"]
    return running[0] if running else None


def load(a):
    topic = WORKLOADS[a.workload]
    os.environ["KAFKA_TOPIC"] = topic
    producer = {"UNIFORM_SYMBOLS": str(a.uniform_symbols if a.workload == "uniform" else 0)}
    days = a.days.split(",")
    files = [f"debs2022-gc-trading-day-{d}-11-21.csv" for d in days]
    for f in files:
        if not (Path(a.data_dir) / f).exists():
            sys.exit(f"missing {Path(a.data_dir) / f}")
    log("== stopping Flink, recreating topic", topic, f"with {a.partitions} partitions")
    sh("docker compose rm -sf jobmanager taskmanager")
    sh("docker compose up -d --wait kafka")
    sh("docker compose build kafka-setup")
    kafka("kafka-topics.sh", f"--delete --if-exists --topic {topic}")
    for _ in range(60):  # deletion is asynchronous
        if topic not in kafka("kafka-topics.sh", "--list").split():
            break
        time.sleep(1)
    out = sh("docker compose run --rm --no-deps kafka-setup", env={"KAFKA_PARTITIONS": str(a.partitions)})
    log("   " + next(l for l in out.splitlines() if f"topic {topic} with" in l))
    t0 = time.time()
    for f in files:
        out = sh("docker compose run --rm --no-deps replay-producer",
                 env={"DATA_DIR": a.data_dir, "CSV_FILE": f"/data/{f}", **producer})
        log("   " + f + ": " + next(l for l in out.splitlines() if "replay done" in l).split("] ")[1])
    n = topic_records(topic)
    secs = time.time() - t0
    log(f"== loaded {n:,} records in {secs:.0f} s ({n / secs:,.0f} msg/s incl. container starts); "
        f"disk: {sh('docker compose exec -T kafka du -sh /var/lib/kafka/data').split()[0]}")


def reset(_=None):
    """Stops the job and removes all results; the events in topic `ticks` are kept."""
    log("== reset: stopping Flink, removing results (the events in topic ticks are kept)")
    sh("docker compose rm -sf jobmanager taskmanager")
    sh("docker compose up -d --wait timescaledb kafka")
    sh("docker compose run --rm --no-deps db-setup")
    sql("TRUNCATE ema_window, data_issue")
    kafka("kafka-topics.sh", "--delete --if-exists --topic advisories")
    for _ in range(60):  # deletion is asynchronous
        if "advisories" not in kafka("kafka-topics.sh", "--list").split():
            break
        time.sleep(1)
    sh("docker compose run --rm --no-deps kafka-setup")  # recreates `advisories`; an existing `ticks` is kept
    sh("docker compose run --rm --no-deps --entrypoint sh jobmanager -c 'rm -rf /flink-checkpoints/*'")


def status(_=None):
    """Prints how many events are loaded and whether a job is running."""
    sh("docker compose up -d --wait kafka")
    for topic in WORKLOADS.values():
        parts = partition_records(topic)
        log(f"{topic:14} {sum(parts):>13,} records in {len(parts)} partitions" if parts else f"{topic:14} not loaded")
    try:
        job = flink_job()
    except OSError:
        job = None
    log(f"job            {'running: ' + job['name'] if job else 'none running'}")


def start_job(env):
    """Starts JobManager and TaskManagers with `env` and waits until every task runs; returns the RUNNING time."""
    sh("docker compose up -d --build --no-deps jobmanager taskmanager", env=env)
    job = None
    for _ in range(120):
        try:
            job = flink_job()
        except OSError:
            job = None
        if job and job["tasks"]["running"] == job["tasks"]["total"]:
            break
        time.sleep(1)
    else:
        sys.exit("job did not start")
    detail = json.load(urllib.request.urlopen(f"http://localhost:8081/jobs/{job['jid']}"))
    t_running = detail["timestamps"]["RUNNING"] / 1000
    log(f"   job {job['jid']} RUNNING at {datetime.fromtimestamp(t_running, timezone.utc):%H:%M:%S} UTC")
    return t_running


def run(a):
    topic = WORKLOADS[a.workload]
    os.environ["KAFKA_TOPIC"] = topic
    sh("docker compose up -d --wait kafka")
    partitions = partition_records(topic)
    total = sum(partitions)
    if total == 0:
        sys.exit(f"topic {topic} is empty: run `bench/backlog.py load --workload {a.workload}` first")
    if a.parallelism > a.taskmanagers * a.slots:
        sys.exit(f"parallelism {a.parallelism} needs at least that many slots "
                 f"(TaskManagers {a.taskmanagers} x slots {a.slots})")
    group = "backlog-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    settings = dict(kv.split("=", 1) for kv in a.set)
    if a.workload == "uniform":  # every window mixes instruments: without this, each would be a price jump issue
        settings = {"PRICE_JUMP_REPORT": "1e18", **settings}
    env = {"KAFKA_GROUP": group, "TASKMANAGERS": str(a.taskmanagers), "TASK_SLOTS": str(a.slots),
           "PARALLELISM": str(a.parallelism), **settings}
    log(f"== backlog of {total:,} records ({a.workload}); TaskManagers={a.taskmanagers} slots={a.slots} "
        f"parallelism={a.parallelism} group={group}" + (f" {settings}" if settings else ""))
    reset()
    advisory_latency.start()
    sh("docker compose up -d --wait prometheus cadvisor")
    t_running = start_job(env)

    # Done = the group has committed every record (offsets are committed at each checkpoint), and the output has
    # not changed for `stable` seconds (the last windows fire after the idleness timeout).
    last, last_change = None, time.time()
    while True:
        time.sleep(5)
        state = sql("SELECT count(*) || ' ' || (SELECT count(*) FROM data_issue) FROM ema_window")
        if state != last:
            last, last_change = state, time.time()
        lag = group_lag(group, topic)
        if lag == 0 and time.time() - last_change >= a.stable:
            break
        if time.time() - t_running > a.timeout:
            sys.exit(f"timeout: lag={lag}, output={state}")
    t_last = float(sql("SELECT extract(epoch FROM max(emitted_at)) FROM ema_window"))
    drain = t_last - t_running

    # Metrics while the backlog was read (Prometheus scrapes every 5 s)
    start, end = int(t_running), int(t_last) + 5
    rate = prom_series("sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate)",
                       start, end)
    rates = [v for vals in rate.values() for v in vals if v > 1000]
    cpu = prom_series("sum by (container_env_SERVICE) (rate(container_cpu_usage_seconds_total[30s]))", start, end)
    avg = lambda v: sum(v) / len(v) if v else float("nan")
    job_id = flink_job()["jid"]
    times = task_times(job_id, start, end, from_start=True)
    tasks = {name: {k: round(avg(v)) for k, v in t.items()} for name, t in times.items()}
    cpu_pct = {dict(k).get("container_env_SERVICE", "?"): round(avg(v) * 100) for k, v in cpu.items()}

    mem = memory(start, end)
    # up to now: the counters' last samples may come after the last output row
    skew = {"partitions": balance(partitions), "records": subtask_skew(job_id, start, int(time.time())),
            "busy": {name: balance(t["busy"]) for name, t in times.items()}}

    digest = hashlib.md5(subprocess.run(
        ["docker", "compose", "exec", "-T", "timescaledb", "psql", "-U", "postgres", "-d", "trading", "-c", EMA_EXPORT],
        cwd=ROOT, capture_output=True, check=True).stdout).hexdigest()
    rows, buy, sell = sql("SELECT count(*) || ' ' || count(*) FILTER (WHERE advisory = 'BUY') || ' ' || "
                          "count(*) FILTER (WHERE advisory = 'SELL') FROM ema_window").split()
    issues = dict(line.split("|") for line in sql("SELECT reason, count(*) FROM data_issue GROUP BY 1").splitlines())
    latency = advisory_latency.stop(argparse.Namespace(label=group))

    result = {
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "workload": a.workload,
        "taskmanagers": a.taskmanagers, "slots": a.slots, "parallelism": a.parallelism, "settings": settings, "group": group,
        "records": total, "drain_s": round(drain, 1), "records_per_s": round(total / drain),
        "read_rate_avg": round(avg(rates)), "read_rate_max": round(max(rates, default=0)),
        "ema_rows": int(rows), "buy": int(buy), "sell": int(sell), "issues": issues, "digest": digest,
        "tasks": tasks, "cpu_pct": cpu_pct, "mem_mb": mem, "skew": skew,
        "advisory_latency_ms": {"messages": latency["messages"], **latency["ms"]["fired_to_received"]},
    }
    RESULTS.parent.mkdir(exist_ok=True)
    with RESULTS.open("a") as f:
        f.write(json.dumps(result) + "\n")

    log(f"== drained in {drain:.1f} s (job RUNNING → last output row): {total / drain:,.0f} records/s overall")
    log(f"   read rate (all source subtasks, 5 s samples): avg {avg(rates):,.0f}/s, max {max(rates, default=0):,.0f}/s")
    log(f"   output: {int(rows):,} ema_window rows, {buy} BUY, {sell} SELL; issues {issues}")
    log(f"   digest {digest}" + ("  (= reference week)" if digest == WEEK_DIGEST else ""))
    log("   busy / backpressured ms/s (avg over subtasks):")
    for name, t in sorted(tasks.items(), key=lambda kv: -kv[1]["busy"]):
        log(f"     {t['busy']:5d} / {t['backpressured']:4d}  {name}")
    log("   CPU % of a core: " + ", ".join(f"{k} {v}" for k, v in sorted(cpu_pct.items(), key=lambda kv: -kv[1])))
    log("   memory MB, max: " + ", ".join(f"{k} {v['max']}" for k, v in sorted(mem.items(), key=lambda kv: -kv[1]["max"])))
    log("   skew, max/mean: " + ", ".join(
        f"{k} {v['max_over_mean']}" for k, v in {"Kafka partitions": skew["partitions"], **skew["records"]}.items()))
    log("   busy time, max/mean over subtasks: " + ", ".join(
        f"{k.split('____')[0]} {v['max_over_mean']}" for k, v in skew["busy"].items()
        if tasks[k]["busy"] >= 50))  # of nearly idle tasks it is noise
    log(f"   appended to {RESULTS.relative_to(ROOT)}")
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    l = sub.add_parser("load", help="recreate topic ticks and fill it (Flink stopped)")
    l.add_argument("--data-dir", default="./data")
    l.add_argument("--days", default="08,09,10,11,12,13,14", help="comma-separated days of Nov 2021")
    l.add_argument("--partitions", type=int, default=8)
    l.add_argument("--workload", choices=WORKLOADS, default="real")
    l.add_argument("--uniform-symbols", type=int, default=UNIFORM_SYMBOLS, help="symbols of the uniform workload")
    r = sub.add_parser("run", help="replay the stored backlog into the job and measure")
    r.add_argument("--workload", choices=WORKLOADS, default="real")
    r.add_argument("--taskmanagers", type=int, default=4)
    r.add_argument("--slots", type=int, default=1, help="task slots per TaskManager")
    r.add_argument("--parallelism", type=int, default=4)
    r.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                   help="job setting from docker-compose.yml, e.g. WATERMARK_ALIGNMENT_S=0 (repeatable)")
    r.add_argument("--stable", type=int, default=30, help="seconds without output change that end the run")
    r.add_argument("--timeout", type=int, default=3600)
    sub.add_parser("reset", help="stop the job and remove all results; keep the loaded events")
    sub.add_parser("status", help="print what is loaded and whether a job is running")
    a = ap.parse_args()
    {"load": load, "run": run, "reset": reset, "status": status}[a.cmd](a)


if __name__ == "__main__":
    main()
