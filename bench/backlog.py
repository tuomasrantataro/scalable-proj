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
         bench/results/backlog_runs.jsonl.

Usage (repo root):
  python3 bench/backlog.py load --data-dir ./data                      # the whole week (default)
  python3 bench/backlog.py load --data-dir ./data --days 08            # one trading day
  python3 bench/backlog.py run                                         # 4 TaskManagers, parallelism 4
  python3 bench/backlog.py run --taskmanagers 8 --parallelism 8
  python3 bench/backlog.py run --taskmanagers 2 --slots 4 --parallelism 8
  python3 bench/backlog.py reset
  python3 bench/backlog.py run --set WATERMARK_ALIGNMENT_S=0          # any job setting from docker-compose.yml
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

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results" / "backlog_runs.jsonl"
TOPIC = "ticks"
# md5 of the ema_window export below for the whole week; the same with Iggy (1 partition) and Kafka (8 partitions)
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


def topic_records():
    out = sh(f"docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 "
             f"--topic {TOPIC}", check=False)
    return sum(int(line.rsplit(":", 1)[1]) for line in out.split() if line.count(":") == 2)


def group_lag(group):
    """Records of `ticks` not yet committed by the group (offsets are committed at each checkpoint)."""
    out = sh(f"docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "
             f"localhost:9092 --describe --group {group}", check=False)
    lag, seen = 0, False
    for line in out.splitlines():
        f = line.split()
        if len(f) >= 6 and f[1] == TOPIC:
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


def flink_job():
    jobs = json.load(urllib.request.urlopen("http://localhost:8081/jobs/overview"))["jobs"]
    running = [j for j in jobs if j["state"] == "RUNNING"]
    return running[0] if running else None


def load(a):
    days = a.days.split(",")
    files = [f"debs2022-gc-trading-day-{d}-11-21.csv" for d in days]
    for f in files:
        if not (Path(a.data_dir) / f).exists():
            sys.exit(f"missing {Path(a.data_dir) / f}")
    log("== stopping Flink, recreating topic", TOPIC, f"with {a.partitions} partitions")
    sh("docker compose rm -sf jobmanager taskmanager")
    sh("docker compose up -d --wait kafka")
    sh("docker compose build kafka-setup")
    kafka("kafka-topics.sh", f"--delete --if-exists --topic {TOPIC}")
    for _ in range(60):  # deletion is asynchronous
        if TOPIC not in kafka("kafka-topics.sh", "--list").split():
            break
        time.sleep(1)
    out = sh("docker compose run --rm --no-deps kafka-setup", env={"KAFKA_PARTITIONS": str(a.partitions)})
    log("   " + next(l for l in out.splitlines() if f"topic {TOPIC} with" in l))
    t0 = time.time()
    for f in files:
        out = sh("docker compose run --rm --no-deps replay-producer",
                 env={"DATA_DIR": a.data_dir, "CSV_FILE": f"/data/{f}"})
        log("   " + f + ": " + next(l for l in out.splitlines() if "replay done" in l).split("] ")[1])
    n = topic_records()
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


def run(a):
    total = topic_records()
    if total == 0:
        sys.exit("topic ticks is empty: run `bench/backlog.py load` first")
    if a.parallelism > a.taskmanagers * a.slots:
        sys.exit(f"parallelism {a.parallelism} needs at least that many slots "
                 f"(TaskManagers {a.taskmanagers} x slots {a.slots})")
    group = "backlog-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    settings = dict(kv.split("=", 1) for kv in a.set)
    env = {"KAFKA_GROUP": group, "TASKMANAGERS": str(a.taskmanagers), "TASK_SLOTS": str(a.slots),
           "PARALLELISM": str(a.parallelism), **settings}
    log(f"== backlog of {total:,} records; TaskManagers={a.taskmanagers} slots={a.slots} "
        f"parallelism={a.parallelism} group={group}" + (f" {settings}" if settings else ""))
    reset()
    sh("docker compose up -d --wait prometheus cadvisor")
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

    # Done = the group has committed every record (offsets are committed at each checkpoint), and the output has
    # not changed for `stable` seconds (the last windows fire after the idleness timeout).
    last, last_change = None, time.time()
    while True:
        time.sleep(5)
        state = sql("SELECT count(*) || ' ' || (SELECT count(*) FROM data_issue) FROM ema_window")
        if state != last:
            last, last_change = state, time.time()
        lag = group_lag(group)
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
    busy = prom_series("avg by (task_name) (flink_taskmanager_job_task_busyTimeMsPerSecond)", start, end)
    bp = prom_series("avg by (task_name) (flink_taskmanager_job_task_backPressuredTimeMsPerSecond)", start, end)
    cpu = prom_series("sum by (container_env_SERVICE) (rate(container_cpu_usage_seconds_total[30s]))", start, end)
    avg = lambda v: sum(v) / len(v) if v else float("nan")
    tasks = {dict(k)["task_name"]: {"busy": round(avg(v)), "backpressured": round(avg(bp.get(k, [])))}
             for k, v in busy.items()}
    cpu_pct = {dict(k).get("container_env_SERVICE", "?"): round(avg(v) * 100) for k, v in cpu.items()}

    digest = hashlib.md5(subprocess.run(
        ["docker", "compose", "exec", "-T", "timescaledb", "psql", "-U", "postgres", "-d", "trading", "-c", EMA_EXPORT],
        cwd=ROOT, capture_output=True, check=True).stdout).hexdigest()
    rows, buy, sell = sql("SELECT count(*) || ' ' || count(*) FILTER (WHERE advisory = 'BUY') || ' ' || "
                          "count(*) FILTER (WHERE advisory = 'SELL') FROM ema_window").split()
    issues = dict(line.split("|") for line in sql("SELECT reason, count(*) FROM data_issue GROUP BY 1").splitlines())

    result = {
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "taskmanagers": a.taskmanagers, "slots": a.slots, "parallelism": a.parallelism, "settings": settings, "group": group,
        "records": total, "drain_s": round(drain, 1), "records_per_s": round(total / drain),
        "read_rate_avg": round(avg(rates)), "read_rate_max": round(max(rates, default=0)),
        "ema_rows": int(rows), "buy": int(buy), "sell": int(sell), "issues": issues, "digest": digest,
        "tasks": tasks, "cpu_pct": cpu_pct,
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
    log(f"   appended to {RESULTS.relative_to(ROOT)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    l = sub.add_parser("load", help="recreate topic ticks and fill it (Flink stopped)")
    l.add_argument("--data-dir", default="./data")
    l.add_argument("--days", default="08,09,10,11,12,13,14", help="comma-separated days of Nov 2021")
    l.add_argument("--partitions", type=int, default=8)
    r = sub.add_parser("run", help="replay the stored backlog into the job and measure")
    r.add_argument("--taskmanagers", type=int, default=4)
    r.add_argument("--slots", type=int, default=1, help="task slots per TaskManager")
    r.add_argument("--parallelism", type=int, default=4)
    r.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                   help="job setting from docker-compose.yml, e.g. WATERMARK_ALIGNMENT_S=0 (repeatable)")
    r.add_argument("--stable", type=int, default=30, help="seconds without output change that end the run")
    r.add_argument("--timeout", type=int, default=3600)
    sub.add_parser("reset", help="stop the job and remove all results; keep the loaded events")
    a = ap.parse_args()
    {"load": load, "run": run, "reset": reset}[a.cmd](a)


if __name__ == "__main__":
    main()
