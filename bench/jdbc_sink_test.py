#!/usr/bin/env python3
"""Flink JDBC connector (4.1.0-2.2) on Flink 2.3 with TimescaleDB: correctness, idempotency, failover.

Phases (all on the same Kafka backlog, each with its own consumer group so it reads from offset 0):
  A  baseline     fresh table; write every closed 5-min window; snapshot it as ref_a
  B  idempotency  same data again, table NOT truncated -> same rows and content, every row writes = 2
  C  failover     fresh table; kill a TaskManager running a source reader mid-run, restart it;
                  compare with ref_a (missing / changed / extra rows, rows written more than once)
  D  no idleness  fresh table, IDLENESS_S=0. With the Iggy connector no window fired (an idle reader held
                  back the watermark); with Kafka every reader has partitions, so windows should fire.

Usage (repo root; wipes Kafka and TimescaleDB):
  python3 bench/jdbc_sink_test.py --data-dir ./data --csv <day>.csv
"""
import argparse
import json
import os
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timezone

print_ = print


def print(*a, **k):  # noqa: A001 - progress must show when piped
    print_(*a, **k, flush=True)


def sh(cmd, env=None, check=True):
    r = subprocess.run(cmd, shell=True, env=dict(os.environ, **(env or {})), text=True, capture_output=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd} failed:\n{r.stdout}\n{r.stderr}")
    return r.stdout


def sql(q):
    return sh(f'docker compose exec -T timescaledb psql -U postgres -d trading -tA -c "{q}"').strip()


CONTENT = "count(*) || ' ' || md5(coalesce(string_agg(symbol||'|'||window_end||'|'||close||'|'||ticks||'|'||last_event_time, ',' ORDER BY symbol, window_end), ''))"


def snapshot():
    rows, digest = sql(f"SELECT {CONTENT} FROM window_close").split()
    return int(rows), digest


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def late_events(since):
    latest = {}
    for line in sh(f"docker compose logs --no-color --since {since} taskmanager").splitlines():
        m = re.search(r"\[late-events subtask=(\d+)\] total=(\d+)", line)
        if m:
            latest[m.group(1)] = int(m.group(2))
    return sum(latest.values())


def start_flink(group, extra=None):
    env = {"PIPELINE": "jdbc", "KAFKA_GROUP": group, "REPORT_INTERVAL_MS": "1000"}
    env.update(extra or {})
    sh("docker compose up -d --no-deps --force-recreate jobmanager taskmanager", env=env)


def stop_flink():
    sh("docker compose rm -sf jobmanager taskmanager")


def wait_stable(stable_s=20, timeout=1200, min_rows=1, query="SELECT count(*) FROM window_close"):
    """Waits until `query` is >= min_rows and unchanged for `stable_s`. Returns seconds until the last change."""
    t0 = time.time()
    last, last_change = -1, time.time()
    while time.time() - t0 < timeout:
        time.sleep(2)
        try:
            n = int(sql(query) or 0)
        except RuntimeError:
            continue
        if n != last:
            last, last_change = n, time.time()
        elif n >= min_rows and time.time() - last_change >= stable_s:
            return last_change - t0
    raise TimeoutError(f"row count not stable within {timeout}s (last={last})")


def flink_rest(path):
    return json.load(urllib.request.urlopen(f"http://localhost:8081{path}"))


def active_source_container():
    """Container name of the TaskManager whose source subtask has emitted the most records."""
    jobs = [j for j in flink_rest("/jobs")["jobs"] if j["status"] == "RUNNING"]
    job = flink_rest(f"/jobs/{jobs[0]['id']}")
    src = next(v for v in job["vertices"] if v["name"].startswith("Source"))
    subtasks = flink_rest(f"/jobs/{job['jid']}/vertices/{src['id']}")["subtasks"]
    busy = max(subtasks, key=lambda s: s["metrics"]["write-records"])
    # Flink 2.3 REST: "endpoint": "scalable-proj-taskmanager-1.scalable-proj_default:41357"
    container = busy["endpoint"].split(":")[0].split(".")[0]
    return container, busy["metrics"]["write-records"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--partitions", default="8", help="partitions of topic ticks")
    ap.add_argument("--kill-after-rows", type=float, default=0.3, help="phase C: kill at this fraction of ref rows")
    a = ap.parse_args()
    os.environ["KAFKA_PARTITIONS"] = a.partitions  # read by kafka-setup in docker-compose.yml
    print(f"== partitions: {a.partitions}")

    print("== setup: fresh Kafka + TimescaleDB, preload")
    stop_flink()
    sh("docker compose down -v")
    sh("docker compose build kafka-setup jobmanager")
    sh("docker compose up -d --wait kafka timescaledb")
    sh("docker compose run --rm --no-deps kafka-setup")
    out = sh("docker compose run --rm --no-deps replay-producer",
             env={"DATA_DIR": a.data_dir, "CSV_FILE": f"/data/{a.csv}"})
    print("   " + next(l for l in out.splitlines() if "replay done" in l))

    # A: baseline
    print("== A baseline")
    since = now_utc()
    start_flink("jdbc-a")
    secs = wait_stable()
    rows_a, digest_a = snapshot()
    late_a = late_events(since)
    stop_flink()
    sql("DROP TABLE IF EXISTS ref_a; CREATE TABLE ref_a AS SELECT * FROM window_close")
    writes = sql("SELECT min(writes) || ' ' || max(writes) FROM window_close")
    print(f"   rows={rows_a} digest={digest_a} late_events={late_a} writes(min max)={writes} "
          f"time_to_last_row={secs:.0f}s")

    # B: idempotency (no truncate)
    print("== B idempotency: same data again, table not truncated")
    since = now_utc()
    start_flink("jdbc-b")
    # the row count is already final here, so wait on the total number of writes instead
    wait_stable(min_rows=rows_a + 1, query="SELECT sum(writes) FROM window_close")
    rows_b, digest_b = snapshot()
    late_b = late_events(since)
    stop_flink()
    writes = sql("SELECT count(*) FILTER (WHERE writes = 2) || ' ' || count(*) FILTER (WHERE writes <> 2) FROM window_close")
    print(f"   rows={rows_b} same_content={digest_b == digest_a} late_events={late_b} "
          f"rows_with_writes=2/other={writes}")

    # C: failover
    print("== C failover: kill a TaskManager with a source reader mid-run")
    sql("TRUNCATE window_close")
    since = now_utc()
    start_flink("jdbc-c")
    target = int(rows_a * a.kill_after_rows)
    while int(sql("SELECT count(*) FROM window_close")) < target:
        time.sleep(1)
    victim, records = active_source_container()
    print(f"   killing {victim} (source emitted {records} records so far), rows={sql('SELECT count(*) FROM window_close')}")
    sh(f"docker kill {victim}")
    time.sleep(5)
    sh(f"docker start {victim}")
    secs = wait_stable(stable_s=30, timeout=1800)
    rows_c, digest_c = snapshot()
    late_c = late_events(since)
    restarts = len(re.findall(r"switched from state RESTARTING to RUNNING", sh(f"docker compose logs --no-color --since {since} jobmanager")))
    diff = sql("SELECT "
               "(SELECT count(*) FROM ref_a r WHERE NOT EXISTS (SELECT 1 FROM window_close w WHERE w.symbol=r.symbol AND w.window_end=r.window_end)) || ' ' || "
               "(SELECT count(*) FROM window_close w WHERE NOT EXISTS (SELECT 1 FROM ref_a r WHERE w.symbol=r.symbol AND w.window_end=r.window_end)) || ' ' || "
               "(SELECT count(*) FROM window_close w JOIN ref_a r USING (symbol, window_end) WHERE w.close<>r.close OR w.ticks<>r.ticks) || ' ' || "
               "(SELECT coalesce(sum(r.ticks - w.ticks), 0) FROM window_close w JOIN ref_a r USING (symbol, window_end) WHERE w.ticks<r.ticks) || ' ' || "
               "(SELECT count(*) FROM window_close WHERE writes > 1)")
    missing, extra, changed, ticks_lost, rewritten = diff.split()
    stop_flink()
    print(f"   job restarts={restarts} rows={rows_c} same_content_as_A={digest_c == digest_a} late_events={late_c}")
    print(f"   vs A: missing_rows={missing} extra_rows={extra} changed_rows={changed} "
          f"ticks_missing_in_changed_rows={ticks_lost} rows_written_more_than_once={rewritten}")

    # D: no idleness. With the Iggy connector the idle reader held the watermark at Long.MIN_VALUE and no window
    # fired. With Kafka every reader has partitions, so windows fire; only the last ones wait for more data.
    print("== D no idleness (IDLENESS_S=0), 90 s")
    sql("TRUNCATE window_close")
    start_flink("jdbc-d", {"IDLENESS_S": "0"})
    time.sleep(90)
    rows_d = int(sql("SELECT count(*) FROM window_close"))
    job = next(j for j in flink_rest("/jobs")["jobs"] if j["status"] == "RUNNING")["id"]
    window = next(v["id"] for v in flink_rest(f"/jobs/{job}")["vertices"] if "window" in v["name"])
    watermarks = [w["value"] for w in flink_rest(f"/jobs/{job}/vertices/{window}/watermarks")]
    failures = len(flink_rest(f"/jobs/{job}/exceptions").get("exceptionHistory", {}).get("entries", []))
    oversized = "larger than the maximum permitted memory-backed state" in sh("docker compose logs --no-color taskmanager")
    stop_flink()
    print(f"   rows={rows_d} window_watermarks={watermarks} job_failures={failures} "
          f"checkpoint_state_over_5MB={oversized}")

if __name__ == "__main__":
    main()
