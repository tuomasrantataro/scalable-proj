#!/usr/bin/env python3
"""Single-reader throughput of the Iggy Flink source.

1. Fresh Iggy, preload a backlog with the replay producer (Flink not running) -> producer rate.
2. For each poll batch size: new consumer group (so it reads the same backlog from offset 0),
   start Flink with PIPELINE=count, time how fast node-counter drains the backlog -> reader rate.
3. Live run: producer and Flink (full pipeline) at the same time -> does the job keep up?

Usage: python3 bench/source_throughput.py [--repeat 10000] [--batches 100,1000,5000]
Run from the repository root. Wipes the Iggy volume.
"""
import argparse
import functools
import json
import os
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timezone

print = functools.partial(print, flush=True)  # noqa: A001 - show progress when piped
COUNTER = re.compile(r"^(\S+)\s+\|\s+(\S+ \S+) INFO\s+scalable\.report.*\[node-counter subtask=(\d+) host=(\S+)\] total=(\d+)")
VERIFIER = re.compile(r"\[verifier\] total=(\d+) distinct=(\d+) duplicates=(\d+)")


def sh(cmd, env=None, check=True, capture=True):
    e = dict(os.environ, **(env or {}))
    r = subprocess.run(cmd, shell=True, env=e, text=True, capture_output=capture)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd} failed:\n{r.stdout}\n{r.stderr}")
    return r.stdout if capture else ""


def iggy_message_count():
    req = urllib.request.Request("http://localhost:3000/users/login", method="POST",
                                 data=json.dumps({"username": "iggy", "password": "iggy"}).encode(),
                                 headers={"content-type": "application/json"})
    token = json.load(urllib.request.urlopen(req))["access_token"]["token"]
    req = urllib.request.Request("http://localhost:3000/streams/trading/topics/ticks",
                                 headers={"Authorization": f"Bearer {token}"})
    topic = json.load(urllib.request.urlopen(req))
    return sum(int(p["messages_count"]) for p in topic["partitions"])


def counter_samples(since):
    """[(time, subtask, host, total)] from node-counter reports."""
    out = sh(f"docker compose logs --no-color --since {since} taskmanager")
    samples = []
    for line in out.splitlines():
        m = COUNTER.search(line)
        if m:
            t = datetime.strptime(m.group(2), "%Y-%m-%d %H:%M:%S,%f")
            samples.append((t, int(m.group(3)), m.group(4), int(m.group(5))))
    return samples


def cpu_snapshot():
    out = sh("docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}'")
    return {n.replace("scalable-proj-", ""): c for n, c in (l.split() for l in out.splitlines()) if "scalable-proj" in n}


def stop_flink():
    sh("docker compose rm -sf jobmanager taskmanager")


def start_flink(env):
    sh("docker compose up -d --no-deps --force-recreate jobmanager taskmanager", env=env)


def wait_drained(expected, since, timeout):
    """Polls node-counter totals until they reach `expected`. Returns (samples, cpu)."""
    deadline = time.time() + timeout
    cpu = None
    while time.time() < deadline:
        time.sleep(2)
        samples = counter_samples(since)
        latest = {}
        for t, sub, host, total in samples:
            latest[sub] = (t, host, total)
        done = sum(v[2] for v in latest.values())
        if cpu is None and done > 0:
            cpu = cpu_snapshot()  # taken while draining
        if done >= expected:
            return samples, cpu
    raise TimeoutError(f"not drained within {timeout}s (expected {expected})")


def drain_rate(samples, expected):
    """Rate between the first non-zero report and the report that reached `expected`."""
    by_time = {}
    for t, sub, host, total in sorted(samples):
        by_time.setdefault(t, {})[sub] = total
    running, points = {}, []
    for t in sorted(by_time):
        running.update(by_time[t])
        points.append((t, sum(running.values())))
    nonzero = [(t, n) for t, n in points if n > 0]
    t0, n0 = nonzero[0]
    t1, n1 = next((t, n) for t, n in nonzero if n >= expected)
    secs = (t1 - t0).total_seconds()
    readers = {(sub, host) for _, sub, host, total in samples if total > 0}
    return (n1 - n0) / secs if secs > 0 else float("nan"), secs, readers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeat", type=int, default=10000, help="CSV repetitions to preload (x200 msgs)")
    ap.add_argument("--batches", default="100,1000,5000", help="IGGY_POLL_BATCH values")
    ap.add_argument("--partitions", default="1", help="MEASUREMENTS.md results used 4")
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--skip-live", action="store_true")
    ap.add_argument("--data-dir", default="./docs", help="host directory mounted at /data")
    ap.add_argument("--csv", default="pala.csv", help="file name inside --data-dir")
    a = ap.parse_args()
    sh("docker compose build iggy-setup")
    data_env = {"DATA_DIR": a.data_dir, "CSV_FILE": f"/data/{a.csv}"}

    print("== fresh Iggy + setup")
    stop_flink()
    sh("docker compose down -v")
    sh("docker compose up -d iggy iggy-setup", env={"IGGY_PARTITIONS": a.partitions})
    time.sleep(8)

    print(f"== preload: {a.csv} x{a.repeat}, Flink stopped")
    out = sh("docker compose run --rm --no-deps replay-producer", env={"REPEAT": str(a.repeat), **data_env})
    print("   " + next(l for l in out.splitlines() if "replay done" in l))
    expected = iggy_message_count()
    print(f"   Iggy topic holds {expected:,} messages")

    results = []
    for b in a.batches.split(","):
        group = f"bench-{b}"
        sh("docker compose run --rm --no-deps iggy-setup", env={"IGGY_CONSUMER_GROUP": group})
        since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        start_flink({"PIPELINE": "count", "IGGY_POLL_BATCH": b, "IGGY_CONSUMER_GROUP": group,
                     "REPORT_INTERVAL_MS": "1000"})
        samples, cpu = wait_drained(expected, since, a.timeout)
        rate, secs, readers = drain_rate(samples, expected)
        stop_flink()
        results.append((b, rate, secs, readers, cpu))
        print(f"== drain poll_batch={b}: {rate:,.0f} msg/s ({secs:.1f}s), active readers={sorted(readers)}, cpu={cpu}")

    if not a.skip_live:
        best = max(results, key=lambda r: r[1])[0]
        print(f"== live: fresh Iggy, full pipeline, poll_batch={best}, producer at max speed")
        sh("docker compose down -v")
        sh("docker compose up -d iggy iggy-setup", env={"IGGY_PARTITIONS": a.partitions})
        time.sleep(8)
        since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        start_flink({"PIPELINE": "full", "IGGY_POLL_BATCH": best, "REPORT_INTERVAL_MS": "1000"})
        time.sleep(20)  # let the job reach RUNNING before producing
        t_prod = time.time()
        out = sh("docker compose run --rm --no-deps replay-producer", env={"REPEAT": str(a.repeat), **data_env})
        prod_secs = time.time() - t_prod
        print("   " + next(l for l in out.splitlines() if "replay done" in l))
        total = iggy_message_count()
        samples, cpu = wait_drained(total, since, a.timeout)
        rate, secs, readers = drain_rate(samples, total)
        time.sleep(3)
        v = [VERIFIER.search(l) for l in sh(f"docker compose logs --no-color --since {since} taskmanager").splitlines()]
        v = [m for m in v if m][-1]
        print(f"   job consumed {total:,} msgs at {rate:,.0f} msg/s ({secs:.1f}s; producer took {prod_secs:.1f}s incl. container start)")
        print(f"   verifier: total={v.group(1)} distinct={v.group(2)} duplicates={v.group(3)}; cpu={cpu}")


if __name__ == "__main__":
    main()
