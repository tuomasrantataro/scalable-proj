#!/usr/bin/env python3
"""Kills a TaskManager in the middle of a run and starts it again. The results must be the same as without the failure.

Starts `bench/backlog.py run` (4 TaskManagers x 1 slot, parallelism 4), kills one TaskManager container --after seconds
after the job runs, starts it again after --down seconds, and waits for the run to end. Checks that the kill came while the
job was still reading and that the job restarted. Whether the results are right is checked by reference/compare.py and
bench/check_delivery.py afterwards. The time, CPU and skew of this run are not comparable with other runs.

Exit code 0 = the failure happened and the run ended, 1 = otherwise.

Usage (repo root; needs the loaded events):
  python3 bench/failover.py
  python3 bench/failover.py --after 18        # one day is read in about 40 s
"""
import argparse
import subprocess
import sys
import threading
import time

from backlog import ROOT, flink_job, log, prom, sh, sql


def restarts(job_id):
    """From Prometheus: Flink's REST API returns an old value at the first query after a while."""
    r = prom("query", query=f'max(max_over_time(flink_jobmanager_job_numRestarts{{job_id="{job_id}"}}[30m]))')
    return int(float(r[0]["value"][1])) if r else 0


def fail(after, down, state):
    """In a thread: kills and restarts one TaskManager."""
    time.sleep(after)
    container = sh("docker compose ps -q taskmanager").split()[0]
    state["ended_before_kill"] = state["ended"]
    sh(f"docker kill {container}")
    log(f"== failover: killed TaskManager {container[:12]}")
    time.sleep(down)
    sh(f"docker start {container}")
    log("== failover: started it again")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--after", type=float, default=30, help="seconds from job RUNNING to the kill")
    ap.add_argument("--down", type=float, default=10, help="seconds until the TaskManager is started again")
    a = ap.parse_args()
    state, thread = {"ended": False}, None
    run = subprocess.Popen([sys.executable, "-u", "bench/backlog.py", "run"], cwd=ROOT, stdout=subprocess.PIPE, text=True)
    for line in run.stdout:
        print(line, end="", flush=True)
        if " RUNNING at " in line and thread is None:
            thread = threading.Thread(target=fail, args=(a.after, a.down, state))
            thread.start()
        if line.startswith("== drained") or line.startswith("== advisory latency"):
            state["ended"] = True
    code = run.wait()
    if thread is None or code != 0:
        sys.exit(f"FAILED: the run ended with code {code}")
    thread.join()
    if state.get("ended_before_kill", True):
        sys.exit("FAILED: the run had ended before the kill; use a smaller --after")
    n = restarts(flink_job()["jid"])
    again = int(sql("SELECT count(*) FROM ema_window WHERE writes > 1"))
    log(f"== failover: the job restarted {n} time(s); {again:,} rows were written more than once")
    if not n:
        sys.exit("FAILED: the job did not restart, so nothing was tested")


if __name__ == "__main__":
    main()
