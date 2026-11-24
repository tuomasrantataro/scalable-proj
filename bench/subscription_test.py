#!/usr/bin/env python3
"""Full week with the default subscriptions and with every symbol subscribed: does delivering more advisories change
latency, throughput or resource use? One scaling only (4 TaskManagers x 1 slot, parallelism 4).

Runs the two settings alternately (ABBA...), after a discarded warm-up run:
  backlog   bench/backlog.py run: the stored week as fast as the job reads it (latency at saturation)
  live      scripts/run-live.sh: the producer replays the week while the job runs

Per run, besides what these scripts report:
  digest and delivery   ema_window against WEEK_DIGEST; messages in topic `advisories` against the reference and the
                        run's own subscriptions file (missing / unexpected)
  windows               fired → emitted of all 1.86 M windows, whatever is subscribed (TimescaleDB)
  shared advisories     fired → stored of the (symbol, window) pairs that BOTH settings deliver, so the same advisories
                        are compared; from `fired_at` in the message and the broker's append time
All without the first close (cold start).

Results: bench/results/subscription_test/<time>/ results.jsonl and summary.md. Needs `ticks` loaded and ./data.

Usage (repo root):
  reference/.venv/bin/python bench/subscription_test.py --backlog 3 --live 2
"""
import argparse
import hashlib
import json
import os
import re
import subprocess
import urllib.request
from datetime import datetime, timezone

import pandas as pd

import backlog
from advisory_latency import RESULTS as LATENCY_RESULTS, epoch_ms, percentile
from backlog import ROOT, WEEK_DIGEST, kafka, log, sh, sql
from measure import markdown

FILES = {"default": "subscriptions.txt", "all": "subscriptions-all.txt"}


def subscriptions(name):
    subs = {}
    for line in (ROOT / "config" / FILES[name]).read_text().splitlines():
        line = line.split("#")[0].strip()
        if line:
            trader, symbols = line.split(":")
            subs.setdefault(trader.strip(), set()).update(s for s in re.split(r"[,\s]+", symbols) if s)
    return subs


def expected(name, ref):
    subs = subscriptions(name)
    return {(t, r.symbol, r.window_end.strftime("%Y-%m-%dT%H:%M:%S"), r.type)
            for r in ref.itertuples() for t, s in subs.items() if "*" in s or r.symbol in s}


def messages():
    """[(LogAppendTime ms, advisory)] of topic `advisories`, first copy of each (trader, symbol, window_end)."""
    out = kafka("kafka-console-consumer.sh", "--topic advisories --from-beginning --timeout-ms 10000 "
                "--formatter-property print.timestamp=true 2>/dev/null")
    seen, res = set(), []
    for line in out.splitlines():
        m = re.match(r"LogAppendTime:(\d+)\t(\{.*)", line)
        if not m:
            continue
        a = json.loads(m.group(2))
        key = (a["trader"], a["symbol"], a["window_end"])
        if key not in seen:
            seen.add(key)
            res.append((int(m.group(1)), a))
    return res


def pct(values):
    v = sorted(values)
    return {"n": len(v), **{k: round(percentile(v, q)) for k, q in (("p50", 50), ("p95", 95), ("p99", 99), ("max", 100))}} \
        if v else {"n": 0}


def collect(name, ref, shared):
    """What both run types have in common, read after the run."""
    msgs = messages()
    got = {(a["trader"], a["symbol"], a["window_end"], a["type"]) for _, a in msgs}
    exp = expected(name, ref)
    first = min(w for _, w in shared)   # the same close in both settings
    best = {}   # several traders may get the same advisory: the first one stored counts
    for stored, a in msgs:
        key = (a["symbol"], a["window_end"])
        if key in shared and a["window_end"] != first:
            best[key] = min(best.get(key, 1e18), stored - epoch_ms(a["fired_at"]))
    win = sql("SELECT count(*), " + ", ".join(
        f"round(percentile_disc({q}) WITHIN GROUP (ORDER BY ms))" for q in (0.5, 0.95, 0.99)) + ", round(max(ms)) FROM "
        "(SELECT extract(epoch FROM emitted_at - fired_at) * 1000 AS ms FROM ema_window "
        "WHERE window_end > (SELECT min(window_end) FROM ema_window)) t").split("|")
    adv = sql("SELECT count(*), " + ", ".join(
        f"round(percentile_disc({q}) WITHIN GROUP (ORDER BY ms))" for q in (0.5, 0.95, 0.99)) + ", round(max(ms)) FROM "
        "(SELECT extract(epoch FROM emitted_at - fired_at) * 1000 AS ms FROM ema_window WHERE advisory IS NOT NULL "
        "AND window_end > (SELECT min(window_end) FROM ema_window)) t").split("|")
    keys = ("n", "p50", "p95", "p99", "max")
    digest = hashlib.md5(subprocess.run(
        ["docker", "compose", "exec", "-T", "timescaledb", "psql", "-U", "postgres", "-d", "trading", "-c",
         backlog.EMA_EXPORT], cwd=ROOT, capture_output=True, check=True).stdout).hexdigest()
    return {"digest_ok": digest == WEEK_DIGEST, "late": int(sql("SELECT count(*) FROM data_issue WHERE reason = 'LATE'")),
            "delivered": len(got), "expected": len(exp), "missing": len(exp - got), "unexpected": len(got - exp),
            "windows_fired_to_emitted_ms": dict(zip(keys, map(int, map(float, win)))),
            "advisories_fired_to_emitted_ms": dict(zip(keys, map(int, map(float, adv)))),
            "shared_fired_to_stored_ms": pct(best.values())}


def run_backlog(name):
    r = backlog.run(argparse.Namespace(workload="real", taskmanagers=4, slots=1, parallelism=4, stable=30, timeout=3600,
                                       set=[f"SUBSCRIPTIONS_FILE=/config/{FILES[name]}"]))
    return {k: r[k] for k in ("group", "drain_s", "records_per_s", "tasks", "cpu_pct", "mem_mb")}


def run_live(name):
    out = sh("scripts/run-live.sh", env={"SUBSCRIPTIONS_FILE": f"/config/{FILES[name]}", "TASKMANAGERS": "4",
                                         "TASK_SLOTS": "1", "PARALLELISM": "4"})
    log("\n".join("   " + l.strip() for l in out.splitlines() if "replay done" in l or "fired → received" in l))
    sent = [float(x) for x in re.findall(r"\(first to last row sent: ([\d.]+) s\)", out)]
    job = backlog.flink_job()
    detail = json.load(urllib.request.urlopen(f"http://localhost:8081/jobs/{job['jid']}"))
    t0 = detail["timestamps"]["RUNNING"] / 1000
    first, last = (float(x) for x in sql("SELECT extract(epoch FROM min(emitted_at)) || ' ' || extract(epoch FROM "
                                         "max(emitted_at)) FROM ema_window").split())
    avg = lambda v: sum(v) / len(v) if v else float("nan")
    start, end = int(first), int(last) + 5
    cpu = backlog.prom_series("sum by (container_env_SERVICE) (rate(container_cpu_usage_seconds_total[30s]))", start, end)
    times = backlog.task_times(job["jid"], start, end, from_start=False)
    return {"group": "live", "producer_s": round(sum(sent), 1), "drain_s": round(last - first, 1),
            "records_per_s": round(backlog.topic_records("live") / (last - first)),
            "tasks": {n: {k: round(avg(v)) for k, v in t.items()} for n, t in times.items()},
            "cpu_pct": {dict(k).get("container_env_SERVICE", "?"): round(avg(v) * 100) for k, v in cpu.items() if v},
            "mem_mb": backlog.memory(start, end)}


def last_latency():
    """The latest line of advisory_latency.jsonl: what the trader's consumer saw in the run just finished."""
    r = json.loads(LATENCY_RESULTS.read_text().splitlines()[-1])
    return {"messages": r["messages"], "ms": r["ms"], "cold_messages": r["cold_messages"], "before_ready": r["before_ready"]}


def ms(d, q=("p50", "p95", "max")):
    return " / ".join(f"{d[k]:,.0f}" for k in q) if d.get("n", 1) and d.get("p50") is not None else "–"


def summarize(results):
    out = ["# Subscriptions: default file against every symbol (full week, 4x1)\n",
           "Latencies in ms, p50 / p95 / max, without the first close.\n"]
    rows = []
    for r in results:
        if "error" in r:
            rows.append([r["kind"], r["name"], r["error"][:60]] + [None] * 6)
            continue
        rec = r["received"]["ms"]["fired_to_received"]
        rows.append([r["kind"], r["name"], "yes" if r["digest_ok"] else "NO", f"{r['delivered']:,} / {r['missing']} / {r['unexpected']}",
                     f"{r['drain_s']:.1f}", f"{ms(rec)} (n {r['received']['messages']:,})",
                     f"{ms(r['shared_fired_to_stored_ms'])} (n {r['shared_fired_to_stored_ms']['n']})",
                     ms(r["windows_fired_to_emitted_ms"]),
                     f"{r['cpu_pct'].get('taskmanager')} / {r['cpu_pct'].get('kafka')} / {r['cpu_pct'].get('timescaledb')}"])
    out.append(markdown(["Run", "Subscriptions", "Digest ok", "Delivered / missing / unexpected", "Time (s)",
                         "Fired → received, delivered", "Fired → stored, shared advisories",
                         "Fired → emitted, all windows", "CPU % taskmanager / kafka / timescaledb"], rows))
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backlog", type=int, default=3, help="backlog runs per setting")
    ap.add_argument("--live", type=int, default=2, help="live runs per setting")
    ap.add_argument("--no-warmup", action="store_true")
    ap.add_argument("--name", default="")
    a = ap.parse_args()
    os.chdir(ROOT)
    out_dir = ROOT / "bench" / "results" / "subscription_test" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + (f"-{a.name}" if a.name else ""))
    out_dir.mkdir(parents=True)
    ref = pd.read_csv(ROOT / "reference/output-nozero/advisories.csv", parse_dates=["window_end"])
    shared = {(s, w) for _, s, w, _ in expected("default", ref)} & {(s, w) for _, s, w, _ in expected("all", ref)}
    sh("docker compose build kafka-setup jobmanager advisory-latency")
    plan = []
    for kind, n in (("backlog", a.backlog), ("live", a.live)):
        for i in range(n):
            plan += [(kind, name) for name in (("default", "all") if i % 2 == 0 else ("all", "default"))]
    if not a.no_warmup:
        log("## warm-up (discarded)")
        run_backlog("default")
    results = []
    for i, (kind, name) in enumerate(plan, 1):
        log(f"## {i}/{len(plan)} {kind}, subscriptions {name}")
        os.environ.pop("KAFKA_TOPIC", None)
        try:
            r = {"kind": kind, "name": name, "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 **(run_backlog if kind == "backlog" else run_live)(name), "received": last_latency()}
            r.update(collect(name, ref, shared))
            log(f"   delivered {r['delivered']:,}, missing {r['missing']}, unexpected {r['unexpected']}, "
                f"digest ok {r['digest_ok']}, shared {r['shared_fired_to_stored_ms']}")
        except (Exception, SystemExit) as e:
            r = {"kind": kind, "name": name, "error": f"{type(e).__name__}: {e}"}
            log("   FAILED", r["error"])
        results.append(r)
        with (out_dir / "results.jsonl").open("a") as f:
            f.write(json.dumps(r) + "\n")
        (out_dir / "summary.md").write_text(summarize(results))
    log(summarize(results))


if __name__ == "__main__":
    main()
