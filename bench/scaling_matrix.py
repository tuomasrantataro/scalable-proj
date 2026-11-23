#!/usr/bin/env python3
"""Scaling matrix: several resource configurations, repeated, with median, spread, speedup and efficiency.

Every run is one `bench/backlog.py run` on the events already in topic `ticks` (load them first). Runs of a
configuration are spread over the whole session (repeat 1 of every configuration, then repeat 2 in a rotated
order, ...), so a drift of the host (temperature, caches) does not land on a single configuration.

A run counts only if its result digest is right: the reference digest for the whole week, otherwise the digest most
runs agree on. Wrong runs are reported and left out of the statistics.

  time        job RUNNING → last output row (backlog.py's drain time; includes the 10 s idleness timeout at the
              end, so speedups are slightly underestimated)
  spread      (max - min) / median of the valid repeats
  speedup     median time of the baseline / median time (baseline = lowest parallelism, or --baseline)
  efficiency  speedup / (parallelism / baseline parallelism)
  memory      highest working set of all containers of a service together
  skew        most loaded subtask / average: price events, windows, and busy time of the busiest task

Results: bench/results/scaling_matrix/<time>/ runs.jsonl, summary.json and summary.md, updated after every run.

Usage (repo root):
  python3 bench/backlog.py load --data-dir ./data                # once; --partitions >= the largest parallelism
  python3 bench/scaling_matrix.py                                # 1x1,2x1,4x1,8x1,2x4; 3 repeats
  python3 bench/scaling_matrix.py --configs 1x1,1x2,1x4,1x8 --repeats 5
  python3 bench/scaling_matrix.py --configs 4x2@6                # 4 TaskManagers x 2 slots, parallelism 6
  python3 bench/scaling_matrix.py --set JDBC_BATCH_SIZE=5000 --name batch5000
  python3 bench/scaling_matrix.py --workload uniform --name uniform   # evenly spread symbols (load it first)
  python3 bench/scaling_matrix.py --resume bench/results/scaling_matrix/<time>     # run what is missing
  python3 bench/scaling_matrix.py --summarize bench/results/scaling_matrix/<time>  # only redo the summary
"""
import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import backlog
from backlog import ROOT, WEEK_DIGEST, WORKLOADS, log, sh
from measure import markdown, parse_settings

WEEK_RECORDS = 289_116_808
SERVICES = ("taskmanager", "timescaledb", "kafka")


def parse_config(spec):
    m = re.fullmatch(r"(\d+)x(\d+)(?:@(\d+))?", spec)
    if not m:
        sys.exit(f"configuration {spec}: expected TASKMANAGERSxSLOTS[@PARALLELISM], e.g. 4x1 or 4x2@6")
    tms, slots = int(m.group(1)), int(m.group(2))
    parallelism = int(m.group(3) or tms * slots)
    if not 1 <= parallelism <= tms * slots:
        sys.exit(f"configuration {spec}: parallelism {parallelism} does not fit {tms * slots} slots")
    return {"spec": spec, "taskmanagers": tms, "slots": slots, "parallelism": parallelism}


def plan(configs, repeats, done=()):
    """[(repeat, config)], rotating the order of the configurations from repeat to repeat."""
    runs = []
    for r in range(repeats):
        k = r % len(configs)
        runs += [(r, c) for c in configs[k:] + configs[:k] if (r, c["spec"]) not in done]
    return runs


def expected_digest(runs):
    if runs and all(r["records"] == WEEK_RECORDS and r.get("workload", "real") == "real" for r in runs):
        return WEEK_DIGEST
    return Counter(r["digest"] for r in runs).most_common(1)[0][0] if runs else None


def summarize(runs, configs, baseline=None):
    """Statistics per configuration, in the order of `configs`."""
    ok = [r for r in runs if "error" not in r]
    digest = expected_digest(ok)
    med = lambda values: median(values) if values else None
    out = []
    for c in configs:
        mine = [r for r in ok if r["spec"] == c["spec"]]
        valid = [r for r in mine if r["digest"] == digest]
        times = [r["drain_s"] for r in valid]
        services = {s for r in valid for s in r["cpu_pct"]}
        tasks = {t for r in valid for t in r["tasks"]}
        busy = {t: med([r["tasks"][t]["busy"] for r in valid if t in r["tasks"]]) for t in tasks}
        t_med = med(times)
        out.append({
            **c, "valid": len(valid), "wrong_digest": len(mine) - len(valid),
            "failed": sum(1 for r in runs if "error" in r and r["spec"] == c["spec"]),
            "time_s": t_med, "time_min": min(times, default=None), "time_max": max(times, default=None),
            "spread": (max(times) - min(times)) / t_med if len(times) > 1 else None,
            "records_per_s": med([r["records_per_s"] for r in valid]),
            "cpu_pct": {s: med([r["cpu_pct"][s] for r in valid if s in r["cpu_pct"]]) for s in services},
            "busiest_task": max(busy.items(), key=lambda kv: kv[1], default=None),
            "mem_max_mb": {s: med([r["mem_mb"][s]["max"] for r in valid if s in r.get("mem_mb", {})])
                           for s in {s for r in valid for s in r.get("mem_mb", {})}},
            "skew_records": {k: med([r["skew"]["records"][k]["max_over_mean"] for r in valid
                                     if k in r.get("skew", {}).get("records", {})])
                             for k in {k for r in valid for k in r.get("skew", {}).get("records", {})}},
            "skew_busy": {t: med([r["skew"]["busy"][t]["max_over_mean"] for r in valid
                                  if t in r.get("skew", {}).get("busy", {})])
                          for t in {t for r in valid for t in r.get("skew", {}).get("busy", {})}},
            "latency_p50_ms": med([r["advisory_latency_ms"]["p50"] for r in valid
                                   if r["advisory_latency_ms"]["p50"] is not None]),
            "latency_p99_ms": med([r["advisory_latency_ms"]["p99"] for r in valid
                                   if r["advisory_latency_ms"]["p99"] is not None]),
        })
    base = (next((s for s in out if s["spec"] == baseline), None) if baseline
            else min(out, key=lambda s: s["parallelism"], default=None))
    for s in out:
        known = base and base["time_s"] and s["time_s"]
        s["speedup"] = base["time_s"] / s["time_s"] if known else None
        s["efficiency"] = s["speedup"] / (s["parallelism"] / base["parallelism"]) if known else None
    return {"digest": digest, "baseline": base["spec"] if base else None, "configs": out}


def summary_md(s, info):
    f = lambda v, spec: None if v is None else format(v, spec)
    rows = []
    for c in s["configs"]:
        cpu = c["cpu_pct"]
        task = c["busiest_task"]
        rows.append([
            f"{c['taskmanagers']} × {c['slots']}", c["parallelism"], c["valid"],
            f(c["time_s"], ".1f"), None if c["time_s"] is None else f"{c['time_min']:.1f}–{c['time_max']:.1f}",
            f(c["spread"], ".0%"), f(c["records_per_s"], ",.0f"), f(c["speedup"], ".2f"), f(c["efficiency"], ".0%"),
            task and f"{task[0].split('____')[0][:28]} {task[1]:.0f}",
            " / ".join(f(cpu.get(k), ".0f") or "–" for k in SERVICES),
            " / ".join(f(c["mem_max_mb"].get(k), ",.0f") or "–" for k in SERVICES),
            " / ".join(f(v, ".2f") or "–" for v in (c["skew_records"].get("price events into window"),
                                                   c["skew_records"].get("windows into ema"),
                                                   task and c["skew_busy"].get(task[0]))),
            f"{f(c['latency_p50_ms'], '.0f') or '–'} / {f(c['latency_p99_ms'], '.0f') or '–'}"])
    text = (f"# Scaling matrix\n\n{info['records']:,} records ({info.get('workload', 'real')}), {info['partitions']} partitions, settings "
            f"{info['settings'] or 'default'}, host with {info['cpus']} CPUs. Medians over the valid runs; "
            f"baseline {s['baseline']}, digest `{s['digest']}`.\n\n"
            + markdown(["TaskManagers × slots", "Parallelism", "Runs", "Time (s)", "Min–max", "Spread", "Records/s",
                        "Speedup", "Efficiency", "Busiest task (ms/s)", "CPU % TaskManagers / TimescaleDB / Kafka",
                        "Memory MB (same)", "Skew events / windows / busy",
                        "Advisory latency p50 / p99 (ms)"], rows))
    for c in s["configs"]:
        if c["wrong_digest"] or c["failed"]:
            text += f"\n- {c['spec']}: {c['wrong_digest']} run(s) with a wrong digest, {c['failed']} failed (left out)"
    return text + "\n"


def write_summary(out_dir):
    info = json.loads((out_dir / "info.json").read_text())
    runs_file = out_dir / "runs.jsonl"
    runs = [json.loads(l) for l in runs_file.read_text().splitlines() if l.strip()] if runs_file.is_file() else []
    s = summarize(runs, info["configs"], info["baseline"])
    (out_dir / "summary.json").write_text(json.dumps(s, indent=1))
    (out_dir / "summary.md").write_text(summary_md(s, info))
    return runs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--configs", default="1x1,2x1,4x1,8x1,2x4", help="TASKMANAGERSxSLOTS[@PARALLELISM],...")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--baseline", help="configuration the speedup refers to (default: lowest parallelism)")
    ap.add_argument("--workload", choices=WORKLOADS, default="real", help="as in bench/backlog.py")
    ap.add_argument("--cooldown", type=int, default=30, help="seconds between runs")
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                    help="job setting from docker-compose.yml for every run (repeatable)")
    ap.add_argument("--stable", type=int, default=30, help="seconds without output change that end a run")
    ap.add_argument("--timeout", type=int, default=3600, help="seconds per run")
    ap.add_argument("--name", default="", help="name of this matrix (in the directory name)")
    ap.add_argument("--dry-run", action="store_true", help="only print the plan")
    ap.add_argument("--resume", metavar="DIR", help="continue a matrix with its own configurations and settings")
    ap.add_argument("--summarize", metavar="DIR", help="only redo the summary of DIR")
    a = ap.parse_args()
    if a.summarize:
        write_summary(Path(a.summarize))
        log((Path(a.summarize) / "summary.md").read_text())
        return
    done = set()
    if a.resume:
        out_dir = Path(a.resume).resolve()
        info = json.loads((out_dir / "info.json").read_text())
        done = {(r["repeat"], r["spec"]) for r in write_summary(out_dir) if "error" not in r}
    else:
        info = {"configs": [parse_config(s) for s in a.configs.split(",")], "repeats": a.repeats,
                "baseline": a.baseline, "settings": parse_settings(a.set), "workload": a.workload, "cpus": os.cpu_count()}
        specs = [c["spec"] for c in info["configs"]]
        if len(set(specs)) != len(specs):
            sys.exit("a configuration is listed twice")
        if a.baseline and a.baseline not in specs:
            sys.exit(f"baseline {a.baseline} is not in --configs")
    runs = plan(info["configs"], info["repeats"], done)
    log(f"== {len(runs)} runs: " + " ".join(f"{c['spec']}#{r + 1}" for r, c in runs))
    if a.dry_run or not runs:
        return

    sh("docker compose up -d --wait kafka")
    workload = info.get("workload", "real")
    topic = WORKLOADS[workload]
    per_partition = backlog.partition_records(topic)
    records, partitions = sum(per_partition), len(per_partition)
    if records == 0:
        sys.exit(f"topic {topic} is empty: run `bench/backlog.py load --workload {workload}` first")
    if a.resume and (records, partitions) != (info["records"], info["partitions"]):
        sys.exit(f"topic {topic} changed: {records} records in {partitions} partitions, the matrix started with "
                 f"{info['records']} in {info['partitions']}")
    for c in info["configs"]:
        if c["parallelism"] > partitions:
            log(f"   WARNING: {c['spec']}: only {partitions} of {c['parallelism']} source subtasks get a partition")
    if not a.resume:
        info.update(records=records, partitions=partitions)
        out_dir = ROOT / "bench" / "results" / "scaling_matrix" / (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + (f"-{a.name}" if a.name else ""))
        out_dir.mkdir(parents=True)
        (out_dir / "info.json").write_text(json.dumps(info, indent=1))

    failed = 0
    for i, (repeat, c) in enumerate(runs):
        if i:
            time.sleep(a.cooldown)
        log(f"\n== run {i + 1}/{len(runs)}: {c['spec']}, parallelism {c['parallelism']}, repeat {repeat + 1}")
        try:
            result = backlog.run(argparse.Namespace(
                workload=workload, taskmanagers=c["taskmanagers"], slots=c["slots"], parallelism=c["parallelism"],
                set=[f"{k}={v}" for k, v in info["settings"].items()], stable=a.stable, timeout=a.timeout))
        except (RuntimeError, OSError, ValueError, KeyError, SystemExit) as e:
            failed += 1
            log(f"   FAILED: {e}")
            result = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "error": str(e)[:2000]}
        with (out_dir / "runs.jsonl").open("a") as f:
            f.write(json.dumps({"spec": c["spec"], "repeat": repeat, **result}) + "\n")
        write_summary(out_dir)
    log("\n" + (out_dir / "summary.md").read_text())
    log(f"== done: {out_dir.relative_to(ROOT)}")
    if failed:
        sys.exit(f"{failed} run(s) failed; repeat them with --resume {out_dir.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
