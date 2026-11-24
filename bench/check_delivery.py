#!/usr/bin/env python3
"""Were exactly the subscribed advisories delivered? Topic `advisories` against the reference and the subscriptions.

Expected: every advisory of the pandas reference, once per trader subscribed to its symbol. Delivered: the messages in
topic `advisories`, compared by (trader, symbol, window end, BUY/SELL). Repeats (at-least-once delivery) count once.

Run it after a run of the whole week and before the next one, which empties the topic. The subscriptions file is the one
the job was started with, unless --subscriptions names another. With --through (a run of some days only), windows that
the job could not close yet are left out: those after the last window in `ema_window`.

Exit code 0 = nothing missing and nothing unexpected, 1 = otherwise.

Usage (repo root; Kafka and the job still running; needs the output of reference/ema_reference.py):
  python3 bench/check_delivery.py
  python3 bench/check_delivery.py --through 2021-11-08        # only 8 Nov was processed
"""
import argparse
import csv
import json
import re
import sys
from pathlib import Path

from backlog import ROOT, kafka, sh, sql

REFERENCE = ROOT / "reference" / "output-nozero" / "advisories.csv"


def job_subscriptions():
    """The subscriptions file of the running job: /config/<name> in the container is config/<name> here."""
    name = sh("docker compose exec -T jobmanager printenv SUBSCRIPTIONS_FILE", check=False).strip()
    if not name.startswith("/config/"):
        sys.exit("the job is not running: name its subscriptions file with --subscriptions")
    return ROOT / name.lstrip("/")


def subscriptions(path):
    """{trader: {symbols}}; "*" = every symbol. Format: config/subscriptions.txt."""
    subs = {}
    for line in path.read_text().splitlines():
        line = line.split("#")[0].strip()
        if line:
            trader, symbols = line.split(":")
            subs.setdefault(trader.strip(), set()).update(s for s in re.split(r"[,\s]+", symbols) if s)
    return subs


def expected(reference, subs, last_window=None):
    out = set()
    with reference.open(newline="") as f:
        for r in csv.DictReader(f):
            end = r["window_end"].replace(" ", "T")
            if last_window is None or end <= last_window:
                out |= {(t, r["symbol"], end, r["type"]) for t, s in subs.items() if "*" in s or r["symbol"] in s}
    return out


def delivered():
    out = kafka("kafka-console-consumer.sh", "--topic advisories --from-beginning --timeout-ms 10000 2>/dev/null")
    messages = [json.loads(line) for line in out.splitlines() if line.startswith("{")]
    return [(m["trader"], m["symbol"], m["window_end"], m["type"]) for m in messages]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--subscriptions", type=Path, help="default: the file the running job uses")
    ap.add_argument("--reference", type=Path, default=REFERENCE)
    ap.add_argument("--through", metavar="YYYY-MM-DD", help="last day that was processed")
    ap.add_argument("--show", type=int, default=5, help="missing and unexpected messages to print")
    a = ap.parse_args()
    if not a.reference.is_file():
        sys.exit(f"missing {a.reference}: run reference/ema_reference.py --exclude-zero-prices --out reference/output-nozero")
    path = a.subscriptions or job_subscriptions()
    subs = subscriptions(path)
    last = None
    if a.through:
        last = min(sql("SELECT to_char(max(window_end) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') FROM ema_window"
                       .replace('"', '\\"')), f"{a.through}T24:00:00")
    exp, msgs = expected(a.reference, subs, last), delivered()
    got = set(msgs)
    missing, unexpected = sorted(exp - got), sorted(got - exp)
    print(f"subscriptions  {path.name}: {len(subs):,} traders" + (f"; windows up to {last}" if last else ""))
    print(f"messages       {len(msgs):,} ({len(msgs) - len(got):,} repeats)")
    print(f"delivered      {len(got):,} of {len(exp):,} expected, {len(missing):,} missing, {len(unexpected):,} unexpected")
    for name, rows in (("missing", missing), ("unexpected", unexpected)):
        for r in rows[:a.show]:
            print(f"  {name}: {' '.join(r)}")
    ok = not missing and not unexpected and bool(exp)
    print("CORRECT" if ok else "DIFFERENT" if exp else "NOTHING EXPECTED: no subscribed advisory, so nothing was checked")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
