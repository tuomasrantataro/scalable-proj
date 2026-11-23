#!/usr/bin/env python3
"""Long-tail analysis of the data set: how unevenly the load is spread over symbols, and what that means for the
Kafka partitions and the Flink subtasks. Reads the CSV files only (no Docker); a few minutes for the week.

Three loads per symbol, one for each stage of the job:
  rows      every CSV row: producer, Kafka, Flink source
  prices    usable price events (Last set and not 0): keyed shuffle and window operator
  windows   (symbol, 5-min window by Trading time) pairs with a price event: EMA function and TimescaleDB. Late events,
            which the job drops, are included (1,812 windows in the week).

Reports, per load: the share of the most active symbols, the Gini coefficient (0 = even, 1 = one symbol has all),
the symbols that make up 50/80/90/99%, per exchange and security type; and the balance (most loaded / average) over
partitions and subtasks with the real assignment: the producer's hash for Kafka, key groups for Flink. 1.00 is what
evenly spread symbols would approach; parallelism / balance is the best speedup the assignment allows.

Writes <out>/summary.md, summary.json and symbols.csv (one row per symbol).

Usage (repo root):
  python3 bench/symbol_distribution.py
  python3 bench/symbol_distribution.py --days 08 --out /tmp/day08
"""
import argparse
import csv
import itertools
import json
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import workload
from backlog import ROOT
from measure import markdown

LOADS = ("rows", "prices", "windows")
PARTITIONS = (4, 8, 16, 32)
PARALLELISM = (2, 4, 8, 16, 32)
MAX_PARALLELISM = 128       # Flink's default up to parallelism 85
LAST, TRADING_TIME = 21, 23


# --- assignment to partitions and subtasks ---------------------------------------------------------------------------

def java_hash(s):
    """String.hashCode() as a signed 32-bit integer (symbols are ASCII)."""
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return h - (1 << 32) if h >= 1 << 31 else h


def partition(symbol, partitions):
    """As in producer-rs: Math.floorMod(hashCode, partitions)."""
    return java_hash(symbol) % partitions


def murmur(code):
    """Flink's MathUtils.murmurHash(int)."""
    m = 0xFFFFFFFF
    rotl = lambda x, r: ((x << r) | (x >> (32 - r))) & m
    code = code & m
    code = rotl(code * 0xCC9E2D51 & m, 15) * 0x1B873593 & m
    code = (rotl(code, 13) * 5 + 0xE6546B64) & m
    code ^= 4
    code ^= code >> 16
    code = code * 0x85EBCA6B & m
    code ^= code >> 13
    code = code * 0xC2B2AE35 & m
    code ^= code >> 16
    if code < 1 << 31:
        return code
    code = (1 << 32) - code        # -code of the negative int
    return code if code < 1 << 31 else 0


def subtask(symbol, parallelism):
    """The subtask of a keyed operator (key = symbol): key group × parallelism / max parallelism."""
    return murmur(java_hash(symbol)) % MAX_PARALLELISM * parallelism // MAX_PARALLELISM


def balance(load, assign, n):
    """Most loaded / average over n partitions or subtasks."""
    totals = [0] * n
    for symbol, v in load.items():
        totals[assign(symbol, n)] += v
    return max(totals) / (sum(totals) / n)


# --- reading -----------------------------------------------------------------------------------------------------------

def read_day(path):
    rows, prices, minutes, windows, types = Counter(), Counter(), Counter(), set(), {}
    with open(path, "rb", buffering=1 << 20) as f:
        for line in f:
            if not workload.is_row(line):
                continue
            p = line.split(b",", TRADING_TIME + 1)
            symbol = p[0]
            rows[symbol] += 1
            if len(p) <= TRADING_TIME:
                continue
            types[symbol] = p[1]
            last = p[LAST].strip()
            if not last:
                continue
            try:
                if float(last) == 0:
                    continue
            except ValueError:
                continue
            prices[symbol] += 1
            t, arrival = workload.clock_s(p[TRADING_TIME]), workload.clock_s(p[3])
            if t is not None:
                windows.add((symbol, int(t) // 300))
            if arrival is not None:
                minutes[symbol, int(arrival) // 60] += 1
    per_symbol = Counter(s for s, _ in windows)
    peaks = {}
    for (s, _), n in minutes.items():
        peaks[s] = max(n, peaks.get(s, 0))
    text = lambda d: {k.decode(): v for k, v in d.items()}
    return text(rows), text(prices), text(per_symbol), text(peaks), {k.decode(): v.decode() for k, v in types.items()}


def read_days(paths, jobs):
    loads = {name: Counter() for name in LOADS}
    peaks, types = {}, {}
    with ProcessPoolExecutor(jobs) as pool:
        for path, (rows, prices, windows, day_peaks, day_types) in zip(paths, pool.map(read_day, paths)):
            print(f"   {path.name}: {sum(rows.values()):,} rows, {sum(prices.values()):,} price events", flush=True)
            for name, counts in zip(LOADS, (rows, prices, windows)):
                loads[name].update(counts)
            for s, n in day_peaks.items():
                peaks[s] = max(n, peaks.get(s, 0))
            types.update(day_types)
    return loads, peaks, types


# --- statistics --------------------------------------------------------------------------------------------------------

def concentration(load):
    """Shares of the most active symbols (of those with any load)."""
    values = sorted((v for v in load.values() if v > 0), reverse=True)
    total, n = sum(values), len(values)
    if not n:
        return None
    cumulative = list(itertools.accumulate(values))
    top = lambda k: cumulative[max(1, k) - 1] / total
    ascending = values[::-1]
    gini = 2 * sum(i * v for i, v in enumerate(ascending, 1)) / (n * total) - (n + 1) / n
    return {"symbols": n, "total": total, "top_1": top(1), "top_10": top(min(10, n)),
            "top_1pct": top(round(n * 0.01)), "top_10pct": top(round(n * 0.10)), "gini": gini,
            "median": values[n // 2],
            "symbols_for": {str(q): next(i for i, c in enumerate(cumulative, 1) if c >= total * q / 100)
                            for q in (50, 80, 90, 99)}}


def exchange(symbol):
    return symbol.rpartition(".")[2]


def analyse(loads, peaks, types):
    def part(load, keep):
        return {s: v for s, v in load.items() if keep(s)}
    groups = {"all": lambda s: True, **{e: (lambda s, e=e: exchange(s) == e) for e in workload.EXCHANGES},
              "equities": lambda s: types.get(s) == "E", "indices": lambda s: types.get(s) == "I"}
    hottest = max(peaks, key=peaks.get, default=None)
    return {
        "concentration": {name: {g: concentration(part(load, keep)) for g, keep in groups.items()}
                          for name, load in loads.items()},
        "top_symbols": {name: load.most_common(10) for name, load in loads.items()},
        "partition_balance": {name: {n: balance(loads[name], partition, n) for n in PARTITIONS}
                              for name in ("rows", "prices") if loads[name]},
        "subtask_balance": {name: {n: balance(loads[name], subtask, n) for n in PARALLELISM}
                            for name in ("prices", "windows") if loads[name]},
        "hottest_symbol_minute": hottest and {"symbol": hottest, "price_events": peaks[hottest],
                                              "per_s": peaks[hottest] / 60},
    }


def summary_md(r, days):
    pct = lambda v: f"{v:.1%}"
    text = f"# Symbol distribution\n\nDays of Nov 2021: {', '.join(days)}. `rows` = all CSV rows, `prices` = usable price " \
           "events, `windows` = (symbol, 5-min window) pairs with a price event.\n\n## Concentration\n\n"
    rows = []
    for name, groups in r["concentration"].items():
        for g, c in groups.items():
            if c:
                rows.append([name, g, f"{c['symbols']:,}", f"{c['total']:,}", pct(c["top_1"]), pct(c["top_10"]),
                             pct(c["top_1pct"]), pct(c["top_10pct"]), f"{c['gini']:.2f}", f"{c['median']:,}",
                             " / ".join(f"{c['symbols_for'][q]:,}" for q in ("50", "80", "90", "99"))])
    text += markdown(["Load", "Symbols of", "Symbols", "Total", "Top symbol", "Top 10", "Top 1%", "Top 10%", "Gini",
                      "Median symbol", "Symbols for 50 / 80 / 90 / 99%"], rows)
    text += "\n## Balance: most loaded / average (1.00 = even)\n\n"
    text += markdown(["Kafka partitions"] + [str(n) for n in PARTITIONS],
                     [[name] + [f"{b[n]:.2f}" for n in PARTITIONS] for name, b in r["partition_balance"].items()])
    text += "\n" + markdown(
        ["Flink keyed subtasks"] + [str(n) for n in PARALLELISM],
        [[name] + [f"{b[n]:.2f}" for n in PARALLELISM] for name, b in r["subtask_balance"].items()]
        + [[f"best speedup for {name}"] + [f"{n / b[n]:.1f}" for n in PARALLELISM]
           for name, b in r["subtask_balance"].items()])
    h = r["hottest_symbol_minute"]
    if h:
        text += (f"\nHottest key: `{h['symbol']}`, {h['price_events']:,} price events in its busiest minute "
                 f"({h['per_s']:,.0f}/s in real time). One symbol is always processed by one subtask.\n")
    text += "\n## Most active symbols\n\n" + markdown(
        ["Rank"] + list(r["top_symbols"]),
        [[i + 1] + [f"{t[i][0]} ({t[i][1]:,})" if i < len(t) else None for t in r["top_symbols"].values()]
         for i in range(10)])
    return text


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--days", default="08,09,10,11,12,13,14", help="comma-separated days of Nov 2021")
    ap.add_argument("--out", type=Path, default=ROOT / "bench" / "results" / "symbol_distribution")
    ap.add_argument("--jobs", type=int, default=min(7, os.cpu_count() or 1), help="files read in parallel")
    a = ap.parse_args()
    days = a.days.split(",")
    paths = [workload.day_file(a.data_dir, d) for d in days]
    for p in paths:
        if not p.is_file():
            sys.exit(f"missing {p}")
    loads, peaks, types = read_days(paths, a.jobs)
    result = analyse(loads, peaks, types)
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "summary.json").write_text(json.dumps({"days": days, **result}, indent=1))
    (a.out / "summary.md").write_text(summary_md(result, days))
    with (a.out / "symbols.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "exchange", "sec_type", *LOADS, "peak_minute_prices"])
        for s, n in loads["rows"].most_common():
            w.writerow([s, exchange(s), types.get(s, ""), n, loads["prices"][s], loads["windows"][s], peaks.get(s, 0)])
    print((a.out / "summary.md").read_text())
    print(f"== written to {a.out}")


if __name__ == "__main__":
    main()
