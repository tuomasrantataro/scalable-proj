#!/usr/bin/env python3
"""Event types of the data set: which rows are prices, quotes, housekeeping or empty, per exchange and security type.
Reads the CSV files only (no Docker).

The type is not marked in the data; it follows from the attributes that are set (project description, section 2.2). Every
row counts once:
  price         `Last` is set (also if ask or bid is)
  quote         ask or bid price or volume is set, `Last` is not
  housekeeping  only other attributes are set (day's high and low, open, close, times, ...)
  no payload    nothing but ID, SecType, Date and Time

Also: zero prices, how often each attribute is set, and the totals next to those of the project description.

Writes <out>/summary.md and summary.json.

Usage (repo root):
  python3 bench/event_types.py
  python3 bench/event_types.py --days 08
"""
import argparse
import json
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import workload
from backlog import ROOT
from measure import markdown, number

KEYS = 4                        # ID, SecType, Date, Time
QUOTE = (4, 5, 6, 7)            # Ask, Ask volume, Bid, Bid volume
LAST = workload.LAST
TYPES = ("price", "quote", "housekeeping", "no payload")
# Project description, section 2.1 (rows in millions, or share of the rows)
GIVEN = (("rows", "289 M"), ("rows FR / NL / ETR", "55% / 28% / 17%"), ("rows of equities / indices", "239.5 M / 49.5 M"),
         ("price events of equities / indices", "10 M / 49 M"), ("symbols", "5,504"))


def event_type(flags):
    """Type of a row from which of its fields are set."""
    if len(flags) > LAST and flags[LAST]:
        return "price"
    if any(flags[i] for i in QUOTE if i < len(flags)):
        return "quote"
    return "housekeeping" if any(flags[KEYS:]) else "no payload"


def read_day(path):
    """(Counter of (exchange, sec type, set fields), Counter of zero prices per (exchange, sec type), symbols, header)"""
    patterns, zeros, symbols, header = Counter(), Counter(), set(), []
    with open(path, "rb", buffering=1 << 20) as f:
        for line in f:
            if not workload.is_row(line):
                if line.startswith(b"ID,"):
                    header = [h.strip() for h in line.decode().strip().split(",")]
                continue
            p = line.rstrip().split(b",")
            symbols.add(p[0])
            kind = (p[0].rpartition(b".")[2], p[1] if len(p) > 1 else b"")
            patterns[kind, tuple(map(bool, p))] += 1
            if len(p) > LAST and p[LAST]:
                try:
                    zeros[kind] += float(p[LAST]) == 0
                except ValueError:
                    pass
    text = lambda k: (k[0].decode(), k[1].decode())
    return ({(text(k), flags): n for (k, flags), n in patterns.items()}, {text(k): n for k, n in zeros.items()},
            symbols, header)


def analyse(days):
    counts, zeros, attributes, symbols, header, per_day = Counter(), Counter(), Counter(), set(), [], {}
    for day, (patterns, day_zeros, day_symbols, day_header) in days.items():
        per_day[day] = Counter()
        for (kind, flags), n in patterns.items():
            t = event_type(flags)
            counts[kind, t] += n
            per_day[day][t] += n
            for i, is_set in enumerate(flags):
                if is_set:
                    attributes[i] += n
        zeros.update(day_zeros)
        symbols |= day_symbols
        header = header or day_header
    return {"counts": counts, "zeros": zeros, "attributes": attributes, "symbols": len(symbols), "header": header,
            "per_day": per_day}


def of(counts, t=None, exchange=None, sec=None):
    return sum(n for ((e, s), kind), n in counts.items()
               if t in (None, kind) and exchange in (None, e) and sec in (None, s))


def summary_md(r, days):
    c = r["counts"]
    total = of(c)
    pct = lambda n, d: f"{n / d:.1%}" if d else "–"
    groups = [("all", {})] + [(e, {"exchange": e}) for e in workload.EXCHANGES] + \
             [("equities", {"sec": "E"}), ("indices", {"sec": "I"})]
    text = f"# Event types\n\nDays of Nov 2021: {', '.join(days)}. Every row counts once, by the attributes that are set.\n\n"
    rows = []
    for name, g in groups:
        n = of(c, **g)
        rows.append([name, number(n), pct(n, total)]
                    + [f"{of(c, t, **g):,} ({pct(of(c, t, **g), n)})" for t in TYPES]
                    + [number(sum(z for (e, s), z in r["zeros"].items()
                                  if g.get("exchange") in (None, e) and g.get("sec") in (None, s)))])
    text += markdown(["Rows of", "Rows", "Share", *TYPES, "zero prices"], rows)
    text += "\n## Per day\n\n" + markdown(["Day", "Rows", *TYPES], [
        [d, number(sum(v.values())), *(number(v[t]) for t in TYPES)] for d, v in r["per_day"].items()])
    share = lambda e: f"{of(c, exchange=e) / total:.0%}" if total else "–"
    million = lambda n: f"{n / 1e6:.1f} M"
    found = (million(total), " / ".join(share(e) for e in workload.EXCHANGES),
             f"{million(of(c, sec='E'))} / {million(of(c, sec='I'))}",
             f"{million(of(c, 'price', sec='E'))} / {million(of(c, 'price', sec='I'))}", f"{r['symbols']:,}")
    text += "\n## Compared with the project description\n\n" + markdown(
        ["", "Description", "Files"], [[name, given, f] for (name, given), f in zip(GIVEN, found)])
    text += "\n## Attributes that are set\n\n" + markdown(["Attribute", "Rows", "Share"], [
        [r["header"][i] if i < len(r["header"]) else f"field {i + 1}", number(n), pct(n, total)]
        for i, n in sorted(r["attributes"].items())])
    return text


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--days", default="08,09,10,11,12,13,14", help="comma-separated days of Nov 2021")
    ap.add_argument("--out", type=Path, default=ROOT / "bench" / "results" / "event_types")
    ap.add_argument("--jobs", type=int, default=min(5, os.cpu_count() or 1), help="files read in parallel")
    a = ap.parse_args()
    days = a.days.split(",")
    paths = [workload.day_file(a.data_dir, d) for d in days]
    for p in paths:
        if not p.is_file():
            sys.exit(f"missing {p}")
    with ProcessPoolExecutor(a.jobs) as pool:
        r = analyse(dict(zip(days, pool.map(read_day, paths))))
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "summary.json").write_text(json.dumps({
        "days": days, "symbols": r["symbols"],
        "rows": [{"exchange": e, "sec_type": s, "type": t, "rows": n} for ((e, s), t), n in sorted(r["counts"].items())],
        "zero_prices": [{"exchange": e, "sec_type": s, "rows": n} for (e, s), n in sorted(r["zeros"].items())]}, indent=1))
    (a.out / "summary.md").write_text(summary_md(r, days))
    print((a.out / "summary.md").read_text())
    print(f"== written to {a.out}")


if __name__ == "__main__":
    main()
