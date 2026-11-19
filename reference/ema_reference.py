#!/usr/bin/env python3
"""Batch reference implementation of Query 1 (EMA) and Query 2 (crossover advisories).

Written from docs/requirements.md alone, independent of the Flink job, so the two can be compared.

Definitions (requirement IDs from docs/requirements.md):
  price event   a row whose `Last` is non-NULL (FR-9). Rows without `Last` are counted and ignored.
                Zero prices are kept by default (the spec does not exclude them); --exclude-zero-prices drops them.
  event time    (`Trading date`, or `Date` when it is empty) + `Trading time`, as local exchange time (§2.3).
                The system `Time` column is not used: it can lag the trade by hours (e.g. closing auctions).
  window        5-min tumbling, aligned to local midnight (FR-5..7). window_start <= t < window_end.
  close         price of the event with the latest event time in the window; ties go to the row that comes
                later in the input (file order, files in date order) (FR-10). Arrival order is not assumed.
  EMA           EMA_j = close * a_j + EMA_j(prev) * (1 - a_j), a_j = 2 / (1 + j), j in {38, 100} (FR-11),
                previous EMA = 0 before a symbol's first window (FR-12). State persists over days (FR-13).
  late event    the input is a stream in file order (files in date order), and its event-time clock (the
                watermark) is the highest event time seen so far minus --lateness (default 60 s). A window is
                evaluated when the watermark reaches its end (FR-8). An event whose window has already been
                evaluated is late: it is dropped and counted (NFR-7). In this data set every price event is in
                order except one block of 5,506 Paris index rows at the end of 10 Nov, which re-send the day's
                open/high/low with `Trading time` 01:00, 17.9 h late; any bound below 17.9 h gives the same
                result. --lateness inf keeps every event in its event-time window (pure batch semantics).
  empty window  a symbol without a price event in a window gets no output row; its EMAs are not updated and
                no crossover is evaluated (architecture.md §3.5). "Previous window" in FR-15/16 therefore means
                the symbol's previous window that had a price event.
  advisory      BUY  if EMA38 >  EMA100 and prev EMA38 <= prev EMA100 (FR-15)
                SELL if EMA38 <  EMA100 and prev EMA38 >= prev EMA100 (FR-16)
                Window w_i is evaluated when w_{i+1} starts (FR-8), so an advisory is due at event time
                window_end (`due_at`). `initial` marks an advisory whose previous EMAs are still at their
                initial value 0: an artifact of the EMA = 0 initialization (FR-12), since the first positive
                close always gives BUY. This is usually the symbol's first window, but if its first closes
                are 0 (the `Last = 0` records at 00:00), it is the first window with a positive close.

Outputs (in --out, default reference/output/), timestamps are naive local exchange time:
  windows.parquet     one row per (symbol, window) with a price event: close, ticks, EMAs, previous EMAs
  advisories.csv      every BUY/SELL advisory
  summary.json        input statistics and data-quality counts

Usage:
  reference/.venv/bin/python reference/ema_reference.py [--data-dir data] [--out reference/output]
                                                        [--exclude-zero-prices] [--lateness 60] [--jobs 2]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv

WINDOW = pd.Timedelta(minutes=5)
SMOOTHING = (38, 100)
COLS = ["ID", "SecType", "Date", "Last", "Trading time", "Trading date"]
FILE_RE = re.compile(r"debs2022-gc-trading-day-(\d\d)-(\d\d)-(\d\d)\.csv$")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, file=sys.stderr, flush=True)


def day_files(data_dir: Path) -> list[Path]:
    """The daily CSV files, in date order."""
    files = [p for p in data_dir.iterdir() if FILE_RE.search(p.name)]

    def key(p: Path):
        d, m, y = FILE_RE.search(p.name).groups()
        return (y, m, d)

    return sorted(files, key=key)


# --------------------------------------------------------------------------------------------- reading

def read_price_events(path: Path, exclude_zero: bool) -> tuple[pd.DataFrame, dict]:
    """Reads one daily file and returns its price events as (symbol, sec_type, event_time, price, seq).

    `seq` is the row's position in the file and only breaks ties between equal event times.
    """
    header_line, header = None, None
    with open(path, encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if not line.startswith("#"):
                header_line, header = i, [h.strip() for h in line.rstrip("\n").split(",")]
                break
    # Data rows end with a trailing comma: one more field than the header.
    names = header + ["_trailing"]
    invalid = []

    def on_invalid(row):
        invalid.append(row.text[:120])
        return "skip"

    table = pcsv.read_csv(
        path,
        read_options=pcsv.ReadOptions(skip_rows=header_line + 1, column_names=names, block_size=1 << 26),
        parse_options=pcsv.ParseOptions(invalid_row_handler=on_invalid),
        convert_options=pcsv.ConvertOptions(
            include_columns=COLS, column_types={c: pa.string() for c in COLS}, strings_can_be_null=True),
    )
    # Comment lines inside the data (the column-description line under the header) are not events.
    comment = pc.starts_with(table["ID"], "#")
    n_comment = pc.sum(comment).as_py() or 0
    table = table.append_column("seq", pa.array(np.arange(table.num_rows, dtype=np.int64)))
    table = table.filter(pc.invert(comment))
    rows = table.num_rows
    table = table.filter(pc.is_valid(table["Last"]))

    df = table.to_pandas()
    stats = {"file": path.name, "rows": rows, "malformed_rows": len(invalid), "malformed_sample": invalid[:3],
             "comment_rows": n_comment, "price_rows": len(df)}

    price = pd.to_numeric(df["Last"], errors="coerce")
    date = df["Trading date"].fillna(df["Date"])
    day = pd.to_datetime(date, format="%d-%m-%Y", errors="coerce")
    tod = pd.to_timedelta(df["Trading time"], errors="coerce")
    event_time = day + tod

    bad = price.isna() | event_time.isna()
    stats["unparseable_price_rows"] = int(bad.sum())
    stats["zero_price_rows"] = int((price == 0).sum())
    stats["negative_price_rows"] = int((price < 0).sum())
    stats["trading_date_set"] = int(df["Trading date"].notna().sum())
    stats["trading_date_differs_from_date"] = int(
        (df["Trading date"].notna() & (df["Trading date"] != df["Date"])).sum())
    keep = ~bad
    if exclude_zero:
        keep &= price != 0
    out = pd.DataFrame({
        "symbol": df["ID"].to_numpy()[keep],
        "sec_type": df["SecType"].to_numpy()[keep],
        "event_time": event_time[keep].to_numpy(),
        "price": price[keep].to_numpy(),
        "seq": df["seq"].to_numpy()[keep],
    })
    stats["price_events_parsed"] = len(out)
    return out, stats


def windows_of_file(args: tuple[int, Path, bool, float]) -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    """Per-file partial window aggregates: close (latest event time, then latest seq) and tick count.

    Late events are dropped here using a watermark that starts at this file's first row. That equals the
    watermark of one stream over all files only if no earlier file could have made an event of this file
    late; main() checks that (check_file_boundaries).
    """
    file_idx, path, exclude_zero, lateness_s = args
    t0 = time.time()
    ev, stats = read_price_events(path, exclude_zero)  # in file order
    ev["window_start"] = ev["event_time"].dt.floor(WINDOW)
    ev["file_idx"] = file_idx

    # Disorder statistics (dataset analysis): events whose event time is earlier than an earlier row's of
    # the same symbol in file order, and by how much.
    behind = (ev.groupby("symbol")["event_time"].cummax() - ev["event_time"]).dt.total_seconds()
    behind = behind[behind > 0]
    stats["out_of_order_events"] = int(len(behind))
    stats["out_of_order_max_s"] = float(behind.max()) if len(behind) else 0.0
    stats["out_of_order_p99_s"] = float(behind.quantile(0.99)) if len(behind) else 0.0
    stats["event_time_min"] = str(ev["event_time"].min())
    stats["event_time_max"] = str(ev["event_time"].max())

    # Late events: the window ended at or before the watermark (max event time seen before - lateness).
    if np.isfinite(lateness_s):
        watermark = ev["event_time"].cummax().shift() - pd.Timedelta(seconds=lateness_s)
        late = (ev["window_start"] + WINDOW) <= watermark
        dropped = ev[late]
        stats["late_events_dropped"] = int(late.sum())
        stats["late_symbols"] = int(dropped["symbol"].nunique())
        stats["late_sample"] = dropped.head(3).astype(str).to_dict("records")
        ev = ev[~late]
    else:
        stats["late_events_dropped"] = 0
    stats["price_events_used"] = len(ev)

    ev = ev.sort_values(["symbol", "window_start", "event_time", "seq"], kind="stable")
    g = ev.groupby(["symbol", "window_start"], sort=False)
    agg = g.agg(close=("price", "last"), close_time=("event_time", "last"), close_seq=("seq", "last"),
                ticks=("price", "size")).reset_index()
    agg["file_idx"] = file_idx
    stats["windows"] = len(agg)
    stats["seconds"] = round(time.time() - t0, 1)
    sec_types = ev.drop_duplicates("symbol")[["symbol", "sec_type"]]
    log(f"{path.name}: {stats['rows']:,} rows, {stats['price_events_used']:,} price events, "
        f"{stats['windows']:,} windows ({stats['seconds']} s)")
    return agg, stats, sec_types


def check_file_boundaries(file_stats, lateness_s: float):
    """Late events are decided per file; that is exact if, for every file, the watermark left by the earlier
    files (their max event time - lateness) is before the end of the file's earliest window."""
    prev_max = None
    for s in file_stats:
        if s["event_time_min"] == "NaT":
            continue
        lo, hi = pd.Timestamp(s["event_time_min"]), pd.Timestamp(s["event_time_max"])
        if prev_max is not None and np.isfinite(lateness_s):
            if prev_max - pd.Timedelta(seconds=lateness_s) >= lo.floor(WINDOW) + WINDOW:
                sys.exit(f"{s['file']}: events could be late relative to earlier files; the per-file late-event "
                         f"check is not exact for this input")
        prev_max = hi if prev_max is None else max(prev_max, hi)


# --------------------------------------------------------------------------------------------- query 1 + 2

def merge_partials(parts: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines per-file aggregates; a window that spans files keeps the latest close and sums the ticks."""
    w = pd.concat(parts, ignore_index=True)
    w = w.sort_values(["symbol", "window_start", "close_time", "file_idx", "close_seq"], kind="stable")
    g = w.groupby(["symbol", "window_start"], sort=True)
    out = g.agg(close=("close", "last"), close_time=("close_time", "last"), ticks=("ticks", "sum")).reset_index()
    out["window_end"] = out["window_start"] + WINDOW
    return out


def ema_and_crossovers(w: pd.DataFrame) -> pd.DataFrame:
    """Adds EMA38/EMA100 and their previous values per symbol, and BUY/SELL advisories.

    `w` must be sorted by (symbol, window_start). The recursion is evaluated exactly as written in FR-11:
    close * a + prev * (1 - a), so results can be compared with a streaming implementation bit for bit.
    """
    closes = w["close"].to_numpy(dtype=np.float64)
    first = (w["symbol"] != w["symbol"].shift()).to_numpy()
    res = {}
    for j in SMOOTHING:
        a = 2.0 / (1 + j)
        b = 1.0 - a
        ema = np.empty_like(closes)
        prev = np.empty_like(closes)
        e = 0.0
        for i in range(len(closes)):  # 8-9 M iterations; plain floats are fast enough
            if first[i]:
                e = 0.0
            prev[i] = e
            e = closes[i] * a + e * b
            ema[i] = e
        res[j] = (ema, prev)
    w = w.copy()
    w["ema38"], w["prev_ema38"] = res[38]
    w["ema100"], w["prev_ema100"] = res[100]
    buy = (w["ema38"] > w["ema100"]) & (w["prev_ema38"] <= w["prev_ema100"])
    sell = (w["ema38"] < w["ema100"]) & (w["prev_ema38"] >= w["prev_ema100"])
    w["advisory"] = pd.Series(np.where(buy, "BUY", np.where(sell, "SELL", "")), index=w.index).astype("string")
    w.loc[w["advisory"] == "", "advisory"] = pd.NA
    w["first_window"] = first
    return w


def advisories(w: pd.DataFrame) -> pd.DataFrame:
    a = w[w["advisory"].notna()]
    return pd.DataFrame({
        "symbol": a["symbol"],
        "type": a["advisory"],
        "window_start": a["window_start"],
        "window_end": a["window_end"],
        "due_at": a["window_end"],  # FR-8: w_i is evaluated when w_{i+1} starts
        "close": a["close"],
        "ema38": a["ema38"],
        "ema100": a["ema100"],
        "prev_ema38": a["prev_ema38"],
        "prev_ema100": a["prev_ema100"],
        "initial": (a["prev_ema38"] == 0) & (a["prev_ema100"] == 0),
    }).reset_index(drop=True)


# --------------------------------------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--out", type=Path, default=Path("reference/output"))
    ap.add_argument("--files", nargs="*", help="only these file names (default: every daily file)")
    ap.add_argument("--exclude-zero-prices", action="store_true", help="treat Last = 0 as not a price event")
    ap.add_argument("--lateness", type=float, default=60.0,
                    help="watermark delay in seconds (default 60); 'inf' keeps late events")
    ap.add_argument("--jobs", type=int, default=2, help="files read in parallel (each needs ~6 GB of RAM)")
    args = ap.parse_args(argv)

    files = day_files(args.data_dir)
    if args.files:
        files = [f for f in files if f.name in set(args.files)]
    if not files:
        sys.exit(f"no debs2022-gc-trading-day-*.csv files in {args.data_dir}")
    args.out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    tasks = [(i, f, args.exclude_zero_prices, args.lateness) for i, f in enumerate(files)]
    with ProcessPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        results = list(pool.map(windows_of_file, tasks))
    parts, file_stats, sec_types = zip(*results)
    check_file_boundaries(file_stats, args.lateness)

    w = merge_partials(list(parts))
    sec = pd.concat(sec_types).drop_duplicates("symbol").set_index("symbol")["sec_type"]
    w.insert(1, "sec_type", w["symbol"].map(sec))
    log(f"{len(w):,} windows for {w['symbol'].nunique():,} symbols; computing EMAs")
    w = ema_and_crossovers(w)
    adv = advisories(w)

    cols = ["symbol", "sec_type", "window_start", "window_end", "close", "close_time", "ticks",
            "ema38", "ema100", "prev_ema38", "prev_ema100", "advisory", "first_window"]
    w[cols].to_parquet(args.out / "windows.parquet", index=False)
    adv.to_csv(args.out / "advisories.csv", index=False, float_format="%.10g")

    summary = {
        "exclude_zero_prices": args.exclude_zero_prices,
        "lateness_s": args.lateness if np.isfinite(args.lateness) else "inf",
        "files": list(file_stats),
        "totals": {
            "rows": sum(s["rows"] for s in file_stats),
            "price_events_used": sum(s["price_events_used"] for s in file_stats),
            "zero_price_rows": sum(s["zero_price_rows"] for s in file_stats),
            "out_of_order_events": sum(s["out_of_order_events"] for s in file_stats),
            "late_events_dropped": sum(s["late_events_dropped"] for s in file_stats),
            "symbols": int(w["symbol"].nunique()),
            "windows": len(w),
            "windows_with_zero_close": int((w["close"] == 0).sum()),
            "advisories": len(adv),
            "buy": int((adv["type"] == "BUY").sum()),
            "sell": int((adv["type"] == "SELL").sum()),
            "initial_advisories": int(adv["initial"].sum()),
            "seconds": round(time.time() - t0, 1),
        },
    }
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    log(json.dumps(summary["totals"]))


if __name__ == "__main__":
    main()
