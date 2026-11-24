#!/usr/bin/env python3
"""Compares the Flink job's results with the pandas reference (NFR-9): windows, closes, EMAs and advisories.

Reads table `ema_window` from TimescaleDB (or a file written with --save-export) and `windows.parquet` of
ema_reference.py. The job excludes zero prices, so the matching reference is the run with --exclude-zero-prices.

Checks, each of which must hold:
  windows     the same (symbol, window end) pairs on both sides
  values      ticks, close, EMA38 and EMA100 of every window are the same bit for bit (the largest difference is shown)
  advisories  the same BUY/SELL per window, and the same `initial` marks
  digest      the md5 of the two results in the export format of bench/backlog.py is equal (its `WEEK_DIGEST`)

With --through, windows that the job could not evaluate yet are no difference: the input ended before the watermark
(last event time - --lateness) passed their end, and the next day's events would close them. They are counted and
left out, but only if the job has no row for them.

Exit code 0 = identical, 1 = different.

Usage (repo root; TimescaleDB running, with the results of a run):
  reference/.venv/bin/python reference/compare.py                        # whole week
  reference/.venv/bin/python reference/compare.py --through 2021-11-08   # only day 1 was processed
  reference/.venv/bin/python reference/compare.py --save-export out.csv  # also keep the job's results
  reference/.venv/bin/python reference/compare.py --export out.csv       # compare a saved export, no database
  reference/.venv/bin/python reference/compare.py --reference-digest     # only the digest of the reference
"""
from __future__ import annotations

import argparse
import hashlib
import io
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
COLUMNS = ["symbol", "window_end", "close", "ticks", "ema38", "ema100", "advisory", "initial"]
EXPORT = (f"\\copy (SELECT {', '.join(COLUMNS)} FROM ema_window ORDER BY symbol, window_end) TO STDOUT CSV")


def load_reference(path: Path, through: str | None) -> pd.DataFrame:
    w = pd.read_parquet(path)
    w["initial"] = w["advisory"].notna() & (w["prev_ema38"] == 0) & (w["prev_ema100"] == 0)
    w["advisory"] = w["advisory"].fillna("").astype(str)
    w["symbol"] = w["symbol"].astype(str)
    return restrict(w[[*COLUMNS, "close_time"]], through)


def restrict(w: pd.DataFrame, through: str | None) -> pd.DataFrame:
    """Windows that start on or before the day `through`, in the order of the export (symbols by byte value)."""
    if through:
        start = w["window_end"] - pd.Timedelta(minutes=5)
        w = w[start < pd.Timestamp(through) + pd.Timedelta(days=1)]
    return w.sort_values(["symbol", "window_end"], ignore_index=True)


def export_from_database() -> bytes:
    r = subprocess.run(["docker", "compose", "exec", "-T", "timescaledb", "psql", "-U", "postgres", "-d", "trading",
                        "-c", EXPORT], cwd=ROOT, capture_output=True)
    if r.returncode != 0:
        sys.exit(f"reading ema_window failed: {r.stderr.decode()}")
    return r.stdout


def parse_export(data: bytes) -> pd.DataFrame:
    w = pd.read_csv(io.BytesIO(data), names=COLUMNS, keep_default_na=False,
                    dtype={"symbol": str, "window_end": str, "close": float, "ticks": "int64", "ema38": float,
                           "ema100": float, "advisory": str, "initial": str})
    # market times are local exchange time stored as if UTC
    w["window_end"] = pd.to_datetime(w["window_end"].str.removesuffix("+00"), format="%Y-%m-%d %H:%M:%S")
    w["initial"] = w["initial"] == "t"
    return w


def number(x: float) -> str:
    """Shortest decimal that reads back as the same double, as PostgreSQL prints it for prices and EMAs."""
    s = repr(float(x))
    return s.removesuffix(".0")


def export_digest(w: pd.DataFrame) -> str:
    """md5 of the rows as PostgreSQL's COPY ... CSV writes them."""
    h = hashlib.md5()
    for s, e, c, t, a, b, adv, ini in w[COLUMNS].itertuples(index=False, name=None):
        h.update(f"{s},{e:%Y-%m-%d %H:%M:%S}+00,{number(c)},{t},{number(a)},{number(b)},{adv},{'t' if ini else 'f'}\n"
                 .encode())
    return h.hexdigest()


def drop_open(ref: pd.DataFrame, job: pd.DataFrame, last_event: pd.Timestamp, lateness_s: float):
    """The reference without the windows still open at the end of the input that the job has no row for, and their
    number. A window is open if its end is after the final watermark."""
    still_open = ref["window_end"] > last_event - pd.Timedelta(seconds=lateness_s)
    keys = pd.MultiIndex.from_frame(job[["symbol", "window_end"]])
    missing = ~pd.MultiIndex.from_frame(ref[["symbol", "window_end"]]).isin(keys)
    skip = still_open.to_numpy() & missing
    return ref[~skip].reset_index(drop=True), int(skip.sum())


def compare(job: pd.DataFrame, ref: pd.DataFrame) -> tuple[list[tuple[str, bool, str]], pd.DataFrame]:
    """[(check, passed, detail)] and the rows that differ."""
    m = job.merge(ref, on=["symbol", "window_end"], how="outer", suffixes=("_job", "_ref"), indicator=True)
    missing, extra = int((m["_merge"] == "right_only").sum()), int((m["_merge"] == "left_only").sum())
    both = m[m["_merge"] == "both"]
    checks = [("rows unique", not job.duplicated(["symbol", "window_end"]).any(), "no (symbol, window) twice"),
              ("windows", missing == 0 and extra == 0,
               f"{len(both):,} in both, {missing:,} only in the reference, {extra:,} only in the job")]
    different = m["_merge"] != "both"
    for col in ("ticks", "close", "ema38", "ema100"):
        a, b = both[f"{col}_job"].to_numpy(float), both[f"{col}_ref"].to_numpy(float)
        off = a != b
        with np.errstate(divide="ignore", invalid="ignore"):
            rel = np.where(off, np.abs(a - b) / np.abs(b), 0.0)
        checks.append((col, not off.any(), f"{int(off.sum()):,} differ" + (
            f", largest absolute {np.abs(a - b).max():.3g}, relative {np.nanmax(rel):.3g}" if off.any() else "")))
        different |= pd.Series(off, index=both.index).reindex(m.index, fill_value=False)
    for col in ("advisory", "initial"):
        off = both[f"{col}_job"].astype(object) != both[f"{col}_ref"].astype(object)
        counts = ", ".join(f"{k} {v:,}" for k, v in ref[col].value_counts().sort_index().items() if k not in ("", False))
        checks.append((col, not off.any(), f"{int(off.sum()):,} differ; reference: {counts.replace('True', 'marked')}"))
        different |= off.reindex(m.index, fill_value=False)
    return checks, m[different]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reference", type=Path, default=ROOT / "reference" / "output-nozero" / "windows.parquet")
    ap.add_argument("--through", metavar="YYYY-MM-DD", help="compare only windows up to this day")
    ap.add_argument("--export", type=Path, help="read the job's results from this file instead of the database")
    ap.add_argument("--save-export", type=Path, help="write the job's results to this file")
    ap.add_argument("--reference-digest", action="store_true", help="only print the digest of the reference")
    ap.add_argument("--show", type=int, default=10, help="differing rows to print")
    ap.add_argument("--lateness", type=float, default=1, help="OUT_OF_ORDERNESS_S of the job (with --through)")
    a = ap.parse_args(argv)
    if not a.reference.is_file():
        sys.exit(f"missing {a.reference}: run reference/ema_reference.py --exclude-zero-prices --out reference/output-nozero")
    ref = load_reference(a.reference, a.through)
    ref_digest = export_digest(ref)
    if a.reference_digest:
        print(ref_digest)
        return 0
    data = a.export.read_bytes() if a.export else export_from_database()
    if a.save_export:
        a.save_export.write_bytes(data)
    job = restrict(parse_export(data), a.through)
    if job.empty:
        sys.exit("the job has no results (ema_window is empty)")
    open_windows = 0
    if a.through:
        ref, open_windows = drop_open(ref, job, ref["close_time"].max(), a.lateness)
        ref_digest = export_digest(ref)
    checks, rows = compare(job, ref[COLUMNS])
    # the raw bytes when nothing was filtered, so the digest is the one bench/backlog.py reports
    job_digest = export_digest(job) if a.through else hashlib.md5(data).hexdigest()
    checks.append(("digest", job_digest == ref_digest, f"job {job_digest}, reference {ref_digest}"))
    if open_windows:
        print(f"note windows      {open_windows:,} still open at the end of the input, left out")
    for name, ok, detail in checks:
        print(f"{'ok  ' if ok else 'FAIL'} {name:12} {detail}")
    if len(rows):
        print(f"\n{len(rows):,} differing rows, the first {min(a.show, len(rows))}:")
        print(rows.head(a.show).to_string(index=False))
    same = all(ok for _, ok, _ in checks)
    print("identical" if same else "DIFFERENT")
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
