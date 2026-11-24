"""Unit tests for ema_reference.py on small hand-made CSV files.

Run: reference/.venv/bin/python -m pytest reference -q
"""
import numpy as np
import pandas as pd
import pytest

import ema_reference as ref

HEADER = ("ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,"
          "Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,"
          "Nominal value,Last,Last volume,Trading time,Total volume,Mid price,Trading date,Profit,Current price,"
          "Related indices,Day high bid time, Day low bid time,Open Time, Last trade time, Close Time,"
          " Day high Time, Day low Time,Bid time,Auction Time")


def row(sym, date, ttime, last=None, trading_date=None, sec="E", systime="12:00:00.000"):
    f = [""] * 40  # 39 columns + the trailing comma of the real files
    f[0], f[1], f[2], f[3] = sym, sec, date, systime
    if last is not None:
        f[21] = f"{last:.6f}"
        f[23] = ttime
    if trading_date:
        f[26] = trading_date
    return ",".join(f)


def write_day(tmp_path, day, rows):
    name = f"debs2022-gc-trading-day-{day}.csv"
    lines = ["# comment", "#", HEADER, "#'Unique ID','Security Type'"] + rows
    (tmp_path / name).write_text("\n".join(lines) + "\n")
    return name


def run(tmp_path, exclude_zero=False, lateness=None):
    out = tmp_path / "out"
    argv = ["--data-dir", str(tmp_path), "--out", str(out), "--jobs", "1"]
    if lateness is not None:
        argv += ["--lateness", str(lateness)]
    if exclude_zero:
        argv.append("--exclude-zero-prices")
    ref.main(argv)
    w = pd.read_parquet(out / "windows.parquet")
    adv = pd.read_csv(out / "advisories.csv", parse_dates=["window_start", "window_end", "due_at"])
    return w, adv


def ts(s):
    return pd.Timestamp(s)


def test_close_is_latest_event_time_not_file_order(tmp_path):
    write_day(tmp_path, "08-11-21", [
        row("A.FR", "08-11-2021", "10:01:00.000", 2.0),
        row("A.FR", "08-11-2021", "10:00:30.000", 1.0),   # arrives later, happened earlier
        row("A.FR", "08-11-2021", "10:02:00.000"),         # no Last: not a price event
        row("A.FR", "08-11-2021", "10:01:00.000", 3.0),   # tie on event time: later row wins
    ])
    w, _ = run(tmp_path)
    assert len(w) == 1
    assert w.loc[0, "close"] == 3.0
    assert w.loc[0, "ticks"] == 3
    assert w.loc[0, "window_start"] == ts("2021-11-08 10:00")
    assert w.loc[0, "window_end"] == ts("2021-11-08 10:05")


def test_late_events_dropped_after_window_evaluated(tmp_path):
    write_day(tmp_path, "08-11-21", [
        row("A.FR", "08-11-2021", "10:00:10.000", 1.0),
        row("B.FR", "08-11-2021", "10:05:30.000", 2.0),
        row("A.FR", "08-11-2021", "10:04:00.000", 3.0),   # watermark 10:04:30 < 10:05: still on time
        row("B.FR", "08-11-2021", "10:06:00.000", 4.0),   # watermark -> 10:05:00: window 10:00-10:05 evaluated
        row("A.FR", "08-11-2021", "10:04:50.000", 5.0),   # late: dropped
        row("A.FR", "08-11-2021", "10:05:10.000", 6.0),   # its window is still open
    ])
    w, _ = run(tmp_path, lateness=60)
    a = w[w.symbol == "A.FR"]
    assert a["close"].tolist() == [3.0, 6.0]
    assert a["ticks"].tolist() == [2, 1]
    w, _ = run(tmp_path, lateness="inf")
    assert w[w.symbol == "A.FR"]["close"].tolist() == [5.0, 6.0]


def test_window_boundaries_aligned_to_midnight(tmp_path):
    write_day(tmp_path, "08-11-21", [
        row("A.FR", "08-11-2021", "00:00:00.000", 1.0),
        row("A.FR", "08-11-2021", "00:04:59.999", 2.0),
        row("A.FR", "08-11-2021", "00:05:00.000", 3.0),   # starts w1
    ])
    w, _ = run(tmp_path)
    assert list(w["window_start"]) == [ts("2021-11-08 00:00"), ts("2021-11-08 00:05")]
    assert list(w["close"]) == [2.0, 3.0]


def test_trading_date_takes_precedence_over_system_date(tmp_path):
    write_day(tmp_path, "09-11-21", [
        row("A.FR", "09-11-2021", "23:58:00.000", 5.0, trading_date="08-11-2021"),
    ])
    w, _ = run(tmp_path)
    assert w.loc[0, "window_start"] == ts("2021-11-08 23:55")


def test_window_spanning_two_files_is_merged(tmp_path):
    write_day(tmp_path, "08-11-21", [row("A.FR", "08-11-2021", "23:56:00.000", 1.0)])
    write_day(tmp_path, "09-11-21", [
        row("A.FR", "09-11-2021", "23:57:00.000", 2.0, trading_date="08-11-2021"),
        row("A.FR", "09-11-2021", "00:01:00.000", 3.0),
    ])
    w, _ = run(tmp_path)
    assert list(w["window_start"]) == [ts("2021-11-08 23:55"), ts("2021-11-09 00:00")]
    assert list(w["close"]) == [2.0, 3.0]
    assert list(w["ticks"]) == [2, 1]


def test_ema_recursion_and_state_per_symbol(tmp_path):
    write_day(tmp_path, "08-11-21", [
        row("A.FR", "08-11-2021", "10:00:00.000", 10.0),
        row("B.NL", "08-11-2021", "10:00:00.000", 100.0, sec="I"),
        row("A.FR", "08-11-2021", "10:05:00.000", 20.0),
        row("A.FR", "08-11-2021", "10:30:00.000", 30.0),   # windows 10:10..10:25 empty: skipped
    ])
    w, _ = run(tmp_path)
    a = w[w.symbol == "A.FR"].reset_index(drop=True)
    assert len(a) == 3

    def expected(closes, j):
        e, out = 0.0, []
        for c in closes:
            e = c * (2 / (1 + j)) + e * (1 - 2 / (1 + j))
            out.append(e)
        return out

    assert a["ema38"].tolist() == expected([10, 20, 30], 38)
    assert a["ema100"].tolist() == expected([10, 20, 30], 100)
    assert a["prev_ema38"].tolist() == [0.0] + expected([10, 20], 38)
    b = w[w.symbol == "B.NL"].reset_index(drop=True)
    assert b.loc[0, "ema38"] == 100.0 * (2 / 39)  # B does not inherit A's state
    assert b.loc[0, "sec_type"] == "I"


def test_ema_matches_pandas_ewm():
    rng = np.random.default_rng(1)
    closes = 100 + rng.standard_normal(500).cumsum()
    w = pd.DataFrame({"symbol": ["X"] * 500, "close": closes})
    got = ref.ema_and_crossovers(w)
    for j in (38, 100):
        # EMA with initial value 0: prepend a 0 and use the recursive (adjust=False) form
        want = pd.Series(np.r_[0.0, closes]).ewm(alpha=2 / (1 + j), adjust=False).mean().to_numpy()[1:]
        np.testing.assert_allclose(got[f"ema{j}"].to_numpy(), want, rtol=1e-12)


def test_crossovers(tmp_path):
    # First window: EMA38 = 2c/39 > EMA100 = 2c/101 with prev both 0 -> initial BUY.
    # Then a crash: EMA38 reacts faster and falls below EMA100 -> SELL. Then a rally -> BUY.
    prices = [100.0] * 60 + [1.0] * 30 + [1000.0] * 30
    start = ts("2021-11-08 00:00")
    rows = []
    for i, p in enumerate(prices):
        t = start + i * pd.Timedelta(minutes=5)
        rows.append(row("A.FR", "08-11-2021", t.strftime("%H:%M:%S.000"), p))
    write_day(tmp_path, "08-11-21", rows)
    w, adv = run(tmp_path)
    assert adv["type"].tolist() == ["BUY", "SELL", "BUY"]
    assert adv["initial"].tolist() == [True, False, False]
    assert (adv["due_at"] == adv["window_end"]).all()
    # every advisory satisfies the FR-15/16 conditions, and no other window does
    buy = (w.ema38 > w.ema100) & (w.prev_ema38 <= w.prev_ema100)
    sell = (w.ema38 < w.ema100) & (w.prev_ema38 >= w.prev_ema100)
    assert (buy | sell).sum() == len(adv)
    # window indexes 69 and 90, computed by hand from the FR-11 recursion
    assert adv["window_start"].tolist()[1:] == [start + 69 * pd.Timedelta(minutes=5),
                                                start + 90 * pd.Timedelta(minutes=5)]


def test_zero_prices_kept_by_default_and_excludable(tmp_path):
    write_day(tmp_path, "08-11-21", [
        row("A.FR", "08-11-2021", "00:00:00.000", 0.0),
        row("A.FR", "08-11-2021", "10:00:00.000", 5.0),
    ])
    w, adv = run(tmp_path)
    assert w["close"].tolist() == [0.0, 5.0]
    # the first positive close follows a zero close: still the EMA = 0 initialization artifact
    assert adv["type"].tolist() == ["BUY"] and adv["initial"].tolist() == [True]
    w, adv = run(tmp_path, exclude_zero=True)
    assert w["close"].tolist() == [5.0]
    assert adv["initial"].tolist() == [True]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
