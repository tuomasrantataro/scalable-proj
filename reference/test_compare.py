"""Unit tests for compare.py. Run: reference/.venv/bin/python -m pytest reference -q"""
import hashlib

import pandas as pd
import pytest

import compare as c

CSV = (b"A.FR,2021-11-08 09:05:00+00,19.68,2,1.0092307692307692,0.3897029702970297,BUY,t\n"
       b"A.FR,2021-11-08 09:10:00+00,20,1,1.9,0.8,,f\n"
       b"B.NL,2021-11-09 00:00:00+00,5,3,0.25,0.1,BUY,f\n")


def reference(tmp_path, **change):
    w = pd.DataFrame({
        "symbol": ["A.FR", "A.FR", "B.NL"],
        "window_end": pd.to_datetime(["2021-11-08 09:05", "2021-11-08 09:10", "2021-11-09 00:00"]),
        "close_time": pd.to_datetime(["2021-11-08 09:04", "2021-11-08 09:09", "2021-11-08 23:58"]),
        "close": [19.68, 20.0, 5.0], "ticks": [2, 1, 3], "ema38": [1.0092307692307692, 1.9, 0.25],
        "ema100": [0.3897029702970297, 0.8, 0.1], "prev_ema38": [0.0, 1.0, 0.2], "prev_ema100": [0.0, 0.4, 0.3],
        "advisory": pd.array(["BUY", None, "BUY"], dtype="string")})
    for col, (row, value) in change.items():
        w.loc[row, col] = value
    path = tmp_path / "windows.parquet"
    w.to_parquet(path)
    return path


def run(tmp_path, ref, csv=CSV, *args):
    export = tmp_path / "export.csv"
    export.write_bytes(csv)
    return c.main(["--reference", str(ref), "--export", str(export), *args])


def test_identical_results_and_digest(tmp_path, capsys):
    assert run(tmp_path, reference(tmp_path)) == 0
    assert hashlib.md5(CSV).hexdigest() in capsys.readouterr().out


@pytest.mark.parametrize("change", [
    {"ema38": (1, 1.9000000000000001 + 4e-16)}, {"close": (0, 19.69)}, {"ticks": (2, 4)},
    {"advisory": (1, "SELL")}, {"prev_ema38": (2, 0.0), "prev_ema100": (2, 0.0)}])
def test_any_difference_fails(tmp_path, change):
    assert run(tmp_path, reference(tmp_path, **change)) == 1


def test_missing_extra_and_duplicate_rows_fail(tmp_path):
    ref, lines = reference(tmp_path), CSV.splitlines(keepends=True)
    assert run(tmp_path, ref, b"".join(lines[:2])) == 1
    assert run(tmp_path, ref, CSV + b"C.FR,2021-11-08 09:05:00+00,1,1,1,1,,f\n") == 1
    assert run(tmp_path, ref, CSV + lines[0]) == 1


def test_through_keeps_the_last_window_of_the_day(tmp_path):
    ref, lines = reference(tmp_path), CSV.splitlines(keepends=True)
    assert len(c.load_reference(ref, "2021-11-08")) == 3      # the window ending at midnight started on the 8th
    assert len(c.load_reference(ref, "2021-11-07")) == 0
    assert run(tmp_path, ref, CSV, "--through", "2021-11-08") == 0
    assert run(tmp_path, ref, b"".join(lines[1:]), "--through", "2021-11-08") == 1


def test_windows_still_open_at_the_end_of_a_partial_run_are_left_out(tmp_path, capsys):
    ref, lines = reference(tmp_path), CSV.splitlines(keepends=True)
    # the last event is at 23:58, so the watermark (23:57) never passed the end of B.NL's window
    assert run(tmp_path, ref, b"".join(lines[:2]), "--through", "2021-11-08") == 0
    assert "1 still open" in capsys.readouterr().out
    assert run(tmp_path, ref, b"".join(lines[:2])) == 1                                  # whole week: all must be there
    assert run(tmp_path, ref, lines[0] + lines[2], "--through", "2021-11-08") == 1       # a closed window is missing
    wrong = lines[2].replace(b"0.25", b"0.26")                                           # an open one, but reported
    assert run(tmp_path, ref, b"".join(lines[:2]) + wrong, "--through", "2021-11-08") == 1
