# Reference implementation (Python + pandas)

A batch implementation of Query 1 (EMA38/EMA100 per 5-minute window) and Query 2 (BUY/SELL crossover
advisories). It is written from [`docs/requirements.md`](../docs/requirements.md) alone, independently of
the Flink job, so the two can be checked against each other (NFR-9). It computes every symbol (FR-3); the
comparison filters to the subscribed ones (FR-4).

```bash
python3 -m venv reference/.venv && reference/.venv/bin/pip install -r reference/requirements.txt
reference/.venv/bin/python -m pytest reference -q                     # unit tests on hand-made CSVs

reference/.venv/bin/python reference/ema_reference.py                 # whole week: ~2.5 min, 4 cores
reference/.venv/bin/python reference/ema_reference.py --exclude-zero-prices --out reference/output-nozero
reference/.venv/bin/python reference/ema_reference.py --lateness inf --out reference/output-batch

reference/.venv/bin/python reference/plot_symbols.py \
    --windows reference/output-nozero/windows.parquet --suffix _nozero --label "zero prices excluded"

reference/.venv/bin/python reference/compare.py                       # Flink results vs reference/output-nozero
```

`compare.py` compares table `ema_window` with the reference: the same windows, and ticks, close, both EMAs, advisories
and `initial` marks bit for bit. The md5 of `output-nozero` in the export format of `bench/backlog.py` is
`19413a509d53044973e3e6b76c177af1`, the same as the job's (`WEEK_DIGEST`), so every run with that digest equals the
reference.

Options of `ema_reference.py`: `--data-dir` (default `data`), `--out` (default `reference/output`), `--files` (some days only),
`--exclude-zero-prices`, `--lateness SECONDS` (default 60, `inf` keeps late events), `--jobs` (files read
in parallel).

Outputs (git-ignored), timestamps in naive local exchange time:

| File | Content |
|---|---|
| `windows.parquet` | One row per symbol and non-empty window: `symbol, sec_type, window_start, window_end, close, close_time, ticks, ema38, ema100, prev_ema38, prev_ema100, advisory, first_window` |
| `advisories.csv` | `symbol, type, window_start, window_end, due_at, close, ema38, ema100, prev_ema38, prev_ema100, initial` |
| `summary.json` | Per-file and total counts: rows, price events, zero prices, out-of-order and late events, windows, advisories |

## Semantics

| Topic | Decision | Requirement |
|---|---|---|
| Price event | A row with `Last` set. Other rows are ignored. Zero prices are kept unless `--exclude-zero-prices`; see below | FR-9 |
| Event time | `Trading date` (or `Date` when it is empty) + `Trading time`, as local wall-clock time. The system `Time` is not used: closing-auction trades at 17:30 arrive with system time 09:02 | §2.3 |
| Windows | 5-min tumbling, aligned to midnight; `window_start ≤ t < window_end` | FR-5…7 |
| Close | Price of the event with the **latest event time** in the window; ties go to the later row in the input. Arrival order is not assumed | FR-10 |
| EMA | `close · a + prev · (1 − a)`, `a = 2/(1+j)`, evaluated in exactly this order (bit-for-bit comparable); `prev = 0` before a symbol's first window; state persists across days | FR-11…13 |
| Empty window | No row; EMAs unchanged; no crossover check. "Previous window" = the symbol's previous non-empty window | architecture §3.5 |
| Advisory | BUY if `ema38 > ema100 ∧ prev38 ≤ prev100`; SELL if `ema38 < ema100 ∧ prev38 ≥ prev100`. Due at `window_end` (= when `w_{i+1}` starts) | FR-8, 15, 16 |
| Initialization artifact | `initial = true` when both previous EMAs are still 0: the first positive close of a symbol always gives BUY | §7 |
| Late events | The input is treated as one stream in file order. Watermark = highest event time seen so far − `--lateness`; an event whose window has ended at or before the watermark is dropped and counted | NFR-7, FR-8 |

## Results for the full week (8–14 Nov 2021)

289,116,808 rows; 59.7 M price events (49.1 M index, 10.6 M equity); 5,182 symbols with a price event.
Each run takes about 2.5 minutes, with 4 files read in parallel.

| Run | Price events used | Late dropped | Symbols | Windows | Advisories (BUY / SELL) | of which `initial` |
|---|---|---|---|---|---|---|
| `output`: spec, zero prices kept, lateness 60 s | 59,702,357 | 5,695 | 5,182 | 1,902,378 | 25,834 (13,826 / 12,008) | 4,970 |
| `output-nozero`: zero prices excluded | 59,386,164 | 5,506 | 4,970 | 1,855,645 | 11,349 (7,769 / 3,580) | 4,970 |
| `output-batch`: spec, no late-event drop | 59,708,052 | 0 | 5,182 | 1,904,190 | 25,830 (13,824 / 12,006) | 4,970 |

Checks run on these outputs:
- EMAs equal an independent `pandas.ewm(adjust=False)` per symbol, with a prepended 0, exactly (relative difference 0.0).
- Every advisory satisfies FR-15/16, and every window that satisfies them has an advisory.
- No duplicate (symbol, window) rows, and every close time lies inside its window.

### Data findings that decide the results

**Zero prices (`Last = 0`): 316,382 rows, open decision.** Every symbol gets one `Last = 0` record at
`Trading time 00:00:00.000` each day, including Saturday. Xetra also sends some at 09:00 and 17:30. These are
reset records, not trades. Taken literally (FR-9: non-NULL `Last` is a price event), they give 46,733 windows
with close 0. Each such window pulls EMA38 down faster than EMA100, so a symbol trending up gets a SELL at
00:05 and a BUY again the next morning. **Excluding zero prices removes 14,485 of the 25,834 advisories,
more than half.** 11,234 advisories fall in the 00:00 window alone. The figures below show it (the `×` marks and the
SELLs at each day's left edge). The Flink job and the reference must make the same choice. Excluding zero
prices is the meaningful reading, and keeping them is the literal one.

**Out-of-order events: one block, 17.9 h late.** In file order, every price event is in event-time
order per symbol except 5,506 rows at the end of 10 Nov. They are 1,812 Paris indices (e.g. CAC 40 `PX1.FR`)
whose open, day high and day low are re-sent at 18:56 in `Last` with `Trading time 01:00:00.000`. A stream
has evaluated those windows hours before, so they are late. Also late against the global watermark: 189
Xetra `Last = 0` records with `Trading time 00:00` that arrive at the end of each day's file. Since nothing
else is out of order, **any lateness bound from 0 s to 17.9 h gives the same result.** Keeping the late rows
(`output-batch`) instead turns the re-sent values into the close of a 01:00 window. That changes the EMAs of
1,812 symbols from 10 Nov on: 1,791 advisories exist only with the late rows dropped and 1,787 only with
them kept, mostly the same crossover shifted by one window.

**Other:** `Trading date` is set on about 8% of price rows and always equals `Date`. `Trading time` is
always `HH:MM:SS.fff`. No price fails to parse, and none is negative. Event times run from 00:00 to 22:35
each trading day. 13 Nov has only the 00:00 zero records, and 14 Nov has no price events.

## Figures

One PNG per symbol in [`docs/figures/reference/`](../docs/figures/reference/). `<symbol>.png` is the spec
run (zero prices kept) and `<symbol>_nozero.png` excludes them. Top: 5-min close (steps), EMA38, EMA100,
BUY ▲ / SELL ▼ (hollow ▲ = BUY from EMA = 0 initialization; markers below the scale are pinned to the
bottom edge). Bottom: price events per window. The x-axis shows only the symbol's trading hours, with
days side by side.

| Symbol | Profile | Spec | Zero prices excluded |
|---|---|---|---|
| `A169S8.ETR` | busiest index: one update per second (300 per window) during Xetra hours | [png](../docs/figures/reference/A169S8_ETR.png) | [png](../docs/figures/reference/A169S8_ETR_nozero.png) |
| `623100.ETR` | busiest Xetra equity | [png](../docs/figures/reference/623100_ETR.png) | [png](../docs/figures/reference/623100_ETR_nozero.png) |
| `TTE.FR` | busiest Paris equity | [png](../docs/figures/reference/TTE_FR.png) | [png](../docs/figures/reference/TTE_FR_nozero.png) |
| `ASML.NL` | busiest Amsterdam equity | [png](../docs/figures/reference/ASML_NL.png) | [png](../docs/figures/reference/ASML_NL_nozero.png) |
| `EUS5P.NL` | most crossovers; an index updated exactly 20 times per window, 09:00–23:00 | [png](../docs/figures/reference/EUS5P_NL.png) | [png](../docs/figures/reference/EUS5P_NL_nozero.png) |
| `A2PBHR.ETR` | sparse equity: 307 trades in 196 windows, hours-long gaps | [png](../docs/figures/reference/A2PBHR_ETR.png) | [png](../docs/figures/reference/A2PBHR_ETR_nozero.png) |

**Warm-up (shaded).** The EMAs start at 0 (FR-12). After n updates, that start value still has weight
`(1 − a)^n`, with `a = 2/(1+j)`, so an EMA approaches the price gradually and is never exactly "ready" after j
windows. After 38 windows, EMA38 has reached only 86.5% of a steady price. The figures shade each EMA until
the start value weighs < 1%: 88 non-empty windows for EMA38 (~7.3 h of trading) and 231 for EMA100
(~2 trading days), counted from the symbol's first positive close. The y-axis spans only the closes, a few
percent of the price, so an EMA comes into view only near the end of its warm-up. Advisories inside the
shading are driven mostly by the initialization. A sparse symbol may not finish warming up within the week
(`A2PBHR.ETR`: 196–201 windows). Regenerate the figures with:

```bash
reference/.venv/bin/python reference/plot_symbols.py --windows reference/output-nozero/windows.parquet \
    --suffix _nozero --label "zero prices excluded (--exclude-zero-prices), late events dropped (60 s)"
reference/.venv/bin/python reference/plot_symbols.py --windows reference/output/windows.parquet \
    --pick-from reference/output-nozero/windows.parquet --label "zero prices kept (spec FR-9), late events dropped (60 s)"
```
