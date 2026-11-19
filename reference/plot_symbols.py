#!/usr/bin/env python3
"""Plots a week of reference results (ema_reference.py output) for a few symbols, one PNG per symbol.

Top panel: 5-min closes, EMA38, EMA100, and BUY/SELL advisories. Bottom panel: price events per window.
Shaded: the EMA warm-up. EMAs start at 0 (FR-12), and after n updates the initial 0 still has weight
(1 - a)^n, a = 2 / (1 + j). The shading lasts until that weight is below WARMUP_RESIDUAL (1%), counted in
non-empty windows from the symbol's first positive close: 88 windows for EMA38, 231 for EMA100.
The time axis shows only the hours the symbol trades (from its first to its last non-zero close of the
day, rounded to whole hours); trading days are placed side by side, so nights and the weekend take no
space. Windows outside those hours (e.g. the 00:00 `Last = 0` records) are drawn at the day's left
edge, and advisories on days without trading are counted in the footer.

Usage:
  reference/.venv/bin/python reference/plot_symbols.py --windows reference/output/windows.parquet \
      --out docs/figures/reference [--symbols A.FR B.NL ...] [--pick-from other/windows.parquet]
      [--label "zero prices kept"] [--suffix _x]
Without --symbols, picks one symbol per data profile (see pick_symbols) and prints why.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# Reference palette (dataviz skill, light mode)
SURFACE = "#fcfcfb"
GRID = "#e9e8e4"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
MUTED = "#8a8984"
CLOSE = "#b9b8b2"
EMA38 = "#2a78d6"     # slot 1 blue
EMA100 = "#eb6834"    # slot 2 orange
BUY = "#008300"       # slot 6 green, triangle up
SELL = "#e34948"      # slot 8 red, triangle down
TICKS = "#86b6ef"     # blue 250
GAP_H = 0.6           # hours of blank space between days
WARM_BOTH = "#ecebe6"  # both EMAs warming up
WARM_100 = "#f4f3ef"   # EMA100 still warming up
WARMUP_RESIDUAL = 0.01  # warm-up ends when the initial EMA = 0 has less than this weight


def warmup_windows(j: int) -> int:
    """Number of EMA updates until the weight of the initial 0, (1 - a)^n, is below WARMUP_RESIDUAL."""
    return int(np.ceil(np.log(WARMUP_RESIDUAL) / np.log(1 - 2 / (1 + j))))

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": TEXT_2,
    "xtick.color": TEXT_2, "ytick.color": TEXT_2, "axes.titlecolor": TEXT, "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
})

EXCHANGE = {"FR": "Paris", "NL": "Amsterdam", "ETR": "Xetra (Frankfurt)"}


def pick_symbols(w: pd.DataFrame) -> dict[str, str]:
    """One symbol per profile, chosen from per-symbol statistics."""
    real = w[w["close"] > 0]
    s = real.groupby("symbol").agg(ticks=("ticks", "sum"), windows=("ticks", "size"),
                                   sec=("sec_type", "first"))
    adv = w[w["advisory"].notna() & ~((w["prev_ema38"] == 0) & (w["prev_ema100"] == 0))].groupby("symbol").size()
    s["advisories"] = adv.reindex(s.index).fillna(0).astype(int)
    s["exchange"] = s.index.str.rsplit(".", n=1).str[1]
    picks = {}
    picks[s[s.sec == "I"]["ticks"].idxmax()] = "busiest index (most price events)"
    for ex in ("ETR", "FR", "NL"):
        eq = s[(s.sec == "E") & (s.exchange == ex) & ~s.index.isin(picks)]
        picks[eq["ticks"].idxmax()] = f"busiest equity on {EXCHANGE[ex]}"
    rest = s[~s.index.isin(picks)]
    picks[rest["advisories"].idxmax()] = "most crossovers"
    # Sparse: trades on at least 4 days in few windows (long gaps between closes), with a crossover.
    days = real.assign(d=real["window_start"].dt.date).groupby("symbol")["d"].nunique()
    rest = s[~s.index.isin(picks)]
    sparse = rest[(days.reindex(rest.index) >= 4) & (rest.windows <= 200) & (rest.advisories >= 1)]
    if len(sparse):
        picks[sparse["ticks"].idxmin()] = "sparse equity (few trades, many empty windows)"
    return picks


def compressed_x(ts: pd.Series, days: list, tod_lo: float, tod_hi: float) -> np.ndarray:
    """Maps timestamps to hours on an axis of trading days side by side; NaN for non-trading days."""
    day_idx = {d: i for i, d in enumerate(days)}
    idx = ts.dt.date.map(day_idx).to_numpy(dtype=float)
    tod = (ts - ts.dt.normalize()).dt.total_seconds().to_numpy() / 3600
    span = tod_hi - tod_lo
    return idx * (span + GAP_H) + np.clip(tod - tod_lo, 0, span)


def plot_symbol(w: pd.DataFrame, symbol: str, why: str, label: str, out: Path):
    d = w[w["symbol"] == symbol].sort_values("window_start").reset_index(drop=True)
    real = d[d["close"] > 0]
    days = sorted(real["window_start"].dt.date.unique())
    tod = lambda s: (s - s.dt.normalize()).dt.total_seconds() / 3600  # noqa: E731
    tod_lo = float(np.floor(tod(real["window_start"]).min()))
    tod_hi = float(np.ceil(tod(real["window_end"]).max()))
    if tod_hi <= tod_lo:
        tod_hi = tod_lo + 1
    span = tod_hi - tod_lo

    on_day = d["window_start"].dt.date.isin(days)
    d = d[on_day].reset_index(drop=True)
    hidden = w[(w["symbol"] == symbol) & w["advisory"].notna() & ~w["window_start"].dt.date.isin(days)]
    xe = compressed_x(d["window_end"], days, tod_lo, tod_hi)       # EMA known at window end
    xs = compressed_x(d["window_start"], days, tod_lo, tod_hi)
    # break lines between days
    brk = np.r_[False, d["window_start"].dt.date.to_numpy()[1:] != d["window_start"].dt.date.to_numpy()[:-1]]

    def broken(x, y):
        x, y = np.asarray(x, float).copy(), np.asarray(y, float).copy()
        ins = np.flatnonzero(brk)
        return np.insert(x, ins, np.nan), np.insert(y, ins, np.nan)

    fig, (ax, axt) = plt.subplots(2, 1, figsize=(13, 7.0), sharex=True,
                                  gridspec_kw={"height_ratios": [3.2, 1], "hspace": 0.08})
    # The close is drawn as steps: it is known at its window's end and holds until the next non-empty window.
    ax.plot(*broken(xe, d["close"]), color=CLOSE, lw=1, label="5-min close", zorder=1, drawstyle="steps-post")
    ax.plot(*broken(xe, d["ema38"]), color=EMA38, lw=2, label="EMA38", zorder=3, solid_joinstyle="round")
    ax.plot(*broken(xe, d["ema100"]), color=EMA100, lw=2, label="EMA100", zorder=2, solid_joinstyle="round")

    # y range from the real (non-zero) closes: zero closes and the EMA warm-up from 0 would flatten the
    # chart. Lines below the range run off the bottom; advisories there are pinned to the bottom edge.
    lo, hi = real["close"].min(), real["close"].max()
    pad = (hi - lo) * 0.1 or hi * 0.01
    y_min = lo - pad
    a = d[d["advisory"].notna()]
    xa = xe[a.index]
    ya = np.maximum(a["ema38"].to_numpy(), y_min + pad * 0.25)
    for kind, color, marker in (("BUY", BUY, "^"), ("SELL", SELL, "v")):
        m = (a["advisory"] == kind).to_numpy()
        init = ((a["prev_ema38"] == 0) & (a["prev_ema100"] == 0)).to_numpy()
        n = int((m & ~init).sum())
        if n:
            ax.scatter(xa[m & ~init], ya[m & ~init], s=80, marker=marker, color=color,
                       edgecolors=SURFACE, linewidths=1.5, zorder=5, label=f"{kind} ({n})")
        if (m & init).any():
            ax.scatter(xa[m & init], ya[m & init], s=70, marker=marker,
                       facecolors="none", edgecolors=color, linewidths=1.5, zorder=5,
                       label=f"{kind} from EMA = 0 init")

    ax.set_ylim(y_min, hi + pad)

    # EMA warm-up shading, from the first positive close to the end of the n-th update after it
    first = int(np.flatnonzero(d["close"].to_numpy() > 0)[0])
    x_end_axis = len(days) * (span + GAP_H) - GAP_H
    warm_end = {}
    for j in (38, 100):
        last = first + warmup_windows(j) - 1
        warm_end[j] = (xe[last], True) if last < len(d) else (x_end_axis, False)
    x0 = xs[first]
    ax.axvspan(x0, warm_end[38][0], color=WARM_BOTH, lw=0, zorder=0,
               label=f"EMA38 + EMA100 warm-up ({warmup_windows(38)} windows)")
    if warm_end[100][0] > warm_end[38][0]:
        ax.axvspan(warm_end[38][0], warm_end[100][0], color=WARM_100, lw=0, zorder=0,
                   label=f"EMA100 warm-up ({warmup_windows(100)} windows)")
    warm_note = "" if warm_end[100][1] else " EMA100 still warming up at week's end."
    zero = d["close"] == 0
    if zero.any():
        ax.scatter(xe[zero.to_numpy()], np.full(zero.sum(), y_min + pad * 0.25), marker="x", s=30, color=MUTED,
                   linewidths=1.2, zorder=4, label="close = 0")

    axt.bar(xs, d["ticks"], width=5 / 60, align="edge", color=TICKS, linewidth=0)
    axt.set_ylabel("price events\nper window")
    ax.set_ylabel("price")

    for a_ in (ax, axt):
        a_.grid(axis="y", color=GRID, lw=1)
        a_.set_axisbelow(True)
        for sp in ("top", "right", "left"):
            a_.spines[sp].set_visible(False)
        a_.tick_params(length=0)
        for i in range(1, len(days)):
            a_.axvline(i * (span + GAP_H) - GAP_H / 2, color=GRID, lw=1)

    ticks, labels = [], []
    step = 2 if span > 8 else 1
    for i, day in enumerate(days):
        for h in np.arange(np.ceil(tod_lo / step) * step, tod_hi + 1e-9, step):
            ticks.append(i * (span + GAP_H) + h - tod_lo)
            labels.append(f"{int(h):02d}" + (f"\n{pd.Timestamp(day):%a %d %b}" if h == np.ceil(tod_lo / step) * step
                                               else ""))
    axt.set_xticks(ticks, labels)
    axt.set_xlim(-0.2, len(days) * (span + GAP_H) - GAP_H + 0.2)

    sec = d["sec_type"].iloc[0]
    ex = symbol.rsplit(".", 1)[1]
    n_adv = int(d["advisory"].notna().sum())
    fig.suptitle(f"{symbol}  ·  {'index' if sec == 'I' else 'equity'}, {EXCHANGE.get(ex, ex)}  ·  {why}",
                 x=0.06, y=0.975, ha="left", fontsize=12, fontweight="bold", color=TEXT)
    fig.text(0.06, 0.945,
             f"{int(d['ticks'].sum()):,} price events in {len(d):,} non-empty 5-min windows, {n_adv} advisories. "
             f"Reference run: {label}.\nHours {int(tod_lo):02d}:00–{int(tod_hi):02d}:00 local time, trading days only; "
             f"empty windows skipped (EMAs unchanged).\n"
             f"Shaded: warm-up from the start value EMA = 0 (FR-12), until it weighs < {WARMUP_RESIDUAL:.0%}."
             f"{warm_note}" + (" A close = 0 (×) pulls both EMAs down again." if zero.any() else ""),
             ha="left", va="top", linespacing=1.5, fontsize=9, color=TEXT_2)
    if len(hidden):
        fig.text(0.06, 0.01, f"Not shown: {len(hidden)} advisor{'y' if len(hidden) == 1 else 'ies'} on days without trading "
                 f"({', '.join(f'{t} {pd.Timestamp(s):%a %H:%M}' for t, s in hidden[['advisory', 'window_end']].itertuples(index=False))}).",
                 ha="left", fontsize=8, color=MUTED)
    n_leg = len(ax.get_legend_handles_labels()[0])
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=int(np.ceil(n_leg / 2)), frameon=False, fontsize=8.5,
              labelcolor=TEXT_2, handletextpad=0.4, columnspacing=1.4, borderaxespad=0.2)
    fig.subplots_adjust(left=0.06, right=0.99, top=0.79, bottom=0.11)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=130)
    plt.close(fig)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--windows", type=Path, default=Path("reference/output/windows.parquet"))
    ap.add_argument("--out", type=Path, default=Path("docs/figures/reference"))
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--pick-from", type=Path, help="windows.parquet to choose and describe symbols from "
                    "(default: --windows); lets two runs be plotted for the same symbols")
    ap.add_argument("--label", default="zero prices kept (spec)")
    ap.add_argument("--suffix", default="", help="appended to each file name")
    args = ap.parse_args(argv)

    w = pd.read_parquet(args.windows)
    auto = pick_symbols(pd.read_parquet(args.pick_from) if args.pick_from else w)
    picks = {s: auto.get(s, "") for s in args.symbols} if args.symbols else auto
    for sym, why in picks.items():
        path = args.out / f"{sym.replace('.', '_')}{args.suffix}.png"
        plot_symbol(w, sym, why, args.label, path)
        print(f"{path}  {sym}: {why}")


if __name__ == "__main__":
    main()
