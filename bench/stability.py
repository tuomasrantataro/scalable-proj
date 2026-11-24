#!/usr/bin/env python3
"""Is the latency stable while the load lasts? Latency over wall time, from the results of the run in the database.

Every window has `fired_at` (the watermark passed its end) and `emitted_at` (EMA result produced), so fired → emitted can
be followed through a run, for all windows and for the windows with an advisory. That is nearly all of the advisory
latency (see bench/advisory_latency.py for the stages after it). The first window close (cold start) is left out, as
in the other measurements.

  timeline   per BIN_S seconds of wall time: windows, p50, p95, p99, max
  segments   the same per trading day if the run covers several (each day has about the same load), else per third
             of the run: a latency that grows from segment to segment is not stable
  session    the same per trading day for the windows ending in SESSION. At more than real-time pace the quiet hours pass in
             an instant and their closes pile up, so nights and the start of the run are reported apart, not dropped: the
             rows "all" and the plot contain everything, and bins with off-session windows are shaded
  lag        consumer lag from Prometheus over the same time

bench/measure.py stores this with every point (peak_test.py, subscriber_sweep.py). For a live run (scripts/run-live.sh),
record it right after the run, before the next one empties the table:
  reference/.venv/bin/python bench/stability.py record --name live       # → bench/results/stability/<time>-live/
Plots (matplotlib, so with the reference's environment):
  reference/.venv/bin/python bench/stability.py plot bench/results/peak_test/<time>-<name>
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from backlog import ROOT, log, prom, sql
from latency_sweep import window_latency

BIN_S = 5
SESSION = ("09:00", "17:30")     # a window counts if it ends after the first and not after the second
LAG = "sum(flink_taskmanager_job_task_operator_pendingRecords)"
IN_SESSION = f"window_end::time > '{SESSION[0]}' AND window_end::time <= '{SESSION[1]}'"
STEADY = "window_end > (SELECT min(window_end) FROM ema_window)"
MS = "extract(epoch FROM emitted_at - fired_at) * 1000"
KEYS = ("p50", "p95", "p99", "max")
AGG = ("count(*) || ',' || coalesce(array_to_string(percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY ms), "
       "','), ',,') || ',' || coalesce(max(ms)::text, '') || ',' || count(*) FILTER (WHERE NOT (" + IN_SESSION + "))")


def rows(key, where="TRUE"):
    """[[key, n, p50, p95, p99, max, off-session windows]] of fired → emitted over the steady windows; `key` is an SQL expression."""
    out = sql(f"SELECT {key} || '|' || {AGG} FROM (SELECT *, {MS} AS ms FROM ema_window WHERE {STEADY} AND {where}) t "
              + (f"GROUP BY {key} ORDER BY {key}" if key != "''" else ""))
    res = []
    for line in out.splitlines():
        k, rest = line.split("|")
        n, *q = rest.split(",")
        if int(n):
            res.append([k, int(n), *(round(float(x)) for x in q)])
    return res


def bounds():
    """Epoch seconds of the first and last fired_at of the run."""
    return [float(x) for x in sql("SELECT extract(epoch FROM min(fired_at)) || ' ' || extract(epoch FROM max(fired_at)) "
                                  "FROM ema_window").split()]


def collect(lag_range=None):
    """Timeline, segments and overall percentiles of the run in ema_window. lag_range: (start, end) epoch seconds, default
    the run's own span."""
    t0, t1 = bounds()
    key = f"floor((extract(epoch FROM fired_at) - {t0}) / {BIN_S})::int"
    bins = [[int(r[0]) * BIN_S, *r[1:]] for r in rows(key)]
    days = sql("SELECT count(DISTINCT window_end::date) FROM ema_window WHERE " + STEADY)
    if int(days) > 1:
        seg_key = "window_end::date::text"
        seg = rows(seg_key)
        seg_adv = rows(seg_key, "advisory IS NOT NULL")
    else:
        third = (t1 - t0) / 3 or 1
        seg_key = f"(1 + least(2, floor((extract(epoch FROM fired_at) - {t0}) / {third})))::int::text"
        seg = [[f"third {r[0]}", *r[1:]] for r in rows(seg_key)]
        seg_adv = [[f"third {r[0]}", *r[1:]] for r in rows(seg_key, "advisory IS NOT NULL")]
    ses = [[r[0], *r[1:]] for r in rows(seg_key, IN_SESSION)]
    ses_adv = rows(seg_key, "advisory IS NOT NULL AND " + IN_SESSION)
    ses_adv += [["all", *r[1:]] for r in rows("''", "advisory IS NOT NULL AND " + IN_SESSION)]
    start, end = lag_range or (t0, t1)
    lag = [[round(float(t) - t0, 1), round(float(v))] for r in
           prom("query_range", query=LAG, start=start, end=end, step=BIN_S) for t, v in r["values"]
           if v not in ("NaN", "+Inf", "-Inf")]
    return {"bin_s": BIN_S, "run_s": round(t1 - t0, 1), "timeline": bins, "segments": seg, "segments_advisories": seg_adv, "session": ses,
            "session_advisories": ses_adv, "session_hours": SESSION, "session_all": (rows("''", IN_SESSION) or [None])[0],
           
            "lag": lag, "windows_ms": window_latency(),
            "advisories_ms": dict(zip(("n",) + KEYS, (rows("''", "advisory IS NOT NULL") or [["", 0]])[0][1:6]))}


def describe(s):
    cell = lambda r: " / ".join(f"{v:,}" for v in r[2:6])
    lines = [f"Run {s['run_s']:.0f} s of wall time (first to last window closed), bins of {s['bin_s']} s. Window fired → EMA result, ms,",
             "without the first close.", "",
             "| Segment | Windows | p50 / p95 / p99 / max | Advisories | p50 / p95 / p99 / max |", "|---|---|---|---|---|"]
    adv = {r[0]: r for r in s["segments_advisories"]}
    for r in s["segments"]:
        a = adv.get(r[0])
        lines.append(f"| {r[0]} | {r[1]:,} | {cell(r)} | {f'{a[1]:,}' if a else 0} | {cell(a) if a else '–'} |")
    w, a = s["windows_ms"], s["advisories_ms"]
    if w.get("windows"):
        lines.append(f"| all | {w['windows']:,} | " + " / ".join(f"{w[k]:,.0f}" for k in KEYS) +
                     f" | {a.get('n', 0):,} | " + (" / ".join(f"{a[k]:,}" for k in KEYS) if a.get("n") else "–") + " |")
    if s["session"]:
        lines += ["", f"Only windows ending in the trading session ({s['session_hours'][0]}–{s['session_hours'][1]}):", "",
                  "| Segment | Windows | p50 / p95 / p99 / max | Advisories | p50 / p95 / p99 / max |", "|---|---|---|---|---|"]
        adv = {r[0]: r for r in s["session_advisories"]}
        lines += [f"| {r[0]} | {r[1]:,} | {cell(r)} | {f'{adv[r[0]][1]:,}' if r[0] in adv else 0} | "
                  f"{cell(adv[r[0]]) if r[0] in adv else '–'} |" for r in s["session"] + [["all", *s["session_all"][1:]]]]
    lag = [v for _, v in s["lag"]]
    if lag:
        lines += ["", f"Consumer lag: max {max(lag):,}, last {lag[-1]:,} records ({len(lag)} samples)."]
    return "\n".join(lines) + "\n"


def plot(s, path, title):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    ink, muted, grid, surface = "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"
    series = [("p50", 2, "#2a78d6"), ("p95", 3, "#eb6834"), ("max", 5, "#1baf7a")]
    fig, (ax, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(9, 7), facecolor=surface,
                                       gridspec_kw={"height_ratios": [3, 1.2, 1.2]})
    t = [r[0] + s["bin_s"] / 2 for r in s["timeline"]]
    end = max(t + [p for p, _ in s["lag"]] + [1])
    for name, i, color in series:
        v = [max(1, r[i]) for r in s["timeline"]]
        ax.plot(t, v, color=color, lw=2, label=name, solid_capstyle="round")
    ax.set_yscale("log")
    ax.set_ylabel("fired → EMA result (ms, log)", color=muted)
    ax.legend(loc="lower right", bbox_to_anchor=(1, 1), frameon=False, ncol=3, fontsize=9)
    for r in s["timeline"]:
        if r[6] > 0.2 * r[1]:
            for x in (ax, ax2, ax3):
                x.axvspan(r[0], r[0] + s["bin_s"], color="#ecebe5", lw=0, zorder=0)
    ax2.bar(t, [r[1] / s["bin_s"] for r in s["timeline"]], width=s["bin_s"] * 0.85, color="#2a78d6")
    ax2.set_ylabel("windows / s", color=muted)
    ax3.plot([p for p, _ in s["lag"]], [v / 1e6 for _, v in s["lag"]], color="#2a78d6", lw=2)
    ax3.set_ylabel("lag (M records)", color=muted)
    ax3.set_xlabel("seconds since the first window closed", color=muted)
    for x in (ax, ax2, ax3):
        x.set_facecolor(surface)
        x.grid(axis="y", color=grid, lw=0.8)
        x.set_axisbelow(True)
        x.tick_params(colors=muted, labelsize=9)
        x.set_xlim(0, end * 1.02)
        for side in ("top", "right", "left"):
            x.spines[side].set_visible(False)
        x.spines["bottom"].set_color("#b9b8b0")
    for x in (ax2, ax3):
        x.set_ylim(bottom=0)
    ax.set_title(title, loc="left", color=ink, fontsize=11)
    ax3.text(1, -0.55, f"shaded: windows ending outside {s['session_hours'][0]}–{s['session_hours'][1]} (at least 20% of the bin's)",
             transform=ax3.transAxes, ha="right", color=muted, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=160, facecolor=surface)
    plt.close(fig)


def write_timeline(s, path):
    path.write_text("\n".join(["second,windows,p50,p95,p99,max,off_session_windows"] + [",".join(map(str, r)) for r in s["timeline"]]) + "\n")


def try_plot(s, path, title):
    try:
        plot(s, path, title)
    except ImportError:
        log("   matplotlib not available: no plot (use reference/.venv/bin/python bench/stability.py plot DIR)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record", help="of the run in the database")
    r.add_argument("--name", default="")
    r.add_argument("--note", default="", help="what the run was (stored with the result)")
    p = sub.add_parser("plot", help="plots of every point of a peak_test/subscriber_sweep directory, or of a recorded one")
    p.add_argument("dir")
    a = ap.parse_args()
    if a.cmd == "record":
        out = ROOT / "bench" / "results" / "stability" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                                                         + (f"-{a.name}" if a.name else ""))
        out.mkdir(parents=True)
        s = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "note": a.note, **collect()}
        (out / "stability.json").write_text(json.dumps(s) + "\n")
        write_timeline(s, out / "timeline.csv")
        (out / "summary.md").write_text("# Latency stability" + (f": {a.name}" if a.name else "") + "\n\n"
                                        + (a.note + "\n\n" if a.note else "") + describe(s))
        try_plot(s, out / "stability.png", f"Window latency over the run ({a.name or 'run'})")
        log(describe(s) + f"\n→ {out.relative_to(ROOT)}")
        return
    d = Path(a.dir)
    if (d / "stability.json").is_file():
        s = json.loads((d / "stability.json").read_text())
        plot(s, d / "stability.png", f"Window latency over the run ({d.name})")
    for line in (d / "results.jsonl").read_text().splitlines() if (d / "results.jsonl").is_file() else []:
        point = json.loads(line)
        if "stability" in point and point["stability"]["timeline"]:
            name = point["name"].replace(" ", "-")
            plot(point["stability"], d / f"stability-{name}.png", f"Window latency over the run: {point['name']}, "
                 f"{point['events_per_s']:,} events/s")
            log(f"wrote {d / f'stability-{name}.png'}")


if __name__ == "__main__":
    main()
