"""Workloads cut from the CSV files: feed rate profile, busiest minutes, replay slices and subscription files.

Used by bench/peak_test.py and bench/subscriber_sweep.py; nothing here touches Docker.

Why slices: they can hold a single exchange, and their rates and symbols are known before the replay. The files are in
arrival order, but early in each file there are snapshot rows that carry a much later `Time`. A slice is the contiguous
part of the file where the feed's clock (median `Time` of each 1,000-line block) is in the range, without rows that are
ahead of the clock. Late rows are kept. The producer paces by the same kind of clock (producer-rs/src/feed.rs).
"""
import itertools
import json
import os
import random
import re
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCHANGES = ("FR", "NL", "ETR")
BLOCK = 1000        # lines per clock reading
AHEAD_S = 5         # rows further ahead of the clock than this are left out of a slice
MAX_GAP = 20        # blocks outside the range that do not end a region
LAST = 21           # column `Last` (non-empty = price event)
SYMBOL = re.compile(r"[A-Za-z0-9_.-]+")   # as accepted by Subscriptions.java


def day_file(data_dir, day):
    return Path(data_dir) / f"debs2022-gc-trading-day-{day}-11-21.csv"


def clock_s(t):
    """b"HH:MM:SS[.mmm]" → seconds since midnight, or None."""
    try:
        return int(t[:2]) * 3600 + int(t[3:5]) * 60 + float(t[6:])
    except ValueError:
        return None


def hhmm_s(s):
    h, m = s.split(":")
    return int(h) * 3600 + int(m) * 60


def s_hhmm(s):
    return f"{int(s) // 3600:02}:{int(s) % 3600 // 60:02}"


def is_row(line):
    return not line.startswith((b"#", b"ID,")) and line.strip() != b""


def blocks_of(f):
    """Yields (bytes, [(line, time or None)]) per block of BLOCK lines; `line` only for data rows."""
    while block := list(itertools.islice(f, BLOCK)):
        rows = []
        for line in block:
            if is_row(line):
                parts = line.split(b",", 4)
                rows.append((line, clock_s(parts[3]) if len(parts) > 4 else None))
        yield sum(map(len, block)), rows


def block_clock(rows):
    times = sorted(t for _, t in rows if t is not None)
    return times[len(times) // 2] if times else None


def profile_day(path):
    """Rows per exchange and minute (column Time), and the byte offset and clock of every block."""
    minutes, blocks, offset = Counter(), [], 0
    with open(path, "rb", buffering=1 << 20) as f:
        for size, rows in blocks_of(f):
            blocks.append([offset, block_clock(rows)])
            offset += size
            for line, t in rows:
                if t is not None:
                    minutes[line[:line.index(b",")].rpartition(b".")[2], int(t) // 60] += 1
    out = {}
    for (exchange, minute), n in minutes.items():
        out.setdefault(exchange.decode(), {})[s_hhmm(minute * 60)] = n
    return {"size": os.stat(path).st_size, "minutes": out, "blocks": blocks, "end": offset}


def profile(data_dir, days, log=print):
    """{day: profile_day}, cached in <data_dir>/slices (one file scan is about a minute per day)."""
    cache = Path(data_dir) / "slices"
    cache.mkdir(exist_ok=True)
    result, todo = {}, []
    for day in days:
        path, cached = day_file(data_dir, day), cache / f"profile-{day}.json"
        if not path.is_file():
            raise FileNotFoundError(path)
        p = json.loads(cached.read_text()) if cached.is_file() else None
        if p and p["size"] == path.stat().st_size:
            result[day] = p
        else:
            todo.append(day)
    if todo:
        log(f"== profiling the feed rate of day(s) {', '.join(todo)}")
        with ProcessPoolExecutor(min(len(todo), os.cpu_count() or 1)) as pool:
            for day, p in zip(todo, pool.map(profile_day, [day_file(data_dir, d) for d in todo])):
                write_atomic(cache / f"profile-{day}.json", json.dumps(p))
                result[day] = p
    return result


def minute_rows(p, scope):
    """{"HH:MM": rows} of one exchange, or of all of them (scope "all")."""
    total = Counter()
    for exchange, minutes in p["minutes"].items():
        if scope in ("all", exchange):
            total.update(minutes)
    return total


def peak(profiles, scope, first="08:00", last="18:00"):
    """Busiest minute of a scope within [first, last): {"scope", "day", "minute", "rows"}."""
    best = None
    for day in sorted(profiles):
        for minute, n in sorted(minute_rows(profiles[day], scope).items()):
            if first <= minute < last and (best is None or n > best["rows"]):
                best = {"scope": scope, "day": day, "minute": minute, "rows": n}
    return best


def region(p, start_s, end_s):
    """Byte range of the largest contiguous part of the file whose clock is in [start_s, end_s)."""
    runs = []   # [first block, last block, blocks in range]
    for i, (_, clock) in enumerate(p["blocks"]):
        if clock is None or not start_s <= clock < end_s:
            continue
        if runs and i - runs[-1][1] <= MAX_GAP:
            runs[-1][1] = i
            runs[-1][2] += 1
        else:
            runs.append([i, i, 1])
    if not runs:
        return None
    first, last, _ = max(runs, key=lambda r: r[2])
    end = p["blocks"][last + 1][0] if last + 1 < len(p["blocks"]) else p["end"]
    return p["blocks"][first][0], end


def extract(data_dir, p, day, start, end, scope="all", log=print):
    """Writes the slice [start, end) (HH:MM) of a day to <data_dir>/slices and returns its description (cached)."""
    out = Path(data_dir) / "slices" / f"{day}-{start.replace(':', '')}-{end.replace(':', '')}-{scope}.csv"
    meta_file = out.with_suffix(".json")
    if out.is_file() and meta_file.is_file():
        meta = json.loads(meta_file.read_text())
        if meta["source_size"] == p["size"] and meta["bytes"] == out.stat().st_size:
            return meta
    bounds = region(p, hhmm_s(start), hhmm_s(end))
    if bounds is None:
        raise ValueError(f"day {day}: no rows between {start} and {end}")
    log(f"== cutting slice {out.name}")
    suffix = None if scope == "all" else b"." + scope.encode()
    rows_out = ahead = leading = 0
    first_clock = last_clock = None
    seconds, minutes, price_rows, symbols = Counter(), Counter(), Counter(), Counter()
    tmp = out.with_suffix(".tmp")
    with open(day_file(data_dir, day), "rb", buffering=1 << 20) as f, open(tmp, "wb", buffering=1 << 20) as w:
        f.seek(bounds[0])
        pos = bounds[0]
        for size, rows in blocks_of(f):
            if pos >= bounds[1]:
                break
            pos += size
            clock = block_clock(rows)
            if clock is None:
                continue
            for line, t in rows:
                symbol = line[:line.index(b",")]
                if suffix and not symbol.endswith(suffix):
                    continue
                if t is not None and t > clock + AHEAD_S:
                    ahead += 1
                    continue
                if first_clock is None:  # the producer paces from the first row, so it must be on the clock
                    if t is None or abs(t - clock) > 1:
                        leading += 1
                        continue
                    first_clock = t
                w.write(line)
                rows_out += 1
                symbols[symbol] += 1
                if t is not None:
                    last_clock = max(t, last_clock or t)
                    seconds[int(t)] += 1
                    minutes[int(t) // 60] += 1
                parts = line.split(b",", LAST + 1)
                if len(parts) > LAST and parts[LAST].strip():
                    price_rows[symbol] += 1
    if rows_out == 0:
        tmp.unlink()
        raise ValueError(f"day {day}: no {scope} rows between {start} and {end}")
    os.replace(tmp, out)
    meta = {"file": out.name, "day": day, "start": start, "end": end, "scope": scope, "rows": rows_out,
            "dropped_ahead": ahead, "dropped_leading": leading, "span_s": round(last_clock - first_clock, 3),
            "peak_minute_rows": max(minutes.values()), "peak_second_rows": max(seconds.values()),
            "symbols": len(symbols), "price_events": sum(price_rows.values()),
            "price_rows": {s.decode(): n for s, n in price_rows.most_common()},
            "source_size": p["size"], "bytes": out.stat().st_size}
    write_atomic(meta_file, json.dumps(meta))
    return meta


def write_atomic(path, text):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def subscriptions(price_rows, traders, per_trader, seed=1):
    """{trader: [symbols]}: each trader follows `per_trader` different symbols, drawn with probability
    proportional to their price events, so popular symbols have many followers (long tail, like the feed).
    The first n traders are the same for every number of traders."""
    weighted = [(s, n) for s, n in sorted(price_rows.items()) if n > 0 and SYMBOL.fullmatch(s)]
    out = {}
    for i in range(traders):
        rnd = random.Random(f"{seed}-{i}")
        drawn = sorted(weighted, key=lambda sn: rnd.random() ** (1 / sn[1]), reverse=True)[:per_trader]
        out[f"trader-{i + 1:05}"] = sorted(s for s, _ in drawn)
    return out


def subscriptions_text(subs, note):
    lines = [f"# Generated by bench/subscriber_sweep.py: {note}. Format: see ../subscriptions.txt."]
    lines += [f"{t}: {' '.join(symbols)}" for t, symbols in subs.items()]
    return "\n".join(lines) + "\n"
