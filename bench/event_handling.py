#!/usr/bin/env python3
"""What the job did with every event of the run just finished: skipped, dropped, reported, used.

  rows in the topic          Kafka
  price events and drops     the quality gate's counters in the TaskManager logs (last line of every subtask)
  reported issues            table data_issue
  results                    table ema_window

Run it after a run and before the next one, which removes the logs and empties the tables. The counters start again when
the job restarts after a failure, so do not use it after a failover test.

Usage (repo root; the job still running):
  python3 bench/event_handling.py
"""
import argparse
import re
import sys

from backlog import sh, sql, topic_records
from measure import markdown, number

LINE = re.compile(r"\[quality subtask=(\d+)\] usable=(\d+) issues:(.*)")


def gate_counts():
    """(usable price events, {issue: count}), summed over the subtasks."""
    last = {}
    for line in sh("docker compose logs --no-log-prefix --no-color taskmanager").splitlines():
        m = LINE.search(line)
        if m:
            last[m.group(1)] = (int(m.group(2)), {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", m.group(3))})
    if not last:
        sys.exit("no quality-gate counters in the TaskManager logs: is the job of the run still there?")
    issues = {}
    for _, counts in last.values():
        for k, v in counts.items():
            issues[k] = issues.get(k, 0) + v
    return sum(n for n, _ in last.values()), issues


def main():
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    topic = sh("docker compose exec -T jobmanager printenv KAFKA_TOPIC", check=False).strip()
    if not topic:
        sys.exit("the job is not running")
    usable, gate = gate_counts()
    stored = dict(line.split("|") for line in sql("SELECT reason, count(*) FROM data_issue GROUP BY 1 ORDER BY 1").splitlines())
    rows = topic_records(topic)
    dropped = {k: gate.get(k, 0) for k in ("zero_price", "malformed", "negative_price", "implausible_time")}
    price = usable + sum(dropped.values())
    late = int(stored.get("LATE", 0))
    windows, ticks, symbols, buy, sell, initial = sql(
        "SELECT count(*), coalesce(sum(ticks), 0), count(DISTINCT symbol), count(*) FILTER (WHERE advisory = 'BUY'), "
        "count(*) FILTER (WHERE advisory = 'SELL'), count(*) FILTER (WHERE initial) FROM ema_window").split("|")
    share = lambda n: f"{n / rows:.2%}" if rows else None
    table = [["Rows in topic " + topic, number(rows), share(rows), ""],
             ["No price (`Last` empty)", number(rows - price), share(rows - price), "skipped"],
             ["Price events", number(price), share(price), ""]]
    table += [[f"- {k}", number(v), share(v), "dropped, " + ("counted" if k == "zero_price" else "stored")]
              for k, v in dropped.items()]
    table += [["- late (window already evaluated)", number(late), share(late), "dropped, stored"],
              ["- used in windows", number(int(ticks)), share(int(ticks)), "close and tick count"]]
    print("# Event handling\n\n" + markdown(["Events", "Count", "Share", "Handling"], table))
    print(markdown(["Reported issues (data_issue)", "Count"], [[k, number(int(v))] for k, v in stored.items()]))
    print(f"Results: {int(windows):,} windows of {int(symbols):,} symbols, {int(buy):,} BUY, {int(sell):,} SELL "
          f"({int(initial):,} initial).")
    rest = usable - late - int(ticks)
    print("Check: usable = late + used in windows" + (" (exact)" if rest == 0 else
          f" + {rest:,} in windows that are still open (input ended) or were not counted"))


if __name__ == "__main__":
    main()
