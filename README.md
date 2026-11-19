# scalable-proj

Minimal experiment for the Iggy → Flink connector with several Flink nodes
(see [`docs/architecture.md`](docs/architecture.md)).

| Service | What it does |
|---|---|
| `iggy` | Apache Iggy 0.10.0 server (TCP 8090, HTTP 3000) |
| `iggy-setup` | One-shot: creates stream `trading`, topic `ticks` (1 partition by default, see below), consumer group `flink`, and output topic `advisories` (1 partition, `IGGY_ADVISORIES_PARTITIONS`; not written to yet) |
| `jobmanager` | Flink 2.3.0, standalone application mode, runs `scalable.flink.ConnectorTestJob` (UI: http://localhost:8081) |
| `taskmanager` ×2 | 1 slot each, job parallelism 2 |
| `timescaledb` | TimescaleDB 2.31 (PostgreSQL 17), table `window_close` ([schema](config/timescaledb/init.sql)). Written by `PIPELINE=jdbc` (5-min windows, upsert). |
| `replay-producer` | Sends CSV rows to `ticks`, partitioned by symbol ID (Rust, `producer-rs/`) |

Job topology:

```
IggySource (p=2) → node-counter (p=2, chained) ─┬─ keyBy(symbol) → symbol-counter (p=2)
                                                 └─ verifier (p=1): total / distinct / duplicates / gaps per partition
```

Connector: `org.apache.iggy:flink-connector:0.10.0` (built against Flink 2.3.0).

## Run

```bash
docker compose up --build -d
docker compose logs -f taskmanager | grep scalable.report     # per-node counters

REPEAT=500 docker compose run --rm replay-producer            # replay again, 500× the file (100k msgs)
RATE=1000 docker compose run --rm replay-producer             # rate-limited (msgs/s)

docker compose down -v                                        # stop and wipe Iggy data
```

Benchmarks (they wipe Iggy; results in [MEASUREMENTS.md](MEASUREMENTS.md)):

```bash
python3 bench/source_throughput.py --repeat 1 --data-dir ./data --csv <day>.csv
python3 bench/jdbc_sink_test.py --partitions 1 --data-dir ./data --csv <day>.csv   # JDBC sink → TimescaleDB
```

Options are set with env vars: `IGGY_PARTITIONS` (default 1; the connector has one watermark per reader, so more partitions make events late), `REPEAT`, `RATE`, `DATA_DIR` (default `./docs`),
and `CHECKPOINT_INTERVAL` (default `10 s`).

## Findings (2026-11-16)

| Test | Result |
|---|---|
| 200 rows, 2 TMs | 200/200 received, no duplicates or gaps. Per-partition counts match Iggy's. |
| 100k rows while running | 100,200/100,200, no duplicates or gaps |
| keyBy across TMs | Works: about half of the records cross to the other TM |
| **Split distribution** | **All 4 partitions go to one source subtask. The other reader stays idle.** The enumerator assigns splits only on `addReader`, so the first reader to register gets every split. The Iggy consumer group shows 1 member. |
| Kill the source TM, **no checkpointing** | **Job reports RUNNING but consumes nothing.** `addSplitsBack` loses the splits: `IggySourceSplit.equals` includes `currentOffset`, and `sentToReaders` is never cleared. |
| Kill the source TM, checkpointing on | Recovers. Splits move to the other TM, offsets continue with no gaps or duplicates. |

More notes from reading the connector source:
- The reader polls with `PollingStrategy.next()` and `autoCommit=true`, so Iggy consumer-group offsets are
  committed on poll, and offsets stored in the Flink checkpoint are not used for reading. This is at-most-once
  delivery, not exactly-once: records polled but not processed before a crash would be skipped. This was not
  reproduced here because the job was idle when the TM was killed.
- The consumer group must already exist, because the connector does not create it (`iggy-setup` creates it).
- The connector logs every message at INFO. The Flink image sets `org.apache.iggy` to WARN.
- `isAvailable()` always returns a completed future, so an idle source busy-polls Iggy.

## Single-reader throughput (2026-11-16)

`python3 bench/source_throughput.py --repeat 10000` (wipes Iggy). Setup: 2M messages (~110 B CSV rows),
4 partitions, one 16-core host, checkpointing every 10 s. Only one source reader is ever active (see above).

| Run | Result |
|---|---|
| Producer alone (batch 4000 per send) | 2M in 9.5 s ≈ **212k msg/s** |
| Drain backlog, source only, poll batch 100 | **~210k msg/s** (9 s) |
| Drain backlog, source only, poll batch 1000 | ~540k msg/s (3 s, coarse) |
| Drain backlog, source only, poll batch 5000 | ~735k msg/s (2 s, coarse) |
| Live: full pipeline + producer at max, poll batch 5000 | 153k msg/s, kept up with the producer; 2M/2M, no duplicates |

- Counters report once per second, so the 2–3 s drains are accurate to only about ±30–50%. The ranking is reliable.
- The connector's default poll batch (100) costs about 3× in throughput. Set `IGGY_POLL_BATCH` to at least 1000.
- The idle reader's TaskManager uses about 100% of a core with no data: `isAvailable()` always returns a
  completed future, so the reader busy-polls.
- In the live run the producer was the limit, not the reader.

Conclusion: the connector works with several Flink nodes, but source reading does not scale out (one reader
gets all partitions) and it is not exactly-once. To get parallel reading and exactly-once, use a fixed
enumerator (assign splits round-robin as readers register, and seek to the checkpointed offset) or the
custom-source fallback from architecture.md §7.
