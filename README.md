# scalable-proj

Detecting trading trends in the DEBS 2022 tick data: Iggy → Flink → TimescaleDB
(see [`docs/architecture.md`](docs/architecture.md)). The Flink job computes EMA38/EMA100 per symbol and
5-minute window, BUY/SELL crossover advisories, and a report of suspicious input data.

| Service | What it does |
|---|---|
| `iggy` | Apache Iggy 0.10.0 server (TCP 8090, HTTP 3000) |
| `iggy-setup` | One-shot: creates stream `trading`, topic `ticks` (1 partition by default, see below), consumer group `flink`, and output topic `advisories` (1 partition, `IGGY_ADVISORIES_PARTITIONS`; BUY/SELL advisories as JSON from `PIPELINE=ema`) |
| `jobmanager` | Flink 2.3.0, standalone application mode, runs `scalable.flink.ConnectorTestJob`; `PIPELINE` picks the pipeline: `ema` (default), `full`, `count`, `jdbc` (UI: http://localhost:8081) |
| `taskmanager` ×2 | 1 slot each, job parallelism 2 |
| `timescaledb` | TimescaleDB 2.31 (PostgreSQL 17), [schema](config/timescaledb/init.sql): `ema_window` + view `advisories`, `data_issue` + view `data_issue_summary` (`PIPELINE=ema`), `window_close` (`PIPELINE=jdbc`) |
| `db-setup` | One-shot: applies the idempotent schema (also to an existing database volume) |
| `replay-producer` | Sends CSV rows to `ticks`, partitioned by symbol ID (Rust, `producer-rs/`) |
| `grafana` | Dashboards at http://localhost:3001 (admin/admin; anonymous viewers allowed): Trading trends, Correctness and data quality, System metrics ([architecture §5.1](docs/architecture.md#51-grafana-dashboards)) |
| `prometheus`, `cadvisor` | Metrics: Flink Prometheus reporter (port 9249 on every Flink container) and CPU/memory per container. Prometheus UI: http://localhost:9090 |

Job topology, `PIPELINE=ema` ([code](flink-job/src/main/java/scalable/flink/ema/)):

```
IggySource → parse price rows → watermarks (OUT_OF_ORDERNESS_S, IDLENESS_S)
  → quality gate ─────────────────────────────────────────────┐ zero prices: dropped, counted
  → keyBy(symbol) → 5-min event-time window: close = latest event time ── late events ──┤
  → keyBy(symbol) → EMA function: EMA38, EMA100, BUY/SELL (keyed state) ── price jumps ──┤
  ├→ TimescaleDB ema_window (upsert)                           TimescaleDB data_issue ◄─┘
  └→ BUY/SELL only → subscription filter → Iggy topic `advisories` (one JSON message per trader and advisory)
```

Connector test pipelines: `full` (source → per-node counter → keyBy counter, plus a verifier for lost or
duplicated messages), `count` (source only), `jdbc` (5-min closes into `window_close`).

Connector: `org.apache.iggy:flink-connector:0.10.0` (built against Flink 2.3.0).

## Run

```bash
docker compose up --build -d
docker compose logs -f taskmanager | grep scalable.report     # per-node counters

REPEAT=500 docker compose run --rm replay-producer            # replay again, 500× the file (100k msgs)
RATE=1000 docker compose run --rm replay-producer             # rate-limited (msgs/s)

docker compose down -v                                        # stop and wipe Iggy data
```

Whole week through the EMA job (about 6 min; `IGGY_TOPIC` / `IGGY_CONSUMER_GROUP` select a fresh topic or group):

```bash
export DATA_DIR=./data IGGY_POLL_BATCH=5000
docker compose up -d --build jobmanager taskmanager
for d in 08 09 10 11 12 13 14; do
  CSV_FILE=/data/debs2022-gc-trading-day-$d-11-21.csv docker compose run --rm --no-deps replay-producer
done
docker compose exec timescaledb psql -U postgres -d trading -c 'SELECT * FROM data_issue_summary'
docker compose exec timescaledb psql -U postgres -d trading -c 'SELECT type, count(*) FROM advisories GROUP BY 1'
docker compose exec iggy iggy -u iggy -p iggy --tcp-server-address 127.0.0.1:8090 \
    message poll --offset 0 -m 5 trading advisories 0                     # first advisories from the topic
```

Advisories are delivered per simulated trader, from [`config/subscriptions.txt`](config/subscriptions.txt)
(`trader: SYMBOL SYMBOL ...` or `trader: *`), read when the job starts. To change it, edit the file and restart
the job (`docker compose up -d --force-recreate jobmanager taskmanager`). Note: this starts a new job, so the
EMA state starts from 0 again (no savepoint restore yet). EMAs are still computed and stored for every symbol.

Dashboards are provisioned from `config/grafana/` (data sources, and dashboards as JSON). Edits made in the UI
are lost when Grafana restarts: export the dashboard JSON into `config/grafana/dashboards/` to keep them. The
market dashboards show times in UTC, which is how local exchange time is stored, and default to the dataset's week.

Flink unit tests run in the image build (`docker compose build jobmanager`).

Benchmarks (they wipe Iggy; results in [MEASUREMENTS.md](MEASUREMENTS.md)):

```bash
python3 bench/source_throughput.py --repeat 1 --data-dir ./data --csv <day>.csv
python3 bench/jdbc_sink_test.py --partitions 1 --data-dir ./data --csv <day>.csv   # JDBC sink → TimescaleDB
```

Options are set with env vars: `IGGY_PARTITIONS` (default 1; the connector has one watermark per reader, so more partitions make events late), `REPEAT`, `RATE`, `DATA_DIR` (default `./docs`),
`CHECKPOINT_INTERVAL` (default `10 s`), and for `PIPELINE=ema`: `OUT_OF_ORDERNESS_S` (60), `IDLENESS_S` (10),
`SKEW_REPORT_S` (3600) and `PRICE_JUMP_REPORT` (0.10) for the data quality report, `SUBSCRIPTIONS_FILE`
(`/config/subscriptions.txt`) and `IGGY_ADVISORY_BATCH` (1).

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
