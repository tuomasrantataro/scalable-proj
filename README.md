# scalable-proj

Detecting trading trends in the DEBS 2022 tick data: Kafka → Flink → TimescaleDB
(see [`docs/architecture.md`](docs/architecture.md)). The Flink job computes EMA38/EMA100 per symbol and
5-minute window, BUY/SELL crossover advisories, and a report of suspicious input data.

| Service | What it does |
|---|---|
| `kafka` | Apache Kafka 4.3.2, one broker in KRaft mode (`kafka:9092` inside Compose, `localhost:9094` from the host) |
| `kafka-setup` | One-shot: creates the input topic `ticks` (8 partitions, `KAFKA_PARTITIONS`) and the output topic `advisories` (4 partitions, `KAFKA_ADVISORIES_PARTITIONS`; BUY/SELL advisories as JSON from `PIPELINE=ema`, keyed by symbol). Existing topics are kept. |
| `jobmanager` | Flink 2.3.0, standalone application mode, runs `scalable.flink.TradingJob`; `PIPELINE` picks the pipeline: `ema` (default), `jdbc` (UI: http://localhost:8081) |
| `taskmanager` ×4 | 1 slot each (`TASKMANAGERS`), job parallelism 4 (`PARALLELISM`; the source can use up to the partition count of `ticks`) |
| `timescaledb` | TimescaleDB 2.31 (PostgreSQL 17), [schema](config/timescaledb/init.sql): `ema_window` + view `advisories`, `data_issue` + view `data_issue_summary` (`PIPELINE=ema`), `window_close` (`PIPELINE=jdbc`) |
| `db-setup` | One-shot: applies the idempotent schema (also to an existing database volume) |
| `replay-producer` | Sends CSV rows to `ticks`, one record per row with the symbol ID as key, partitioned by symbol (Rust with librdkafka, `producer-rs/`; lz4-compressed batches) |
| `grafana` | Dashboards at http://localhost:3001 (admin/admin; anonymous viewers allowed): Trading trends, Correctness and data quality, System metrics ([architecture §5.1](docs/architecture.md#51-grafana-dashboards)) |
| `prometheus`, `cadvisor` | Metrics: Flink Prometheus reporter (port 9249 on every Flink container) and CPU/memory per container. Prometheus UI: http://localhost:9090 |

Job topology, `PIPELINE=ema` ([code](flink-job/src/main/java/scalable/flink/ema/)):

```
KafkaSource → parse price rows → watermarks (OUT_OF_ORDERNESS_S, IDLENESS_S, WATERMARK_ALIGNMENT_S)
  → quality gate ─────────────────────────────────────────────┐ zero prices: dropped, counted
  → keyBy(symbol) → 5-min event-time window: close = latest event time ── late events ──┤
  → keyBy(symbol) → EMA function: EMA38, EMA100, BUY/SELL (keyed state) ── price jumps ──┤
  ├→ TimescaleDB ema_window (upsert)                           TimescaleDB data_issue ◄─┘
  └→ BUY/SELL only → subscription filter → Kafka topic `advisories` (one JSON record per trader and advisory)
```

Sink test pipeline: `jdbc` (5-min closes into `window_close`).

Connector: `org.apache.flink:flink-connector-kafka:5.0.0-2.2` (built for Flink 2.2; tested on 2.3, see
[MEASUREMENTS.md](MEASUREMENTS.md)). Each partition of `ticks` has its own watermark, partitions are spread over
all source subtasks, and the read offsets are part of Flink's checkpoints.

Until 2026-11-20 the broker was Apache Iggy 0.10. Its tests, measurements and the reasons for the switch are in
[`docs/iggy.md`](docs/iggy.md).

## Run

```bash
docker compose up --build -d
docker compose logs -f taskmanager | grep scalable.report     # per-node counters

REPEAT=500 docker compose run --rm replay-producer            # replay again, 500× the file (100k msgs)
RATE=1000 docker compose run --rm replay-producer             # rate-limited (msgs/s)

docker compose down -v                                        # stop and wipe Kafka, database and metrics data
```

Whole week through the EMA job (`KAFKA_TOPIC` / `KAFKA_GROUP` select another topic or a fresh consumer group):

```bash
export DATA_DIR=./data
docker compose up -d --build jobmanager taskmanager
for d in 08 09 10 11 12 13 14; do
  CSV_FILE=/data/debs2022-gc-trading-day-$d-11-21.csv docker compose run --rm --no-deps replay-producer
done
docker compose exec timescaledb psql -U postgres -d trading -c 'SELECT * FROM data_issue_summary'
docker compose exec timescaledb psql -U postgres -d trading -c 'SELECT type, count(*) FROM advisories GROUP BY 1'
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic advisories --from-beginning --max-messages 5 --property print.key=true   # first advisories
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group flink                                       # job offsets and lag per partition
```

Advisories are delivered per simulated trader, from [`config/subscriptions.txt`](config/subscriptions.txt)
(`trader: SYMBOL SYMBOL ...` or `trader: *`), read when the job starts. To change it, edit the file and restart
the job (`docker compose up -d --force-recreate jobmanager taskmanager`). Note: this starts a new job, so the
EMA state starts from 0 again (no savepoint restore yet). EMAs are still computed and stored for every symbol.

Dashboards are provisioned from `config/grafana/` (data sources, and dashboards as JSON). Edits made in the UI
are lost when Grafana restarts: export the dashboard JSON into `config/grafana/dashboards/` to keep them. The
market dashboards show times in UTC, which is how local exchange time is stored, and default to the dataset's week.

Flink unit tests run in the image build (`docker compose build jobmanager`).

Throughput and scaling runs without the producer in the measurement: load the input into Kafka once, then
replay the stored backlog into the job as often as needed. Each `run` empties `ema_window` and `data_issue`,
starts the job with a fresh consumer group, waits until the backlog is read, and prints the time, read rate,
busy time per task, CPU per container and a digest of the results (also appended to
`bench/results/backlog_runs.jsonl`). The backlog stays in Kafka until the next `load` or `docker compose down -v`.

```bash
python3 bench/backlog.py load --data-dir ./data                    # whole week, ~4 min (--days 08 for one day)
python3 bench/backlog.py run                                       # 4 TaskManagers, parallelism 4
python3 bench/backlog.py run --taskmanagers 8 --parallelism 8
python3 bench/backlog.py run --set WATERMARK_ALIGNMENT_S=0         # any job setting from docker-compose.yml
```

JDBC sink and failover test (wipes Kafka and TimescaleDB; results in [MEASUREMENTS.md](MEASUREMENTS.md)):

```bash
python3 bench/jdbc_sink_test.py --data-dir ./data --csv <day>.csv
```

Options are set with env vars: `KAFKA_PARTITIONS` (8) and `KAFKA_RETENTION_MS` (-1 = keep forever), used only
when `kafka-setup` creates the topic; `REPEAT`, `RATE`, `KAFKA_COMPRESSION` (lz4), `KAFKA_LINGER_MS` (20) and
`PRODUCER_THREADS` (8) for the producer; `DATA_DIR` (default `./docs`); `TASKMANAGERS` (4), `PARALLELISM` (4),
`KAFKA_MAX_POLL_RECORDS` (5000), `CHECKPOINT_INTERVAL` (default `10 s`), `OUT_OF_ORDERNESS_S` (60), `IDLENESS_S`
(10) and `WATERMARK_ALIGNMENT_S` (300; pauses a partition that is that far ahead in event time, see
[architecture §3.4](docs/architecture.md#34-watermarks-and-late-events-nfr-7)) for the job; and for `PIPELINE=ema`:
`SKEW_REPORT_S` (3600) and `PRICE_JUMP_REPORT` (0.10) for the data quality report, and `SUBSCRIPTIONS_FILE`
(`/config/subscriptions.txt`).
