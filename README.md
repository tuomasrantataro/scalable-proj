# scalable-proj

Detecting trading trends in the DEBS 2022 tick data: Kafka → Flink → TimescaleDB
(see [`docs/architecture.md`](docs/architecture.md)). The Flink job computes EMA38/EMA100 per symbol and
5-minute window, BUY/SELL crossover advisories, and a report of suspicious input data.

| Service | What it does |
|---|---|
| `kafka` | Apache Kafka 4.3.2, one broker in KRaft mode (`kafka:9092` inside Compose, `localhost:9094` from the host) |
| `kafka-setup` | One-shot: creates the input topic `ticks` (8 partitions, `KAFKA_PARTITIONS`) and the output topic `advisories` (4 partitions, `KAFKA_ADVISORIES_PARTITIONS`; BUY/SELL advisories as JSON, keyed by symbol; Kafka timestamp = broker append time). Existing topics are kept. |
| `jobmanager` | Flink 2.3.0, standalone application mode, runs `scalable.flink.TradingJob` (UI: http://localhost:8081) |
| `taskmanager` ×2 | `TASKMANAGERS` (2) containers with `TASK_SLOTS` (1) slots each; job parallelism `PARALLELISM` (2; the source can use up to the partition count of the input topic) |
| `timescaledb` | TimescaleDB 2.31 (PostgreSQL 17), [schema](config/timescaledb/init.sql): `ema_window` + view `advisories`, `data_issue` + view `data_issue_summary` |
| `db-setup` | One-shot: applies the idempotent schema (also to an existing database volume) |
| `replay-producer` | Sends CSV rows to the input topic, one record per row with the symbol ID as key, partitioned by symbol (Rust with librdkafka, `producer-rs/`; lz4-compressed batches, 4 sender threads). Not started by `docker compose up` (Compose profile `producer`); the scripts below run it. |
| `advisory-latency` | Profile `latency`: reads topic `advisories` like a trader's system and prints each message with its receive time; started and evaluated by `bench/advisory_latency.py` |
| `grafana` | Dashboards at http://localhost:3001 (admin/admin; anonymous viewers allowed): Trading trends, Correctness and data quality, System metrics ([architecture §5.1](docs/architecture.md#51-grafana-dashboards)) |
| `prometheus`, `cadvisor` | Metrics: Flink Prometheus reporter (port 9249 on every Flink container) and CPU/memory per container. Prometheus UI: http://localhost:9090 |

Job topology ([code](flink-job/src/main/java/scalable/flink/ema/)):

```
KafkaSource → parse price rows → watermarks (OUT_OF_ORDERNESS_S, IDLENESS_S, WATERMARK_ALIGNMENT_S)
  → quality gate ─────────────────────────────────────────────┐ zero prices: dropped, counted
  → keyBy(symbol) → 5-min event-time window: close = latest event time ── late events ──┤
  → keyBy(symbol) → EMA function: EMA38, EMA100, BUY/SELL (keyed state) ── price jumps ──┤
  ├→ TimescaleDB ema_window (upsert)                           TimescaleDB data_issue ◄─┘
  └→ BUY/SELL only → subscription filter → Kafka topic `advisories` (one JSON record per trader and advisory)
```

Connector: `org.apache.flink:flink-connector-kafka:5.0.0-2.2` (built for Flink 2.2; tested on 2.3, see
[MEASUREMENTS.md](MEASUREMENTS.md)). Each partition of `ticks` has its own watermark, partitions are spread over
all source subtasks, and the read offsets are part of Flink's checkpoints.

Until 2026-11-20 the broker was Apache Iggy 0.10. Its tests, measurements and the reasons for the switch are in
[`docs/iggy.md`](docs/iggy.md).

## Quick start

Needs Docker with Compose v2 and Python 3 (standard library only). The DEBS 2022 CSV files go in `./data`
(`debs2022-gc-trading-day-08-11-21.csv` … `-14-11-21.csv`); `docs/pala.csv` is a 200-row sample.

### 1. Run the whole system

```bash
scripts/run-live.sh                      # all services incl. Grafana, then replays the whole week live
DAYS="08" scripts/run-live.sh            # only one trading day (about 3 min)
```

The script removes earlier results, starts every service (Kafka, TimescaleDB, Flink, Grafana, Prometheus,
cAdvisor), replays the CSV files into Kafka while the job processes them, waits until everything is processed
and prints a summary. It uses its own topic `live`, so events loaded in step 2 are kept. The system keeps
running afterwards:

- Grafana: http://localhost:3001 (admin/admin): Trading trends, Correctness and data quality, System metrics
- Flink UI: http://localhost:8081, Prometheus: http://localhost:9090

To only start the services, without sending data: `docker compose up -d --build`.

### 2. Load the events into Kafka (once)

```bash
python3 bench/backlog.py load --data-dir ./data          # whole week: 289 M events, about 4 min, 5.6 GB
python3 bench/backlog.py load --data-dir ./data --days 08 --partitions 16   # one day, 16 partitions
```

Stops Flink, recreates topic `ticks` (8 partitions by default) and fills it with the producer. The events stay in
Kafka until the next `load` or a tear-down with `-v` (step 4), also across `docker compose down` and restarts.

### 3. Benchmark different scalings

Each `run` resets the system, replays the loaded events into the job at full speed and measures it:

```bash
python3 bench/backlog.py run                                         # 4 TaskManagers × 1 slot, parallelism 4
python3 bench/backlog.py run --taskmanagers 1 --parallelism 1
python3 bench/backlog.py run --taskmanagers 8 --parallelism 8
python3 bench/backlog.py run --taskmanagers 2 --slots 4 --parallelism 8
python3 bench/backlog.py run --set JDBC_BATCH_SIZE=5000              # any job setting from docker-compose.yml
python3 bench/backlog.py reset                                       # only reset (keeps the loaded events)
```

The reset (also done at the start of every `run`) stops the job and removes all results: the tables
`ema_window` and `data_issue`, the topic `advisories` and the Flink checkpoints. The loaded events
are kept. The run then starts a new job (EMA state from 0) with a fresh consumer group, so it reads every event
from the beginning, waits until all are processed, and prints the time, read rate, busy and backpressured time per
task, CPU per container and a digest of the results; each run is also appended to
`bench/results/backlog_runs.jsonl`. For the whole week the digest must be `19413a50…` ("= reference week" in the
output); a different digest means the results are wrong, not just slower.

Each `run` (and `scripts/run-live.sh`) also measures advisory latency with
[`bench/advisory_latency.py`](bench/advisory_latency.py): the `advisory-latency` service reads topic `advisories`
like a trader's system and records when each message arrives. Reported as p50/p95/p99/max per stage: window fired
(`fired_at`) → EMA result (`emitted_at`) → stored in Kafka (broker append time) → received; summaries go to
`bench/results/advisory_latency.jsonl`.

Load/latency curve ([`bench/latency_sweep.py`](bench/latency_sweep.py)): replays the same slice of a day
(default 8 Nov 09:00–09:30, 3.9 M rows) into a fresh job at several speed-ups of the feed's own clock, from real
time (1×, about 2,200 events/s) to the producer's maximum, with every symbol subscribed, and draws latency
p50/p95/p99 against the achieved input rate (`bench/results/latency_sweep/<time>/latency.html`). The default sweep
takes about an hour (the real-time point alone takes 30 min):

```bash
python3 bench/latency_sweep.py                                # speeds 1,3,10,30,100,300,max
python3 bench/latency_sweep.py --speeds 10,30,100,300,0       # without the slow real-time points
```

The producer's `SPEEDUP` (pace by the feed's clock, 1 = real time) and `START_TIME`/`END_TIME` (HH:MM, what the feed
delivered in that time; rows with a stray `Time` do not count, see [`feed.rs`](producer-rs/src/feed.rs)) can also be
used directly, e.g. `SPEEDUP=10 START_TIME=09:00 END_TIME=10:00 CSV_FILE=/data/<day>.csv`.

Scalability scripts (NFR-4 to NFR-6). Each writes `summary.md` and the raw results to `bench/results/<script>/<time>/`,
updated after every run:

```bash
python3 bench/scaling_matrix.py      # configurations × 3 repeats of `backlog.py run`: median, spread, speedup, efficiency
python3 bench/subscriber_sweep.py    # 1 to 100 traders (every symbol); checks every delivery
python3 bench/peak_test.py           # busiest minute overall and per exchange, at 10x, 30x, 100x and maximum speed
python3 bench/peak_test.py --find    # only find the busiest minutes
python3 bench/db_limit.py realistic  # database load at real traffic; also `bursts` and `saturation`: where it becomes the limit
```

`scaling_matrix.py` and `db_limit.py saturation` need the loaded events (step 2); the others replay slices cut from the CSV files
([`bench/workload.py`](bench/workload.py), kept in `data/slices`).

Long tail: each `run` records how evenly the load is spread (records per partition, records and busy time per subtask,
each with max/mean) and the memory per service. To compare with evenly spread symbols, load the same rows with 2,047
equally frequent symbols (as many non-empty windows as the real week) into topic `ticks-uniform`; the results are
meaningless as market data, only the load is comparable:

```bash
python3 bench/symbol_distribution.py                              # offline: concentration, hash balance, symbols.csv
python3 bench/backlog.py load --data-dir ./data --workload uniform
python3 bench/backlog.py run --workload uniform                   # also: scaling_matrix.py --workload uniform
```

Correctness: compare the job's results in TimescaleDB with the pandas reference, bit for bit
([`reference/`](reference/README.md)):

```bash
reference/.venv/bin/python reference/compare.py                        # after a whole-week run
reference/.venv/bin/python reference/compare.py --through 2021-11-08   # after a run of day 8 only
python3 bench/check_delivery.py                                        # delivered advisories, after a whole-week run
```

Every measurement for the report: [FINAL_MEASUREMENTS.md](FINAL_MEASUREMENTS.md), unattended with `scripts/run-final.sh`.

What can be scaled:

| Component | How | Notes |
|---|---|---|
| Flink TaskManagers | `--taskmanagers N` (`TASKMANAGERS`) | containers; each is a JVM |
| Slots per TaskManager | `--slots N` (`TASK_SLOTS`) | TaskManagers × slots must be ≥ parallelism |
| Job parallelism | `--parallelism N` (`PARALLELISM`) | all operators; the source uses at most one subtask per partition |
| Kafka partitions of the input | `load --partitions N` | the only one that needs a new `load`; upper bound for source parallelism |
| Producer threads (live runs only) | `PRODUCER_THREADS=N scripts/run-live.sh` | 4 by default; see MEASUREMENTS.md §3 |
| Kafka broker, TimescaleDB | – | single instances in this setup |

For comparable benchmarks, change one thing at a time on the same loaded events, and compare runs on the same host
with nothing else running. Results and the current limits are in [MEASUREMENTS.md](MEASUREMENTS.md) §5.

### 4. Tear down

```bash
docker compose --profile producer down -v --remove-orphans   # containers, network and all data volumes
```

This deletes everything, including the loaded events, the database, the dashboards' data and the metrics. To stop
the system but keep the data, use `docker compose down` (without `-v`); `docker compose up -d` starts it again.

## More

Inspect the results:

```bash
docker compose exec timescaledb psql -U postgres -d trading -c 'SELECT * FROM data_issue_summary'
docker compose exec timescaledb psql -U postgres -d trading -c 'SELECT type, count(*) FROM advisories GROUP BY 1'
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic advisories --from-beginning --max-messages 5 --property print.key=true   # first advisories
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group <group>                                     # job offsets and lag per partition
docker compose logs -f taskmanager | grep scalable.report         # per-subtask counters
```

Send data by hand: `CSV_FILE=/data/<file> DATA_DIR=./data docker compose run --rm replay-producer` (with
`RATE=<msgs/s>` for a fixed rate, `REPEAT=<n>` to repeat the file; topic `KAFKA_TOPIC`, default `ticks`).

Advisories are delivered per simulated trader, from [`config/subscriptions.txt`](config/subscriptions.txt)
(`trader: SYMBOL SYMBOL ...` or `trader: *`), read when the job starts. To change it, edit the file and restart
the job (`docker compose up -d --force-recreate jobmanager taskmanager`). Note: this starts a new job, so the
EMA state starts from 0 again (no savepoint restore yet). EMAs are still computed and stored for every symbol.

Dashboards are provisioned from `config/grafana/` (data sources, and dashboards as JSON). Edits made in the UI
are lost when Grafana restarts: export the dashboard JSON into `config/grafana/dashboards/` to keep them. The
market dashboards show times in UTC, which is how local exchange time is stored, and default to the dataset's week.

Flink unit tests run in the image build (`docker compose build jobmanager`).

Options are set with env vars: `KAFKA_PARTITIONS` (8) and `KAFKA_RETENTION_MS` (-1 = keep forever), used only
when `kafka-setup` creates the topic; `REPEAT`, `RATE`, `KAFKA_COMPRESSION` (lz4), `KAFKA_LINGER_MS` (20) and
`PRODUCER_THREADS` (4) for the producer; `DATA_DIR` (default `./docs`); `TASKMANAGERS` (2), `TASK_SLOTS` (1),
`PARALLELISM` (2), `KAFKA_TOPIC` (`ticks`), `KAFKA_GROUP` (`flink`),
`KAFKA_MAX_POLL_RECORDS` (5000), `CHECKPOINT_INTERVAL` (default `10 s`), `OUT_OF_ORDERNESS_S` (1), `IDLENESS_S`
(10), `JDBC_BATCH_SIZE` (1000), `JDBC_URL` (with `reWriteBatchedInserts=true`), `EMA_SINK` (`jdbc`; `discard` does not write
`ema_window`, only for measuring the cost of the database) and `WATERMARK_ALIGNMENT_S` (300; pauses a partition that is that far ahead in event time, see
[architecture §3.4](docs/architecture.md#34-watermarks-and-late-events-nfr-7)) for the job;
`SKEW_REPORT_S` (3600) and `PRICE_JUMP_REPORT` (0.10) for the data quality report, and `SUBSCRIPTIONS_FILE`
(`/config/subscriptions.txt`).
