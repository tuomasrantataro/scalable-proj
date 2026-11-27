# scalable-proj

Detecting trading trends in the DEBS 2022 tick data: Kafka → Flink → TimescaleDB. The Flink job computes EMA38 and
EMA100 per symbol and 5-minute window, emits BUY/SELL advisories at their crossovers, delivers them to the subscribed
traders through Kafka, and reports suspicious input data. Details: [`docs/architecture.md`](docs/architecture.md).

## 1. System requirements

| | Needed |
|---|---|
| **OS and software** | Linux, Docker with Compose v2, Python 3 (standard library only), `curl` |
| **CPU** | 8 cores / 16 threads recommended for 4 TaskManagers; everything runs on one host, no GPU is used |
| **Memory** | 16 GB minimum, 32 GB recommended (the containers use 6–7 GB with 4 TaskManagers) |
| **Disk** | 24 GB for the CSV files and about 30 GB free for Kafka, the database and the Docker images |
| **Free ports** | 3001 (Grafana), 5432 (TimescaleDB), 8081 (Flink UI), 9090 (Prometheus), 9094 (Kafka) |
| **Dataset** | The seven files `debs2022-gc-trading-day-08-11-21.csv` … `-14-11-21.csv` of the DEBS 2022 Grand Challenge (https://doi.org/10.5281/zenodo.6382482) in `./data`. A 200-row sample is in `docs/pala.csv`. |

## 2. System components

| Service | What it does |
|---|---|
| `replay-producer` | Rust program (librdkafka) that sends the CSV rows to Kafka, one record per row, keyed and partitioned by symbol. Sends as fast as possible, or at a multiple of the feed's own pace (`SPEEDUP=1` is real time). Runs only when called (Compose profile `producer`). |
| `kafka` | Apache Kafka 4.3.2, one broker in KRaft mode. Input topic `ticks` (8 partitions; `live` for live runs) and output topic `advisories` (4 partitions, JSON). `localhost:9094` from the host. |
| `kafka-setup` | One-shot: creates the topics. |
| `jobmanager` | Flink 2.3.0 in application mode; runs the job `scalable.flink.TradingJob` and coordinates checkpoints. UI: http://localhost:8081 |
| `taskmanager` | Flink workers running the job: parse and quality gate → 5-minute event-time windows → EMA38/EMA100 and BUY/SELL crossovers → subscription filter. `TASKMANAGERS` (2) containers with `TASK_SLOTS` (1) slots each, job parallelism `PARALLELISM` (2). |
| `timescaledb` | TimescaleDB 2.31 (PostgreSQL 17): every window result (`ema_window`, view `advisories`) and the data quality issues (`data_issue`). Port 5432. |
| `db-setup` | One-shot: creates the database schema. |
| `advisory-latency` | Reads topic `advisories` as a trader's system would and records when each advisory arrives (Compose profile `latency`). |
| `grafana` | Dashboards at http://localhost:3001 (admin/admin; viewing without login allowed): Trading trends, Correctness and data quality, System metrics. |
| `prometheus`, `cadvisor` | Metrics of Flink and CPU/memory of every container. Prometheus UI: http://localhost:9090 |

Advisories are delivered per simulated trader, as listed in [`config/subscriptions.txt`](config/subscriptions.txt)
(`trader: SYMBOL SYMBOL ...`, or `trader: *` for every symbol). EMAs and advisories are computed and stored for every
symbol either way.

## 3. Running the system

Run the commands in the repository root.

### Copy the dataset files to ./data
First, copy the test dataset manually to ./data folder. The file `pala.csv` is already there as an example. Contents of that folder should be similar to this:
```
.
├── data
│   ├── debs2022-gc-trading-day-08-11-21.csv
│   ├── debs2022-gc-trading-day-09-11-21.csv
│   ├── debs2022-gc-trading-day-10-11-21.csv
│   ├── debs2022-gc-trading-day-11-11-21.csv
│   ├── debs2022-gc-trading-day-12-11-21.csv
│   ├── debs2022-gc-trading-day-13-11-21.csv
│   ├── debs2022-gc-trading-day-14-11-21.csv
│   ├── pala.csv
```

### Start the system and submit the test dataset
This command takes about 5-10 minutes on author's computer. To see visual progress, open Grafana at `localhost:3001` and set refresh rate from the `Refresh` button at top right corner.

```bash
export TASKMANAGERS=4 PARALLELISM=4 PRODUCER_THREADS=8    # optional; the defaults are 2, 2 and 4
scripts/run-live.sh                                       # the whole week, about 5 minutes
```

The script builds the images, starts every service, replays the CSV files into Kafka at full speed while the job
processes them, waits until everything is processed and prints a summary with the advisory latency. Results of an
earlier run are removed first. Options:

```bash
DAYS="08" scripts/run-live.sh                                       # one trading day
SUBSCRIPTIONS_FILE=/config/subscriptions-all.txt scripts/run-live.sh  # one trader subscribed to every symbol
DATA_DIR=./docs FILES=pala.csv scripts/run-live.sh                  # the 200-row sample
```

To only start the services, without sending data: `docker compose up -d --build`.

### Look at the results

The system keeps running after the replay. Open Grafana at http://localhost:3001; the market dashboards show local
exchange time as UTC and default to the dataset's week (8–14 Nov 2021).

Alternatively check data straigth from the containers.

```bash
docker compose exec timescaledb psql -U postgres -d trading -c 'SELECT type, count(*) FROM advisories GROUP BY 1'
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic advisories --from-beginning --max-messages 5 --property print.key=true
```

For the whole week the result is 1,855,645 windows with 7,769 BUY and 3,580 SELL advisories. To check them against the
pandas reference implementation (needs `pip install -r reference/requirements.txt`, see
[`reference/README.md`](reference/README.md)): `python3 reference/compare.py`.

### Tear down

```bash
docker compose stop                                                         # stop, keep everything
docker compose --profile producer --profile latency down                    # remove the containers, keep the data
docker compose --profile producer --profile latency down -v --remove-orphans  # remove everything, including all data
```

Stopped containers are started again with `docker compose start`. Without `-v`, the database, Kafka topics and
metrics are kept in their Docker volumes. The CSV files in `./data` are never touched.
