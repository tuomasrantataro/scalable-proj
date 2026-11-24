# Final measurements for the report

Every measurement the report needs, step by step from an empty system (part A), and how to repeat single steps without
starting over (part B). What the system is: [`docs/architecture.md`](docs/architecture.md).

## What you need

| | Needed | Notes |
|---|---|---|
| **RAM** | **16 GB minimum, 32 GB recommended** | Containers: 6–7 GB with 4 TaskManagers (measured), about 11 GB with 8 (estimate). The reference (A3) takes about 6 GB per file it reads at once: 12 GB by default, 6 GB with `--jobs 1`. Measured on a host with 35 GB |
| **Disk** | **75 GB**: 24 GB CSV files + about 50 GB free | Kafka up to 20 GB, Docker images and build cache up to 20 GB, slices about 9 GB, the rest 2 GB |
| **Time** | **about 10 hours**, unattended with `scripts/run-final.sh` | Table below. The smoke tests (A1–A4) take about 45 min |
| **CPU** | 8 cores / 16 threads for the 8-TaskManager runs | Everything runs on one host. No GPU is used |
| **Software** | Docker with Compose v2, Python 3.11 or newer, Linux | Free ports: 3001, 5432, 8081, 9090, 9094 |

Times are from earlier runs on the 16-thread laptop, or estimates from them; other hosts differ.

| Step | What | Time |
|---|---|---:|
| A1, A2 | Preparation, build, start | 15 min |
| A3 | Reference, event types, symbol distribution (offline) | 15 min |
| A4 | Smoke tests | 15 min |
| A5 | Full week: correctness, event handling, throughput | 15 min |
| A6 | Scaling with resources | 70 min |
| A7 | Long tail: evenly spread symbols | 45 min |
| A8 | Latency under load; delay from the window's end | 135 min |
| A9 | Exchanges and peak rates; sustained load | 100 min |
| A10 | Subscribers | 40 min |
| A10b | Database load and limit | 95 min |
| A10c | Design alternatives; failover | 50 min |
| A11 | Live run, visualization | 15 min |
| | **Total** | **about 10 h** |

## Run everything unattended

```bash
scripts/run-final.sh
```

Checks the host (CSV files, Docker, memory, disk, mains power, ports, nothing uncommitted), asks once, and runs A1–A11 in the
background with sleep inhibited. **It deletes everything the system has stored** (A2); `./data` and `bench/results` are kept.

| | |
|---|---|
| Progress | `scripts/run-final.sh status`, or `tail -f bench/results/final/run.log` |
| Stop, continue | `scripts/run-final.sh stop`; then `scripts/run-final.sh` again: finished steps are kept, failed ones are repeated |
| Begin anew | move `bench/results/final` away |
| Try it first | `scripts/run-final.sh --test`: the same steps, shortened, on 8 Nov (about 1.5 h). Also deletes what is stored |
| Result | `bench/results/final/summary.md`; a log per step in `bench/results/final/logs/` |

Every step has a time limit. A failure in A1–A5 stops the run; a later step is tried twice, then recorded as failed, and the
run goes on. Expected values (digests, counts, deliveries) are checked; other numbers are not judged.

What is left for you:

1. Before: close other programs, plug in, **stay logged in** (locking the screen is fine), keep the network on (the image
   builds look up their base images). Do not edit files of the repository during the run.
2. After about an hour: `status` shows `A5-week: ok`, the correctness results are in.
3. At the end: read `summary.md`, take the screenshots of A11 (the system keeps running), then A12.

Parts A and B below are the same steps by hand.

## Which requirement is covered where

From the project description (PD) and the evaluation criteria (EC), both in `docs/`.

| Requirement | Shown by | Step |
|---|---|---|
| Functionality: correct EMA38 / EMA100 per window (PD 3.2) | Full week equal to the pandas reference, bit for bit | A5, A11 |
| Functionality: correct buy/sell advisories for all subscribed symbols (PD 3.3) | Delivered messages against reference and subscriptions; every symbol in A11 | A5, A11 |
| Windows of 5 min from 00:00, evaluated once the next one starts (PD 3.1) | Grafana "Correctness" checks; delay from the window's end | A8, A11 |
| Performance: the entire week, high throughput | Stored week at full speed; live week | A5, A6, A11 |
| Performance: low and stable latency under load | Load/latency sweep; latency over the run | A8, A9, A11 |
| Scalability: exchanges, event volumes, peak rates | Busiest minute overall and per exchange, up to the maximum | A9 |
| Scalability: long-tail symbol distribution | Distribution, balance over partitions and subtasks, uniform workload | A3, A7 |
| Scalability: more resources (CPU), utilization | 1 to 8 TaskManagers: time, speedup, CPU, memory. GPU: none used, say so | A6 |
| Scalability: more symbols, more subscribers | Up to 80,000 symbols per close; 1 to 1,000 traders | A10, A10b |
| Different event types; tick and housekeeping events; attributes (metadata) | Event types per exchange and security type; what the job did with each | A3, A5 |
| Out-of-order and late events | `LATE` and `TIME_SKEW` counts; results for several bounds | A5, A8 |
| Dataset analysis (EC, academic writing) | Event types, symbol distribution, peaks, disorder | A3, A9 |
| Creative explorations: alternatives backed by measurements | Table in A10c | A3, A6–A8, A10c |
| Smart visualization (PD 3.4) | Screenshots of Grafana with the week's results | A11 |

Not measurements, but required in the report: architecture and implementation text (normalization, metadata, hardware,
tools), individual answers, 4–5 pages in the ACM template. See [`docs/requirements.md`](docs/requirements.md) §5–§6 and
[`NOTES_FOR_REPORT.md`](NOTES_FOR_REPORT.md).

## Rules

- **One measurement at a time, and nothing else running on the host.** Close the editor and the browser (also Grafana
  tabs: their queries load the database). A full memory ends the run.
- **Plug the laptop in.** A1 opens a shell that keeps it awake.
- **Do not change code or settings between runs you want to compare.** The scripts rebuild the images from the working
  tree. Commit first.
- **Check the results of a run right after it:** the next measurement empties the tables and the topic.
- Each script prints where it stores its results: `bench/results/<script>/<time>-<name>/`. Use only the directories named
  `final…` in the report; the others are older.

---

# Part A: everything from scratch

Run all commands in the repository root, in the shell of A1, one after another.

## A1. Preparation

The seven files `debs2022-gc-trading-day-08-11-21.csv` … `-14-11-21.csv` (DEBS 2022, https://doi.org/10.5281/zenodo.6382482)
must be in `./data`:

```bash
ls data/debs2022-gc-trading-day-*.csv | wc -l        # 7
```

Once, create the Python environment:

```bash
python3 -m venv reference/.venv
reference/.venv/bin/pip install -r reference/requirements.txt
```

**Open the measurement shell.** It keeps the laptop awake until you leave it with `exit`. Do this in every new terminal:

```bash
systemd-inhibit --what=idle:sleep:handle-lid-switch bash
source reference/.venv/bin/activate
export TASKMANAGERS=4 PARALLELISM=4 PRODUCER_THREADS=8
```

The last line sets the scaling of the measurements; the defaults in `docker-compose.yml` are smaller (2, 2 and 4). Options of
the scripts (`--configs`, `--taskmanagers`, …) take precedence over it.

Record the host and the version for the report:

```bash
mkdir -p bench/results/final
git rev-parse --short HEAD | tee bench/results/final/host.txt
lscpu | grep -E 'Model name|^CPU\(s\)' | tee -a bench/results/final/host.txt
free -g | tee -a bench/results/final/host.txt
cat /sys/devices/system/clocksource/clocksource0/current_clocksource | tee -a bench/results/final/host.txt
docker --version | tee -a bench/results/final/host.txt
```

(The clock source `hpet` slows the producer, [`MEASUREMENTS.md`](MEASUREMENTS.md) §3.)

## A2. Empty system, build, start

> **This deletes everything the system has stored:** loaded events, results, metrics and Grafana's data. It does not
> touch `./data` or `bench/results`. To keep the loaded events, use part B.

```bash
docker compose --profile producer --profile latency down -v --remove-orphans
docker compose up -d --build
docker compose ps
python bench/backlog.py status
```

Expected: the build runs the unit tests; `kafka` and `timescaledb` are healthy and 4 `taskmanager` run; `status` prints
`job running: trading-trends-ema` (repeat it after some seconds if not) and topic `ticks` with 0 records.

## A3. Offline: reference and data set (no services needed)

```bash
python -m pytest reference -q
python reference/ema_reference.py --exclude-zero-prices --out reference/output-nozero
python reference/compare.py --reference-digest
python bench/event_types.py
python bench/symbol_distribution.py
```

Expected: all tests pass; digest `19413a509d53044973e3e6b76c177af1`; 289,116,808 rows, of which 59,708,052 price events,
80,567,886 quotes, 5,735,874 housekeeping and 143,104,996 without payload.

Results: `bench/results/event_types/summary.md` (event types, comparison with the project description, attributes) and
`bench/results/symbol_distribution/` (concentration, balance). With less than 20 GB of free memory, add `--jobs 1` to
`ema_reference.py`.

Design alternatives of the data handling (each about 5 min): zero prices taken as prices (25,834 advisories instead of
11,349), and late events kept instead of dropped.

```bash
python reference/ema_reference.py --out reference/output
python reference/ema_reference.py --lateness inf --out reference/output-batch
```

## A4. Smoke tests

One day through every kind of script. Fix problems now, not after eight hours.

```bash
python bench/backlog.py load --data-dir ./data --days 08
python bench/backlog.py run
python reference/compare.py --through 2021-11-08
python bench/check_delivery.py --through 2021-11-08
python bench/event_handling.py
```

Expected: 53,994,554 records; digest `a7ed875d2291c71a8151275953a15be4`; `compare.py` ends with `identical` (7 windows still
open are left out); `check_delivery.py` prints `17 of 17` and `CORRECT`; `event_handling.py` shows 11,856,732 price events.
`backlog.py run` prints values in the lines `memory MB` and `skew`.

```bash
python bench/scaling_matrix.py --configs 4x1 --repeats 1 --name smoke
python bench/peak_test.py --scopes ETR --speeds 100 --days 08 --name smoke
python bench/subscriber_sweep.py --traders 1 --day 08 --start 09:00 --end 09:30 --speed 100 --name smoke
```

Expected: the matrix has one valid run with the digest above; the peak test prints `keeps up` (or `n/a`, if too few
lag samples) and busy times; the subscriber sweep prints 3,897 deliveries of 3,897 expected.

Then delete the smoke results. The two log files hold every run since they were last removed: skip that line if they
contain earlier runs you need.

```bash
rm -r bench/results/*/*-smoke
rm bench/results/backlog_runs.jsonl bench/results/advisory_latency.jsonl
```

## A5. Full week: correctness, event handling, throughput

```bash
python bench/backlog.py load --data-dir ./data
python bench/backlog.py run | tee bench/results/final/a5-run.txt
python reference/compare.py | tee bench/results/final/a5-compare.txt
python bench/check_delivery.py | tee bench/results/final/a5-delivery.txt
python bench/event_handling.py | tee bench/results/final/a5-events.txt
```

Expected:

| Output | Value |
|---|---|
| `load` | 289,116,808 records (4–5 min) |
| `run` | digest `19413a50…` `(= reference week)`, 1,855,645 rows, 7,769 BUY, 3,580 SELL |
| `compare.py` | `identical` |
| `check_delivery.py` | 73 of 73 expected, 0 missing, 0 unexpected, `CORRECT` (default subscriptions: shows the filtering) |
| `event_handling.py` | zero prices 316,382; LATE 5,506; TIME_SKEW 6,681; PRICE_JUMP 638 |

## A6. Scaling with resources: throughput, CPU, memory, skew

```bash
python bench/scaling_matrix.py --name final
```

1×1, 2×1, 4×1, 8×1 and 2×4 (TaskManagers × slots), 3 repeats in rotated order. Expected: every run has the reference
digest; others are listed below the table and left out. The latency column is latency at saturation; the system's latency
is in A8.

If it stops: `python bench/scaling_matrix.py --resume bench/results/scaling_matrix/<time>-final`.

Optional, vertical scaling in one TaskManager (60 min):
`python bench/scaling_matrix.py --configs 1x1,1x2,1x4,1x8 --name final-slots`

## A7. Long tail: the same load with evenly spread symbols

```bash
python bench/backlog.py load --data-dir ./data --workload uniform
python bench/scaling_matrix.py --workload uniform --configs 4x1,8x1,2x4 --name final-uniform
```

Uses its own topic `ticks-uniform`; `ticks` is kept. Compare time and skew with the same configurations of A6. The results
are meaningless as market data and have no reference digest; the runs must agree with each other. Latency is not comparable.

## A8. Latency under load

```bash
python bench/latency_sweep.py --end 09:36 --name final
```

Speeds 1, 3, 10, 30, 100, 300 and maximum, every symbol subscribed. The real-time point alone takes 36 min. Check that the
achieved events/s of the paced points are in proportion to the speed; where they are not, the producer was the limit.

Repeat the short points, for a spread:

```bash
python bench/latency_sweep.py --end 09:36 --speeds 30,100,300,0 --name final-rep1
python bench/latency_sweep.py --end 09:36 --speeds 30,100,300,0 --name final-rep2
python bench/latency_sweep.py --end 09:36 --speeds 30,100,300,0 --name final-rep3
```

Delay from a window's end in market time, for out-of-orderness bounds of 60, 5 and 1 s (the sweep starts when the window
fires, so it leaves this wait out). Whole seconds only:

```bash
python bench/window_delay.py --values 60,5,1 --name final
```

## A9. Exchanges and peak rates; sustained load

```bash
python bench/peak_test.py --name final
```

Finds the busiest minute of the week for all exchanges together, FR, NL and ETR, and replays 45 minutes around each at 10×,
30×, 100× and maximum. Verdict per point in `summary.md`; at maximum speed it may be `n/a` (too short for three lag
samples).

Sustained load: the whole session of the busiest day (10 Nov 09:00–17:30) at 100×, 300× and maximum, about 15 min:

```bash
python bench/peak_test.py --scopes all --speeds 100,300,0 --before 390 --after 120 --name final-session
python bench/stability.py plot bench/results/peak_test/*-final-session
```

Report the percentiles per third of the run (last column) and the plots, and the window latency next to the advisory
latency (it has far more samples). At maximum speed the producer sets the rate, so `keeps up` is no capacity limit of the job.

## A10. Subscribers

```bash
python bench/subscriber_sweep.py --name final-all
python bench/subscriber_sweep.py --mode sample --traders 100,1000 --name final-sample
```

1, 10 and 100 traders with every symbol (the worst case), then 100 and 1,000 traders with 50 symbols each. Expected: all
deliveries correct. Do not use points marked `few!` or `spill!` for percentiles. If `stored → received` grows with the
traders, the single consumer may be the limit, not the pipeline.

## A10b. The database

```bash
python bench/db_limit.py realistic --name final-realistic
python bench/db_limit.py bursts --name final-bursts
python bench/db_limit.py saturation --name final-saturation
```

- `realistic` (50 min): the real mix at real time, 10× and the whole session up to the maximum.
- `bursts` (30 min): up to 80,000 symbols per window close.
- `saturation` (16 min): the stored week and two uniform workloads, with and without the database. Needs the week in `ticks`
  (A5). **It replaces `ticks-uniform`**, so run it after A7.

Report the capacity in rows/s, not only in events/s: the limit in events/s is the capacity times the events per row. The
`Rows/s` of the paced points are the load, not a capacity.

## A10c. Design alternatives and failover

Alternatives measured for *Creative explorations*; most are in earlier steps:

| Decision | Alternative | Step |
|---|---|---|
| Out-of-orderness bound 1 s | 5 s, 60 s | A8 (`window_delay.py`) |
| Zero prices excluded; late events dropped | included; kept | A3 |
| Partitioning by symbol | evenly spread symbols; other partition counts (offline) | A7; A3 |
| Many small TaskManagers (8×1) | few large ones (2×4) | A6 |
| Database in the pipeline | without it | A10b (`saturation`) |
| Multi-row upserts | one statement per row | here |
| Checkpoint every 10 s | every 1 s | here |
| Watermark alignment 300 s | off | here |

The first is the default, for comparison on the same day:

```bash
python bench/scaling_matrix.py --configs 4x1 --name final-default
python bench/scaling_matrix.py --configs 4x1 --set JDBC_URL=jdbc:postgresql://timescaledb:5432/trading --name final-no-multirow
python bench/scaling_matrix.py --configs 4x1 --set "CHECKPOINT_INTERVAL=1 s" --name final-checkpoint1s
```

Without alignment, the results depend on read timing, so the digest is expected to differ from the reference (run twice):

```bash
python bench/backlog.py run --set WATERMARK_ALIGNMENT_S=0 | tee bench/results/final/a10c-no-alignment-1.txt
python bench/backlog.py run --set WATERMARK_ALIGNMENT_S=0 | tee bench/results/final/a10c-no-alignment-2.txt
```

Failover: a TaskManager is killed in the middle of a run and started again. Needs the week in `ticks`.

```bash
python bench/failover.py | tee bench/results/final/a10c-failover.txt
python reference/compare.py | tee bench/results/final/a10c-failover-compare.txt
python bench/check_delivery.py | tee bench/results/final/a10c-failover-delivery.txt
```

Expected: at least one restart, the reference digest, `identical` and `CORRECT`: no event lost, none counted twice. The run
takes longer, and its CPU and skew are not comparable with other runs.

## A11. Live run and visualization

```bash
SUBSCRIPTIONS_FILE=/config/subscriptions-all.txt scripts/run-live.sh | tee bench/results/final/a11-live.txt
python reference/compare.py | tee bench/results/final/a11-compare.txt
python bench/check_delivery.py | tee bench/results/final/a11-delivery.txt
python bench/event_handling.py | tee bench/results/final/a11-events.txt
python bench/stability.py record --name final-live
```

Expected: `identical`; 11,349 of 11,349 delivered (every symbol subscribed), `CORRECT`. `record` stores the latency per
trading day, per 5 s, and the lag, with a plot. Name the spikes at the start and in the nights as such. Compare the latency
only with runs that have the same subscriptions.

Then take the screenshots, at http://localhost:3001 (no login needed), while the results are there:

1. **Trading trends:** market overview; one symbol with BUY/SELL markers (e.g. `ASML.NL`).
2. **Correctness and data quality:** every check must be 0; issues by reason.
3. **System metrics:** set the time range to the run: records/s, busy time, CPU and memory.

## A12. Collect

| Report topic | File |
|---|---|
| Host and version | `bench/results/final/host.txt` |
| Correctness, delivery | `bench/results/final/a5-*.txt`, `a11-*.txt` |
| Dataset, event types | `bench/results/event_types/summary.md`, `bench/results/final/a5-events.txt` |
| Throughput, CPU, memory, scaling, skew | `bench/results/scaling_matrix/<time>-final/summary.md` |
| Long tail | `bench/results/symbol_distribution/summary.md`, `bench/results/scaling_matrix/<time>-final-uniform/summary.md` |
| Latency | `bench/results/latency_sweep/<time>-final*/` (`latency.html`, `results.jsonl`) |
| Delay from the window's end | `bench/results/window_delay/<time>-final/summary.md` |
| Exchanges, peaks, sustained load | `bench/results/peak_test/<time>-final/`, `<time>-final-session/` (with PNGs) |
| Subscribers | `bench/results/subscriber_sweep/<time>-final-*/summary.md` |
| Database | `bench/results/db_limit/<time>-final-*/summary.md` |
| Design alternatives | `bench/results/scaling_matrix/<time>-final-{default,no-multirow,checkpoint1s}/`, `bench/results/final/a10c-*.txt` |
| Live run, stability | `bench/results/final/a11-live.txt`, `bench/results/stability/<time>-final-live/` |

Commit `bench/results`, then update `MEASUREMENTS.md`. State in the report: everything runs on one host; the latency of A8
(sweep) to A11 starts when a window fires and so leaves out the 1 s bound (the delay from the window's end is in
`window_delay`); zero prices are excluded.

---

# Part B: repeating measurements, keeping the loaded data

## What is kept where

| Data | Where | Emptied or replaced by |
|---|---|---|
| **Loaded events** | Kafka topic `ticks` | only `backlog.py load` or `down -v` |
| The same with even symbols | topic `ticks-uniform` | `load --workload uniform`, `db_limit.py saturation`, `down -v` |
| Copy of a live run | topic `live` | every `run-live.sh` |
| Input of the replays | topics `sweep`, `bench-replay` | every point of those scripts |
| Results | tables `ema_window`, `data_issue`; topic `advisories` | every run or point of every script |
| Flink state | checkpoints | every run or point: each starts a new job with EMA = 0 |
| Metrics, dashboards | Prometheus (7 days), Grafana | `down -v` |
| Slices and rate profiles | `data/slices` | never; reused while the CSV files keep their size |
| Measured results | `bench/results` | never; new runs are added (`event_types` and `symbol_distribution` are overwritten) |

## B1. Repeat a measurement

**Open the measurement shell (A1) and run the command again.** Every script resets what it needs first and keeps `ticks`.
What is loaded: `python bench/backlog.py status` (the week: 289,116,808 records in `ticks`).

| Measurement | Command | Needs |
|---|---|---|
| Full week, stored | `python bench/backlog.py run` | `ticks` |
| Single configuration | `python bench/scaling_matrix.py --configs 8x1 --name 8x1` | `ticks` |
| Missing or failed matrix runs | `python bench/scaling_matrix.py --resume bench/results/scaling_matrix/<time>` | `ticks`, unchanged |
| Uniform workload | `python bench/backlog.py run --workload uniform` | `ticks-uniform` |
| Latency sweep, some points | `python bench/latency_sweep.py --end 09:36 --speeds 100,0 --name again` | CSV files |
| Peak test, one exchange | `python bench/peak_test.py --scopes NL --speeds 100 --name nl` | CSV files |
| Subscribers, one point | `python bench/subscriber_sweep.py --mode sample --traders 1000 --name t1000` | CSV files |
| Live run | `scripts/run-live.sh` | CSV files |
| Comparison with the reference | `python reference/compare.py` | results of a full-week run in the tables |

A single matrix configuration is not comparable with a matrix from another day; for a speedup, run the baseline with it
(`--configs 1x1,8x1`).

## B2. Reset only the results

```bash
python bench/backlog.py reset
```

Stops the job and empties the tables, `advisories` and the checkpoints; keeps the events. Not needed before a measurement.

## B3. Stop and continue later, or after a reboot

```bash
docker compose --profile producer --profile latency down
```

Without `-v`, the volumes and so the events are kept. To continue, run the next measurement: the scripts start the services
they need. **Do not use a plain `docker compose up -d` here:** it starts a job that reads all of `ticks` at once and writes
results. For the services without the job:

```bash
docker compose up -d --wait kafka timescaledb
docker compose up -d prometheus cadvisor grafana
```

## B4. After changing something

| Changed | Do | Load again? |
|---|---|---|
| Flink job (`flink-job/`) | run the measurement (it rebuilds the job); then `compare.py` after a full-week run | no |
| Producer or latency consumer (`producer-rs/`) | `docker compose build kafka-setup` | only if the sent rows or partitions change |
| A job setting | `--set KEY=VALUE` (settings in `docker-compose.yml`) | no |
| `config/subscriptions.txt` | run the measurement | no |
| Columns of a database table | recreate the database, below | no |
| Partitions of `ticks` | `python bench/backlog.py load --data-dir ./data --partitions 16` | **yes** |
| Days or CSV files | `python bench/backlog.py load --data-dir ./data --days …` | **yes** |
| CSV files with the same size | also `rm data/slices/*` | **yes** |

Only the database anew, keeping Kafka:

```bash
docker compose rm -sf timescaledb grafana jobmanager taskmanager
docker volume rm scalable-proj_timescale-data
docker compose up -d --wait timescaledb
docker compose run --rm --no-deps db-setup
```

Only Kafka anew (loses `ticks`, `ticks-uniform`, `live`), keeping results and metrics:

```bash
docker compose rm -sf kafka kafka-setup jobmanager taskmanager
docker volume rm scalable-proj_kafka-data
python bench/backlog.py load --data-dir ./data
```

## B5. Free disk space

Only when nothing is running. `ticks` is kept; slices are cut again when needed.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic live
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic sweep
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic bench-replay
rm -r data/slices
```

## B6. When something goes wrong

| Symptom | Do |
|---|---|
| A script stopped in the middle | run it again: every run or point starts with a reset. The matrix continues with `--resume`; for the others, run the missing points (`--speeds`, `--scopes`, `--traders`) |
| `python: command not found`, `No module named pandas` | open the measurement shell (A1) |
| `topic ticks is empty` | load it (A5) |
| `topic ticks changed` with `--resume` | the topic has been loaded differently; start a new matrix |
| Digest different from `19413a50…` | wrong result (or not the whole week loaded): `compare.py` shows the differing windows. Do not report the run |
| `compare.py`: only the last windows of the day are missing | a one-day run compared without `--through` |
| `check_delivery.py`: `the job is not running` | run it before the next measurement or `down`, or name the file with `--subscriptions` |
| Job does not start | `docker compose logs jobmanager`; free ports 8081, 9090, 9094, 5432, 3001 |
| Run or editor killed, no memory | close other programs (`free -g`), repeat the step |
| Nothing helps | A2 (deletes everything), then A5 |
