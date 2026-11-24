# Final measurements for the report

How to run every measurement the report needs, from an empty system (part A), and how to repeat single measurements
without starting over (part B). Results and their interpretation are in [`MEASUREMENTS.md`](MEASUREMENTS.md) (current:
§7), the requirement in [`docs/requirements.md`](docs/requirements.md) §5.2 (D-3). All commands run in the repo root.

| Report topic | Measurement | Step | Time (estimate) |
|---|---|---|---|
| Correctness | Full week against the pandas reference, bit for bit; delivered advisories | A5, A11 | 10 min + live run |
| Throughput under load | Stored week at full speed; live run | A5, A6, A11 | 5 + 80 + 6 min |
| Latency under load | Load/latency sweep, real time to maximum; delay from the window's end | A8 | 65 min (15 min without 1× and 3×), repeats 25 min, delay 45 min |
| Scalability, resources (CPU, memory) | Scaling matrix | A6 | 80 min |
| Scalability, exchanges and peak rates | Peak test: all, FR, NL, ETR | A9 | 65 min |
| Scalability, subscribers | Subscriber sweep | A10 | 25 min |
| Long-tail distribution | Symbol distribution (offline); uniform workload | A3, A7 | 2 + 45 min |
| Event types | Data issue summary | A5 | in A5 |

The whole of part A takes about 7 hours, most of it unattended. `scaling_matrix.py`, `peak_test.py`,
`subscriber_sweep.py` and the uniform workload had **not been run end to end** when this was written: do the smoke tests
(A4) first.

## Rules

- **One measurement at a time**, and nothing else running on the host. Close Grafana tabs: their queries load
  TimescaleDB.
- **Plug the laptop in** and keep it awake. Start long runs as
  `systemd-inhibit --what=idle:sleep:handle-lid-switch <command>`.
- **Do not change code or settings between runs you want to compare.** The scripts rebuild the images from the working
  tree.
- Before the first run, note `git rev-parse --short HEAD` and the host, as the report has to name them:
  `lscpu | grep 'Model name'`, `free -g`, `cat /sys/devices/system/clocksource/clocksource0/current_clocksource`
  (HPET slows the producer, `MEASUREMENTS.md` §3).
- `bench/results/*.jsonl` are append-only and also hold older runs, some of them invalid (`MEASUREMENTS.md` §7, "Metrics
  corrected"). Use only lines from after your start. The other results have a directory per run.
- **Compare `ema_window` with the reference right after the run it belongs to:** the next measurement empties the
  table.

---

# Part A: everything from scratch

## A1. Prerequisites

Docker with Compose v2, Python 3 (standard library only for `bench/`), about 60 GB of free disk: CSV files 24 GB, Kafka up
to 20 GB (`ticks`, `ticks-uniform` and `live` about 6 GB each), slices 2 GB, images.

The seven files `debs2022-gc-trading-day-08-11-21.csv` … `-14-11-21.csv` must be in `./data`:

```bash
ls data/debs2022-gc-trading-day-*.csv | wc -l      # 7
```

## A2. Empty system, build, start

> **This deletes everything the system has stored:** loaded events, results, metrics and Grafana's data. It does not
> touch `./data` or `bench/results`. To keep the loaded events, use part B.

```bash
docker compose --profile producer --profile latency down -v --remove-orphans
docker compose up -d --build                       # builds the images (runs the unit tests) and starts all services
docker compose ps                                  # kafka and timescaledb healthy, 4 taskmanagers running
until curl -sf localhost:8081/jobs/overview | grep -q '"state":"RUNNING"'; do sleep 2; done; echo job running
```

The topic is still empty, so the job idles. Grafana: http://localhost:3001, Flink: http://localhost:8081.

## A3. Offline: reference and long-tail analysis (no services needed)

```bash
python3 -m venv reference/.venv && reference/.venv/bin/pip install -r reference/requirements.txt
reference/.venv/bin/python -m pytest reference -q                                   # all pass
reference/.venv/bin/python reference/ema_reference.py --exclude-zero-prices --out reference/output-nozero
reference/.venv/bin/python reference/compare.py --reference-digest                  # 19413a509d53044973e3e6b76c177af1

python3 bench/symbol_distribution.py               # → bench/results/symbol_distribution/ (2 min)
```

Each of the files read in parallel by `ema_reference.py` needs about 6 GB of memory (`--jobs`, default 2). To also
report what zero prices change, run it without `--exclude-zero-prices` and with `--out reference/output`
(25,834 advisories instead of 11,349).

## A4. Smoke tests (about 15 min)

Load one day, check correctness, then run one short point of each script:

```bash
python3 bench/backlog.py load --data-dir ./data --days 08
python3 bench/backlog.py run
reference/.venv/bin/python reference/compare.py --through 2021-11-08                 # identical

python3 bench/scaling_matrix.py --configs 4x1 --repeats 1 --name smoke
python3 bench/peak_test.py --scopes ETR --speeds 100 --days 08 --name smoke
python3 bench/subscriber_sweep.py --traders 1 --speed 100 --name smoke
```

Check in the output:
- `backlog.py`: the lines `memory MB` and `skew` have values.
- `scaling_matrix.py`: one valid run with `digest` `a7ed875d2291c71a8151275953a15be4` (8 Nov only, 1 s bound; at 60 s it was `fffd77b9…`).
- `peak_test.py`: a lag with samples, busy times, and a verdict other than `n/a`.
- `subscriber_sweep.py`: no "deliveries differ" warning.

Delete the smoke results, so they do not get into the report: `rm -r bench/results/*/*-smoke`.

## A5. Load the week; correctness, event types, throughput

```bash
python3 bench/backlog.py load --data-dir ./data                    # 289,116,808 records, about 4 min
python3 bench/backlog.py run                                       # about 4 min
reference/.venv/bin/python reference/compare.py --save-export /tmp/ema_window.csv   # identical
```

Expected: digest `19413a50… (= reference week)`, 1,855,645 rows, 7,769 BUY, 3,580 SELL.

Handling of event types (late, skewed, implausible events), and the zero prices that are only counted in the log:

```bash
docker compose exec -T timescaledb psql -U postgres -d trading -c 'SELECT * FROM data_issue_summary'
docker compose logs taskmanager | sed -nE 's/.*quality subtask=([0-9]+)\].*zero_price=([0-9]+).*/\1 \2/p' \
    | awk '{z[$1] = $2} END {for (i in z) s += z[i]; print "zero prices:", s + 0}'
```

Expected: TIME_SKEW 6,681, LATE 5,506, PRICE_JUMP 638; zero prices 316,382 (the
sum of each subtask's last log line).

Delivered advisories against the reference and the subscriptions (expected: 73 messages, 0 missing, 0 unexpected).
Run it before the next measurement, which recreates the topic. After A11, start it with
`SUBS=config/subscriptions-all.txt` in front:

```bash
reference/.venv/bin/python - <<'PY'
import json, os, re, subprocess
import pandas as pd

subs = {}
for line in open(os.environ.get("SUBS", "config/subscriptions.txt")):
    line = line.split("#")[0].strip()
    if line:
        trader, symbols = line.split(":")
        subs.setdefault(trader.strip(), set()).update(s for s in re.split(r"[,\s]+", symbols) if s)
ref = pd.read_csv("reference/output-nozero/advisories.csv", parse_dates=["window_end"])
expected = {(t, r.symbol, r.window_end.strftime("%Y-%m-%dT%H:%M:%S"), r.type)
            for r in ref.itertuples() for t, s in subs.items() if "*" in s or r.symbol in s}
out = subprocess.run("docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "
                     "localhost:9092 --topic advisories --from-beginning --timeout-ms 10000".split(),
                     capture_output=True, text=True).stdout
messages = [json.loads(line) for line in out.splitlines() if line.startswith("{")]
got = {(m["trader"], m["symbol"], m["window_end"], m["type"]) for m in messages}
print(f"messages {len(messages)}, distinct {len(got)}, expected {len(expected)}, "
      f"missing {len(expected - got)}, unexpected {len(got - expected)}")
PY
```

## A6. Scaling with resources: throughput, CPU, memory, skew

```bash
systemd-inhibit --what=idle:sleep:handle-lid-switch python3 bench/scaling_matrix.py
```

1×1, 2×1, 4×1, 8×1 and 2×4 (TaskManagers × slots), 3 repeats in rotated order. Report `summary.md` in
`bench/results/scaling_matrix/<time>/`: median time, spread, speedup, efficiency, CPU, memory, skew and the busiest task. Every
run must have the reference digest; runs that have not are listed and left out. Interrupted or failed:
`python3 bench/scaling_matrix.py --resume bench/results/scaling_matrix/<time>`.

For vertical scaling, add `--configs 1x1,1x2,1x4,1x8 --name slots` (another 60 min). The latency column is latency at
saturation, not the system's latency (that is A8).

## A7. Long tail: the same load with evenly spread symbols

```bash
python3 bench/backlog.py load --data-dir ./data --workload uniform        # own topic ticks-uniform; `ticks` is kept
systemd-inhibit --what=idle:sleep:handle-lid-switch \
    python3 bench/scaling_matrix.py --workload uniform --configs 4x1,8x1,2x4 --name uniform
```

Compare time and skew with the same configurations of A6. Its results are meaningless as market data and have no
reference digest; the runs must agree with each other. It has the same number of windows as the real week, but
different advisories, so latency is not comparable.

## A8. Latency under load

```bash
systemd-inhibit --what=idle:sleep:handle-lid-switch python3 bench/latency_sweep.py --end 09:36 --name final
```

Speeds 1, 3, 10, 30, 100, 300 and maximum; `--end 09:36` so that the 09:30 window is closed too
(with the 1 s bound, 09:35 as well, so do not compare the sample counts with runs made at 60 s). Report `latency.html`
and `results.jsonl` in `bench/results/latency_sweep/<time>-final/`: p50/p95/p99 against events/s. The real-time point
alone takes 36 min; without it and 3×: `--speeds 10,30,100,300,0`.

Single points vary, so repeat the short ones, for a spread:

```bash
for i in 1 2 3; do python3 bench/latency_sweep.py --end 09:36 --speeds 30,100,300,0 --name final-rep$i; done
```

Check that the achieved events/s of the paced points are in proportion to the speed; where they are not, the producer
was the limit and the point is not a steady load.

Delay from a window's end in market time (A8 above starts when the window fires), about 45 min:

```bash
systemd-inhibit --what=idle:sleep:handle-lid-switch python3 bench/window_delay.py --values 60,5,1 --name final
```

Use whole seconds only: fractional values are not measured correctly (`MEASUREMENTS.md` §8).

## A9. Exchanges and peak rates

```bash
systemd-inhibit --what=idle:sleep:handle-lid-switch python3 bench/peak_test.py --name final
```

Finds the busiest minute of the week for all exchanges together, FR, NL and ETR, and replays 45 minutes around each at
10×, 30×, 100× and maximum. Scans the files first (about 1 min per day in parallel; kept in `data/slices`). Report
`summary.md` in `bench/results/peak_test/<time>-final/`: real peak rates, verdict, lag, CPU, memory (in
`results.jsonl`) and latency. At maximum speed the verdict may be `n/a` (too short for three lag samples); a longer
slice helps: `--scopes all --speeds 100,300 --before 60 --after 60 --name long`.

## A10. Subscribers

```bash
systemd-inhibit --what=idle:sleep:handle-lid-switch python3 bench/subscriber_sweep.py --name final
```

1, 10, 100 and 1,000 traders with 50 symbols each. Worst case, every trader following every symbol:
`--mode all --traders 1,10,100 --name all`. Report `summary.md` in `bench/results/subscriber_sweep/<time>-final/`. If
`stored → received` grows with the traders, the single consumer is the limit, not the pipeline.

## A11. Live run (the whole system at once)

```bash
SUBSCRIPTIONS_FILE=/config/subscriptions-all.txt systemd-inhibit --what=idle:sleep:handle-lid-switch scripts/run-live.sh
reference/.venv/bin/python reference/compare.py                    # identical
```

Then the delivery check of A5 with `SUBS=config/subscriptions-all.txt` (expected: 11,349 messages, 0 missing, 0
unexpected). Every symbol is subscribed here, so delivery is checked for all of them and the latency has 11,337 samples
instead of 72; A5 keeps the default file, which shows the filtering (FR-4). Compare the latency only with runs that have the
same subscriptions (`MEASUREMENTS.md` §9). Screenshots for the report now: Grafana shows the whole week's results (Trading
trends, Correctness) and the run's system metrics.

## A12. Collect

| Report | File |
|---|---|
| Correctness | output of `compare.py` (A5, A11), the delivery check and `data_issue_summary` |
| Throughput, CPU, memory, scaling, skew | `bench/results/scaling_matrix/<time>/summary.md` (+ `-uniform`) |
| Latency | `bench/results/latency_sweep/<time>-final/` (+ `-final-rep*`) |
| Delay from the window's end | `bench/results/window_delay/<time>-final/summary.md` |
| Exchanges, peaks | `bench/results/peak_test/<time>-final/summary.md` |
| Subscribers | `bench/results/subscriber_sweep/<time>-final/summary.md` |
| Long tail | `bench/results/symbol_distribution/summary.md`, `symbols.csv` |
| Live run | last line of `bench/results/advisory_latency.jsonl`; CPU and memory from Grafana |

Commit `bench/results`, then update `MEASUREMENTS.md`. Say in the report: everything runs on one host; latency in A8 to A11
starts when a window fires, so it leaves out the out-of-orderness wait (1 s); the time from the window's end is in A8
(`window_delay`); zero prices are excluded (A3).

---

# Part B: repeating measurements, keeping the loaded data

## What is kept where

| Data | Where | Emptied or replaced by |
|---|---|---|
| **Loaded events (the CSV data, ready to replay)** | Kafka topic `ticks` | only `backlog.py load` or `down -v` |
| The same with even symbols | topic `ticks-uniform` | only `load --workload uniform` or `down -v` |
| Copy of a live run | topic `live` | every `run-live.sh` |
| Input of the sweep / peak and subscriber tests | topics `sweep` / `bench-replay` | every point of those scripts |
| Results | tables `ema_window`, `data_issue`; topic `advisories` | every run or point of every script |
| Flink state | checkpoints, EMA state | every run or point: each starts a new job with EMA = 0 |
| Metrics, dashboards | Prometheus (7 days), Grafana | `down -v` |
| Slices and rate profiles | `data/slices` | never; reused while the CSV files keep their size |
| Measured results | `bench/results` | never; new runs are added |

## B1. Repeat a measurement

**Just run its command again.** Every script resets what it needs first (results, advisories, checkpoints, job) and keeps
`ticks`. Nothing has to be loaded again for:

| Measurement | Command | Needs |
|---|---|---|
| Full week, stored | `python3 bench/backlog.py run` | `ticks` |
| Scaling matrix | `python3 bench/scaling_matrix.py` | `ticks` |
| Single configuration | `python3 bench/scaling_matrix.py --configs 8x1 --baseline 8x1 --name 8x1` | `ticks` |
| Missing or failed matrix runs | `python3 bench/scaling_matrix.py --resume bench/results/scaling_matrix/<time>` | `ticks`, unchanged |
| Uniform workload | `python3 bench/backlog.py run --workload uniform` | `ticks-uniform` |
| Latency sweep, some points | `python3 bench/latency_sweep.py --end 09:36 --speeds 100,0 --name again` | CSV files |
| Peak test, one exchange | `python3 bench/peak_test.py --scopes NL --speeds 100 --name nl` | CSV files (slice reused) |
| Subscribers, one point | `python3 bench/subscriber_sweep.py --traders 1000 --name t1000` | CSV files (slice reused) |
| Live run | `scripts/run-live.sh` | CSV files |
| Comparison with the reference | `reference/.venv/bin/python reference/compare.py` | results of a full-week run in the tables |
| Comparison with a saved result | `reference/.venv/bin/python reference/compare.py --export /tmp/ema_window.csv` | the file of A5 |

A single matrix configuration is not comparable with a matrix from another day; for a speedup, run the baseline with it
(`--configs 1x1,8x1`).

Is the data still loaded? Should print 289116808 (8 partitions) for `ticks`:

```bash
docker compose up -d --wait kafka
for t in ticks ticks-uniform; do echo "$t $(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic $t 2>/dev/null | awk -F: '{s += $3} END {print s + 0}')"; done
```

## B2. Reset only the results

```bash
python3 bench/backlog.py reset
```

Stops the job and empties the tables, `advisories` and the checkpoints; keeps the events. Not needed before a
measurement (see B1); use it to leave a clean system.

## B3. Stop and continue later, or after a reboot

```bash
docker compose --profile producer --profile latency down      # no -v: the volumes, and so the events, are kept
```

To continue, run the next measurement: the scripts start the services they need. **Do not use a plain
`docker compose up -d` here:** it starts a job that reads all of `ticks` at once and writes results. If you want services
by hand, without the job:

```bash
docker compose up -d --wait kafka timescaledb
docker compose up -d prometheus cadvisor grafana
```

## B4. After changing something

| Changed | Do | Load again? |
|---|---|---|
| Flink job (`flink-job/`) | run the measurement (it rebuilds the job); then `compare.py` after a full-week run | no |
| Producer or latency consumer (`producer-rs/`) | `docker compose build kafka-setup` (the scripts other than `backlog.py run` and `scaling_matrix.py` do this) | only if the sent rows or partitions change |
| A job setting | `--set KEY=VALUE`, only for settings in `docker-compose.yml` (`JDBC_BATCH_SIZE`, `WATERMARK_ALIGNMENT_S`, …) | no |
| `config/subscriptions.txt` | run the measurement | no |
| Database schema | run the measurement (applies the idempotent schema); for changed columns see below | no |
| Partitions of `ticks` | `python3 bench/backlog.py load --data-dir ./data --partitions 16` | **yes** (4 min); the matrix needs at least its largest parallelism |
| Days or CSV files | `python3 bench/backlog.py load --data-dir ./data --days …` | **yes** |
| CSV files with the same size | also delete their profiles and slices: `rm data/slices/*` | **yes** |

Only the database anew (e.g. changed columns), keeping Kafka:

```bash
docker compose rm -sf timescaledb grafana jobmanager taskmanager
docker volume rm scalable-proj_timescale-data
docker compose up -d --wait timescaledb && docker compose run --rm --no-deps db-setup
```

Only Kafka anew (loses `ticks`, `ticks-uniform`, `live`), keeping results and metrics:

```bash
docker compose rm -sf kafka kafka-setup jobmanager taskmanager
docker volume rm scalable-proj_kafka-data
python3 bench/backlog.py load --data-dir ./data
```

## B5. Free disk space

```bash
for t in live sweep bench-replay; do docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --if-exists --topic $t; done     # ticks is kept
rm -r data/slices                                                               # cut again when needed
```

Only do this when nothing is running. Deleting `ticks-uniform` the same way means loading it again (4 min).

## B6. When something goes wrong

| Symptom | Do |
|---|---|
| A script stopped in the middle | run it again: every run or point starts with a reset. The matrix continues with `--resume`; for the others, run the missing points (`--speeds`, `--scopes`, `--traders`) |
| `topic ticks is empty` | load it (A5) |
| `topic ticks changed` with `--resume` | the topic has been loaded differently; start a new matrix |
| Digest different from `19413a50…` | wrong result (or not the whole week loaded): `compare.py` shows the differing windows. Do not report the run |
| `compare.py`: only the last windows of the day are missing | a one-day run compared without `--through` |
| Job does not start | `docker compose logs jobmanager \| tail -50`, free ports 8081, 9090, 9094, 5432, 3001 |
| Nothing helps | A2 (deletes everything), then A5 |
