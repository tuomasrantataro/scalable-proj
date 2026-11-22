# Measurements

Measurements of the Kafka → Flink → TimescaleDB pipeline. Until 2026-11-20 the broker was Apache Iggy; those
measurements, the connector findings and the reasons for replacing it are in [`docs/iggy.md`](docs/iggy.md).

## Setup

| Item | Value |
|---|---|
| Date | 2026-11-20 |
| Host | Laptop, AMD Ryzen 7 5825U (8 cores / 16 threads), 35 GB RAM, Linux 7.3 (Fedora), Docker 29.10. All containers on one host (the same host as the Iggy measurements). |
| Clock source | **HPET** (the kernel marked the TSC unstable), so every clock read is a ~1.4 µs system call. This limits the producer (§3). |
| Kafka | `apache/kafka:4.3.2`, one broker in KRaft mode, 1 GB heap. `ticks`: 8 partitions; `advisories`: 4 partitions. |
| Flink | 2.3.0, 1 JobManager + 4 TaskManagers × 1 slot, parallelism 4, checkpoints every 10 s, heap state backend. `flink-connector-kafka` 5.0.0-2.2, `max.poll.records` 5000. |
| Producer | `replay-producer` (Rust, rdkafka 0.39 / librdkafka 2.12): 1 reader thread, 8 sender threads, one record per CSV row (key = symbol), lz4, `linger.ms` 20, idempotence on. |
| Data | DEBS 2022 week, 7 files, 289,116,808 rows (59.4 M usable price events) |

## 1. Full week: results identical to Iggy, finished faster

Fresh stack (`docker compose down -v`), job started, then the 7 files replayed one after another while the job
ran (the commands in [`start-from-scratch.sh`](start-from-scratch.sh)).

**Correctness.** The job's output was exported (`ema_window`: symbol, window_end, close, ticks, ema38, ema100,
advisory, initial; `data_issue`: reason, symbol, event_time, price, detail) and compared with the same export of
the last Iggy run (1 partition, [`docs/iggy.md`](docs/iggy.md) §5): **byte-identical** (md5 `19413a50…` and
`1b15d2fb…`), in both Kafka runs. So 8 partitions with per-partition watermarks give exactly the results that
needed a 1-partition topic with Iggy.

| | Iggy (2026-11-20) | Kafka, run 1 | Kafka, run 2 |
|---|---|---|---|
| Producer | 1 thread (Iggy SDK) | 1 thread (first version, §3) | 8 threads |
| TaskManagers / parallelism | 8 / 2 | 4 / 4 | 4 / 4 |
| Source readers | 1 of 2 active | 4 of 4 | 4 of 4 |
| `ticks` partitions | 1 | 8 | 8 |
| Producer, whole week | ~3 min 5 s | 24 min 14 s | 4 min 39 s (per day 50–58 s) |
| Start → last output row | **about 5 min** (4:55–5:05) | 24 min 14 s | **4 min 39 s** |
| Job behind the producer at the end | about 2 min | < 1 s | < 1 s |
| `ema_window` rows / BUY / SELL | 1,855,645 / 7,769 / 3,580 | same | same |
| Rows written more than once | 0 | 0 | 0 |
| Data issues (TIME_SKEW / LATE / PRICE_JUMP) | 6,681 / 5,506 / 638 | same | same |
| Messages in topic `advisories` | 73 | 73 | 73 (partitions: 20 / 3 / 15 / 35) |

Run 1 was limited by the first producer version (about 200k msg/s, §3); the job kept up with it. In run 2 the
job also kept up: the last row was written within a second of the producer finishing.

## 2. Where the bottleneck is now

Prometheus, run 2, 21:14:20–21:18:20 UTC (averages over the run; per subtask the four values were within ±10%):

| Task (×4 subtasks) | Busy (ms/s) | Backpressured (ms/s) | Per subtask |
|---|---:|---:|---|
| Kafka source + quality gate | **~235** | ~12 | ~250k rows/s read, ~49k price events/s out |
| 5-min window | ~117 | ~26 | ~1.5k windows/s out |
| EMA + TimescaleDB writer | **~480** | 0 | ~1.5k rows/s |
| Data-issues writer | ~5 | 0 | – |

| Container | CPU (avg, % of a core) |
|---|---:|
| replay-producer | **596** |
| TaskManagers (4 together) | 251 |
| TimescaleDB | 184 |
| Kafka | 111 |

Kafka consumer lag: on average about 45k records per subtask (≈ 0.2 s of input), at most 116k.

- **The producer is the bottleneck.** It used about 6 cores to send ~1.05M msg/s while the job ran (1.41M alone,
  §3), because of the per-record clock reads.
- **The job has headroom.** The busiest task (EMA + JDBC writer) was busy about half the time; the sources a
  quarter. With Iggy the single source reader was busy 95% and limited everything ([`docs/iggy.md`](docs/iggy.md) §5).
- The next limit in the job is the EMA + JDBC writer and TimescaleDB (1.8 cores), not the source.

## 3. Producer: the clock source limits librdkafka

Producer alone (Flink stopped), one trading day (53,994,554 rows), fresh 8-partition topic:

| Version | Time | Throughput | Notes |
|---|---:|---:|---|
| First version: one thread reads and sends | 265.6 s | 203k msg/s | main thread 75% in the kernel; Kafka at 91% |
| + record timestamp set once per 4096 rows | ~45 s run, 12.7 M sent | ~280k msg/s | still 75% in the kernel |
| Reader thread + 1 sender thread | 180.6 s | 299k msg/s | |
| Reader thread + 2 sender threads | 109.2 s | 494k msg/s | |
| Reader thread + 4 sender threads | 55.5 s | 973k msg/s | each sender ~95% busy, about 60% of it in the kernel |
| **Reader thread + 8 sender threads (default)** | **38.4 s** | **1,406k msg/s** | producer ~630% CPU |

For comparison, the fastest Iggy producer sent the same day in 26.4 s (2.05M msg/s; [`docs/iggy.md`](docs/iggy.md) §4).

**Why.** Per-thread CPU (`/proc/<pid>/task/*/stat`) showed the sending thread at 25% user and 75% system time.
librdkafka reads the clock for every record it enqueues (the enqueue time for the message timeout, and the
wall clock for the record timestamp unless the caller sets one). On this laptop the clock source is HPET, so
each read is a system call of ~1.4 µs instead of a ~20 ns vDSO call. The same effect slowed down the first Rust
Iggy producer ([`docs/iggy.md`](docs/iggy.md) §4). Setting the timestamp explicitly removes one read per record;
the enqueue-time read cannot be avoided through the API. Two producer processes in parallel sent about twice as
much as one, so the reads scale over cores, and the producer now spreads the partitions over 8 sender threads
(each partition always on the same thread, so per-symbol order is kept).

On a host whose clock source is TSC this cost should mostly disappear (not measured here). Check with
`cat /sys/devices/system/clocksource/clocksource0/current_clocksource`.

## 4. JDBC sink and failover with the Kafka source

Script `bench/jdbc_sink_test.py` and job `PIPELINE=jdbc` (`JdbcSinkPipeline.java`), both since removed (last in
commit `0d3acfe`): price events → 5-min
windows (last price by event time, tick count) → `INSERT … ON CONFLICT (symbol, window_end) DO UPDATE` into
`window_close`, with a `writes` counter per row. One trading day (53,994,554 rows), 8 partitions, 4 TaskManagers,
checkpoints every 10 s. Each phase uses its own consumer group, so it reads the backlog from offset 0.

Two complete runs of the script (the TaskManager was killed at 30% and at 60% of the expected rows):

| Phase | Run 1 | Run 2 | With Iggy (1 partition, [`docs/iggy.md`](docs/iggy.md) §4) |
|---|---|---|---|
| A: baseline | 381,395 rows, each written once, 37 late events, last row after 55 s | identical (same digest `daaa0d4f…`) | 381,395 rows, 37 late, about 93 s |
| B: same data again, table not truncated | same 381,395 rows, identical content, every row `writes` = 2 | same | same |
| C: kill a TaskManager running a source reader, restart it | **identical content to A**: 0 missing, 0 extra, 0 changed rows; 62,612 windows re-emitted after the restore and upserted (`writes` > 1); 2 job restarts | **identical to A**, 85,977 windows re-emitted; 4 job restarts | **1.0–1.5% of the ticks lost** (287–1,480 windows missing, ~5,600 short); 1 of 7 failovers stalled |
| D: no idleness (`IDLENESS_S=0`), 90 s | 381,229 rows: every window except the last ones of the day | same | 0 rows: an idle reader held the watermark at `Long.MIN_VALUE` |

- **Exactly-once source.** After a TaskManager kill the job restores the read offsets from the checkpoint and
  re-reads from there, so no event is lost; the windows emitted again are absorbed by the upsert. With Iggy the
  same test lost 1–1.5% of the ticks, because the connector resumed from offsets committed on poll.
- **The Kafka connector built for Flink 2.2 works on Flink 2.3** (source, and the sink in the EMA pipeline).
- **The 8-partition topic gives the same 37 late events** as the 1-partition Iggy topic, and an identical digest
  in both runs. The 166 windows missing in phase D are the last windows of each symbol: without idleness the
  watermark stops when the input stops (expected).
- The script picks the TaskManager to kill by the REST `write-records` metric of the source vertex, which read 0
  in both runs (it was unreliable with Iggy too); every TaskManager runs a source reader, so the kill still hit
  one. The REST watermark query of phase D returned an empty list (not investigated); the row count shows the
  windows fired.

## 5. Replaying a stored backlog: scaling without the producer

Script: [`bench/backlog.py`](bench/backlog.py). `load` fills topic `ticks` (8 partitions, kept forever) with the
whole week once, with Flink stopped: **289,116,808 records in 216 s, 5.6 GB on disk** (lz4). Each `run` empties
`ema_window` and `data_issue`, starts the job with a fresh consumer group (so it reads from offset 0) and waits
until the group has committed every record and the output has not changed for 30 s. Time = job RUNNING → last
`ema_window` row (`emitted_at`); it includes the job start and the final idleness timeout (10 s). Metrics are
averages over that time. Non-price rows are still filtered in Flink. Raw results:
`bench/results/backlog_runs.jsonl`.

### Finding: without watermark alignment the results depend on read timing

| Run (4 TaskManagers, parallelism 4) | Time | `ema_window` rows | LATE | BUY / SELL | Digest |
|---|---:|---:|---:|---|---|
| no alignment, run 1 | 142.7 s | 1,855,843 | 4,906 | 7,772 / 3,583 | `14589aec…` |
| no alignment, run 2 | 158.1 s | 1,855,843 | 4,906 | 7,772 / 3,583 | `14589aec…` |
| no alignment, run 3 | 138.2 s | 1,856,070 | 4,219 | 7,772 / 3,583 | `04874b06…` |
| **alignment 300 s** (`WATERMARK_ALIGNMENT_S`, now the default) | 152.9 s | **1,855,645** | **5,506** | **7,769 / 3,580** | **`19413a50…` = reference** |

Reading a backlog, the readers run at their own speed and the partitions drift apart in event time. The
watermark is the minimum over all partitions, so the slowest partition holds it back, and while it does, events
on the other partitions that are late in file order are still accepted. Compared with the reference, the runs
counted up to 1,287 fewer late events and wrote up to 425 more windows, with 3 more BUY and 3 more SELL. Which
rows were accepted was not checked; the only late rows in the reference are the 5,506 index rows of 10 Nov that
are 17.9 h late, so they are the likely ones. How many get through depends on timing. In the live runs (§1) the producer paced all partitions, so
this did not show. Watermark alignment pauses a partition that is more than 300 s of event time ahead of the
slowest; all aligned runs below give exactly the reference results. Its cost is within the run-to-run variation
(153 s aligned vs 138–158 s without).

### Scaling: TaskManagers = parallelism (aligned, whole week)

| TaskManagers / parallelism | Time | Records/s (overall) | Read rate avg / max | Speedup | EMA + JDBC writer busy | Source busy / backpressured | CPU: TaskManagers / TimescaleDB / Kafka |
|---:|---:|---:|---|---:|---:|---|---|
| 1 | 346.9 s | 0.83M | 0.81M / 1.02M | 1.0× | **946** | 352 / 629 | 103% / 87% / 93% |
| 2 | 204.6 s | 1.41M | 1.33M / 1.67M | 1.7× | **893** | 395 / 503 | 221% / 159% / 101% |
| 4 | 152.9 s | 1.89M | 1.66M / 2.13M | 2.3× | **865** | 480 / 257 | 490% / 275% / 113% |
| 8 | 135.9 s | 2.13M | 1.89M / 2.75M | 2.6× | 733 | 446 / 43 | 743% / 377% / 101% |
| 2 × 4 slots, parallelism 8 | 113.0 s | 2.56M | 2.18M / 3.00M | 3.1× | 754 | 452 / 67 | 576% / 379% / 117% |

All four runs: digest `19413a50…`, identical to the reference week. Busy and backpressured in ms/s (1000 = all
the time), averaged over subtasks.

- **The bottleneck is the EMA function chained with the TimescaleDB writer.** Up to 4 subtasks it is busy
  87–95% of the time and never backpressured, while the source and the window operator wait on it
  (backpressured). Whether that time goes to the EMA code or to waiting for TimescaleDB (batches of 1000
  upserts) was not measured; TimescaleDB's CPU grows with the parallelism (0.9 → 3.8 cores).
- **Scaling flattens beyond 4.** From 4 to 8 the time improves only 11%: the writer is no longer saturated
  (733), TimescaleDB uses 3.8 cores, and the containers together use ~12 of the host's 16 hardware threads.
  The single TimescaleDB instance and the shared host are the limit.
- **The whole week takes about 2.5 min** at parallelism 4–8, about 2× faster than the live run (§1), which was
  limited by the producer.
- **Fewer, larger TaskManagers were faster at the same parallelism:** 2 TaskManagers × 4 slots took 113 s, 8 ×
  1 slot 136 s, with less TaskManager CPU (576% vs 743%). A likely reason is that more of the `keyBy` traffic stays
  inside one JVM instead of being serialized and sent over the network between containers (not verified).
- Each configuration ran once (the no-alignment runs show ±7% between runs at parallelism 4).

## Caveats

- Everything runs on one laptop, so producer, Kafka, Flink and TimescaleDB compete for the same 16 threads.
- The HPET clock source makes any per-record clock read expensive (§3); this also applies to the planned
  ingestion timestamp and to `System.currentTimeMillis()` in Flink operators.
- CPU and busy-time values are averages over one run; the full week ran twice with Kafka, the failover test twice (2 kills;
  Iggy had 7).
- Messages are CSV text, one row per Kafka record. Several rows per record would cut the per-record cost but
  would change offsets from one per row to one per batch.
