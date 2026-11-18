# Measurements

Throughput of the Iggy → Flink source with the official connector
(`org.apache.iggy:flink-connector:0.10.0`). Setup and connector findings are in [README.md](README.md).

## Question

Is a single source reader fast enough, given that the connector gives every partition to one reader?
(See README: one reader gets all splits; the second stays idle.)

## Setup

| Item | Value |
|---|---|
| Date | 2026-11-16 |
| Host | Laptop, AMD Ryzen 7 5825U (8 cores / 16 threads), 35 GB RAM, Linux 7.3 (Fedora), Docker 29.10. All containers on one host. |
| Iggy | 0.10.0, 2 shards, cores not pinned, 1 topic `ticks` with 4 partitions (source and producer benchmarks; the sink test and the current default use 1 partition) |
| Flink | 2.3.0, 1 JobManager + 2 TaskManagers × 1 slot, parallelism 2, checkpoints every 10 s |
| Active source readers | 1 in every run (connector behaviour, see README) |
| Producer | `replay-producer`: one thread, 4000 rows per batch, one send call per partition. The partition is chosen by symbol hash. Two implementations: Java (`producer/`) and Rust (`producer-rs/`, a drop-in replacement with the same CLI, env vars and partitioning), chosen with `PRODUCER_DIR`. |
| Clock source | **HPET.** The kernel marked the TSC unstable at boot, so every clock read costs ~1.4 µs instead of ~20 ns. This matters for the producer comparison below. |
| Message | One CSV row as UTF-8 text, about 87 B on average for the full-day file |

## Method

Script: [`bench/source_throughput.py`](bench/source_throughput.py). Each run wipes the Iggy volume.

1. **Producer alone:** load the whole file into Iggy while Flink is stopped. This measures the producer.
2. **Drain:** for each poll batch size, create a new consumer group (so it reads the same backlog from
   offset 0). Start Flink with `PIPELINE=count` (source → counter → discard) and time how long the
   backlog takes to drain. This measures the reader alone.
3. **Live:** fresh Iggy, Flink with the full pipeline (source → counter → `keyBy(symbol)` counter on both
   TaskManagers, plus a verifier with parallelism 1 that checks duplicates and gaps per partition), and
   the producer at full speed at the same time.

The rate comes from the counter reports (one per second), from the first non-zero report to the report
that reaches the expected total. CPU is a single `docker stats` sample taken during the run.

## Results: full trading day (main result)

Data: `data/debs2022-gc-trading-day-08-11-21.csv`, 4.67 GB, **53,994,554 rows** (after skipping
13 comment/header lines), 5,493 symbols, all event types (not filtered).

```bash
python3 bench/source_throughput.py --repeat 1 --data-dir ./data \
    --csv debs2022-gc-trading-day-08-11-21.csv --timeout 1800
```

| Run | Poll batch | Throughput | Duration | CPU: TaskManagers | CPU: Iggy |
|---|---:|---:|---:|---|---:|
| Producer alone | – | **1,506k msg/s** | 35.9 s | – | – |
| Drain, source only | 100 (default) | **265k msg/s** | 203 s | 212% / 100% | 61% |
| Drain, source only | 1000 | **925k msg/s** | 58 s | 100% / 118% | 32% |
| Drain, source only | 5000 | **1,214k msg/s** | 44 s | 123% / 101% | 21% |
| Live, full pipeline + producer | 5000 | **830k msg/s** | 65 s | 157% / 130% | 19% |

Correctness in the live run: verifier total = distinct = 53,994,554, **0 duplicates, 0 gaps**.

In the live run the producer took 53.1 s (about 1.0M msg/s, slower than alone because the job was reading
at the same time). The job finished about 8–9 s after the producer, so it lagged a little behind the
producer at full speed.

### Repeat with the Rust producer

The same benchmark was run twice more, with `--producer-dir producer-rs`: first with the initial
Rust producer, then with the fixed one (see the next section). The drain runs do not involve the producer, so
the three runs give three samples of the same reader measurement.

| Run | Run 1: Java producer | Run 2: Rust, initial | Run 3: Rust, fixed |
|---|---:|---:|---:|
| Producer alone | 1,506k msg/s (35.9 s) | 509k msg/s (106.1 s) | **2,049k msg/s (26.4 s)** |
| Drain, poll batch 100 | 265k msg/s | 234k msg/s | 253k msg/s |
| Drain, poll batch 1000 | 925k msg/s | 957k msg/s | 941k msg/s |
| Drain, poll batch 5000 | 1,214k msg/s | 1,188k msg/s | 1,162k msg/s |
| Live: producer time | 53.1 s | 120.3 s | 39.9 s |
| Live: job throughput | 830k msg/s | 448k msg/s | **831k msg/s** |
| Live: limited by | the job | the producer | the job |
| Live: verifier | 0 duplicates, 0 gaps | 0 duplicates, 0 gaps | 0 duplicates, 0 gaps |

- The reader numbers reproduce: across the three runs the drains vary by ±6% (batch 100) and ±2–3%
  (batches 1000 and 5000).
- With a fast enough producer, the live pipeline levels off at about **830k msg/s**, with both the Java and
  the fixed Rust producer. That is the job's limit on this host, not the producer's.

## Producer: Java vs Rust

Script: [`bench/producer_compare.sh`](bench/producer_compare.sh). Producer alone, full trading day
(53,994,554 rows), Flink stopped, a fresh Iggy for every run, and the two producers alternating for 3 rounds.
The time is the producer's own measurement, from connect to the last send (no JVM or container startup).

| Producer | Runs | Mean | Throughput |
|---|---|---:|---:|
| Java (`producer/`) | 36.3 / 37.8 / 40.2 s | 38.1 s | 1.42M msg/s |
| Rust, initial | 106.7 / 110.7 / 107.2 s | 108.2 s | 0.50M msg/s |
| Rust, initial, TCP_NODELAY off | 108.9 / 108.5 / 106.8 s | 108.0 s | 0.50M msg/s |
| Java (second session) | 38.6 / 40.5 / 37.5 s | 38.9 s | 1.39M msg/s |
| **Rust, fixed** | 29.4 / 27.6 / 27.6 s | **28.2 s** | **1.92M msg/s** |

The fixed Rust producer is **1.38× faster than Java** (standard deviation 1.0 s vs 1.5 s). The initial version was
2.8× slower. Both send byte-identical payloads to the same partitions: with `pala.csv`, both give
per-partition counts `[40, 64, 52, 44]`.

### Why the first Rust version was slow

1. **Not TCP_NODELAY.** The Rust SDK leaves Nagle's algorithm on by default (`nodelay=false`), and the
   Java SDK turns it on. Turning it on made no difference (108.2 s vs 108.0 s). The producer still sets it
   (`IGGY_TCP_NODELAY`, default `true`) to match Java.
2. **The producer was CPU-bound.** It used ~92% of one core while Iggy used 18%.
3. **The time went into building the messages, not sending them.** Timing breakdown (`PROFILE=true`, a
   native run of the full day):

   | Version | Total | Build messages | `send_messages` | Read file, other |
   |---|---:|---:|---:|---:|
   | Initial, `DRY_RUN=true` (no sends) | 85.0 s | 80.3 s | 0 s | 4.6 s |
   | Initial | 109.4 s | 81.1 s | 24.3 s | 4.0 s |
   | Fixed | 31.1 s | 4.2 s | 22.7 s | 4.2 s |

4. **A micro-benchmark** ([`producer-rs/examples/microbench.rs`](producer-rs/examples/microbench.rs)) showed
   where the time went:

   | Step per row | ns/row |
   |---|---:|
   | clone line, find symbol, Java hash, HashSet lookup | 2–17 each |
   | `IggyTimestamp::now()` (= `SystemTime::now()`) | **1,367** |
   | `IggyMessage::from(Vec<u8>)` | 1,413 |

   The SDK's message builder sets `origin_timestamp` from the system clock **for every message**. On this
   laptop the kernel uses **HPET** as the clock source ("TSC found unstable after boot, most likely due to
   broken BIOS"), so each clock read costs ~1.4 µs instead of ~20 ns. That adds up to about 75 s for 54M
   messages. The Java SDK sends `origin_timestamp = 0` and never reads the clock.

**Fix:** the Rust producer builds `IggyMessage` directly and sets `origin_timestamp` once per batch
(one clock read per 4000 rows). On a host whose clock source is TSC, the original version would have lost only about 1 s,
so this slowdown is specific to this laptop.

## Sink: Flink JDBC connector on Flink 2.3 with TimescaleDB (2026-11-18)

Question: does the Flink JDBC connector, whose newest release is `flink-connector-jdbc-core:4.1.0-2.2`
(built for Flink 2.2 and compiled against Flink 2.0.0), work on Flink 2.3?

Script: [`bench/jdbc_sink_test.py`](bench/jdbc_sink_test.py). Job: `PIPELINE=jdbc` in
[`JdbcSinkPipeline.java`](flink-job/src/main/java/scalable/flink/JdbcSinkPipeline.java):
- price events only (`Last` set); event time = `Date` + `Trading time`
- watermarks with 60 s bounded out-of-orderness and 10 s idleness
- `keyBy(symbol)`, 5-minute tumbling event-time windows that keep the last price by event time and a tick count
- `JdbcSink` (at-least-once) writing `INSERT … ON CONFLICT (symbol, window_end) DO UPDATE` into the hypertable `window_close`
  ([schema](config/timescaledb/init.sql)). A `writes` column counts how many times each row was written.

Data: full trading day (53,994,554 rows, 11,856,732 price events), poll batch 5000, checkpoints every 10 s,
TimescaleDB 2.31.0 (PostgreSQL 17), PostgreSQL JDBC driver 42.7.12.

### Result: the connector works on Flink 2.3

| Phase (1-partition topic) | Result |
|---|---|
| A: baseline | 381,395 window rows, each written once, 37 late events. **Identical digest in two separate fresh runs.** About 93 s from start to the last row. |
| B: same data again, table not truncated | Same 381,395 rows, **identical content**, and every row has `writes` = 2. The upsert is idempotent. |
| C: kill a TaskManager mid-run (recovered runs) | The sink behaved correctly: windows re-emitted after the restore were upserted (2,965 and 6,001 rows with `writes` > 1), with no duplicate keys and no extra rows. **But data was lost upstream** (see below). |
| D: no idleness (`IDLENESS_S=0`) | 0 rows. Details below. |

### Finding 1: with several partitions, about half the events arrive late

With the default 4-partition topic, phase A counted **6,404,050 late events (54% of the price events)** and wrote
only **193,423 rows**, half of the 381,395 the 1-partition run writes. A repeat (phase B) gave a different
result: 195,030 rows and 6,437,919 late events.

The cause is the connector, not the data. In file order only **41 of 11,856,732** price events are more
than 60 s behind the running maximum. That was checked with `awk` over the file, and the 1-partition job
saw 37. The connector merges all partitions in one reader into a single queue, and there is one watermark
per reader, not per partition (split). The partition furthest ahead in event time pushes the watermark
forward, so events from the other partitions are late. Which events that hits depends on poll timing, so
runs don't match each other.

**Consequence:** with this connector, event-time results are only correct with a **1-partition topic**, or
with a custom source that emits per-split watermarks. One partition costs nothing, because the connector
uses only one reader anyway (see the README).

### Finding 2: failover loses data (at-most-once delivery, now measured)

Phase C compared with phase A, 1 partition, in two runs that recovered:

| Run | Restored from | Ticks lost | Windows missing | Windows with fewer ticks | Windows with more ticks |
|---|---|---:|---:|---:|---:|
| 1 | checkpoint 3 | 180,167 (1.5%) | 1,480 | 5,624 | 0 |
| 2 | checkpoint 4 | 128,064 (1.1%) | 287 | 5,657 | 0 |

Flink restores the window state from the checkpoint, but the connector resumes reading from the offset
already committed in the Iggy consumer group. Every event read between the checkpoint and the crash is
skipped. In run 1, all damaged windows fell between 10:35 and 11:25 in event time, the part of the data
being processed around the crash. This confirms the at-most-once behaviour that the connector code had
suggested (see README).

### Finding 3: failover sometimes does not recover at all

In one of three runs, after the kill, the job reported RUNNING again (2 restarts, restored from the
checkpoint) but **stopped producing output**. It wrote 554 more rows and then stopped, leaving 261,149
windows missing. Four more kills, alternating between the two TaskManagers and capturing coordinator logs
in case of a stall, all recovered. **In total, 1 of 7 failovers stalled.** The cause is not known, because
the stall was not reproduced with diagnostics on. It looks like the same kind of problem as the split loss
without checkpointing (README): the split is assigned to no reader, so the job is RUNNING but reads nothing.
This is unconfirmed. Check after any failover that output resumes, for example that the Iggy consumer-group
offset moves forward.

### Finding 4: without idleness, windows never fire and checkpoints fail

Phase D (`IDLENESS_S=0`), confirmed in two runs:
- **The watermark never moves:** the window operator's watermark stays at `Long.MIN_VALUE`, because the
  second source reader never gets a split and holds back the minimum.
- **No window fires** and every tick stays in window state.
- **Checkpoints fail:** within a few checkpoints the state passes 6 MB, more than the 5 MB limit of Flink's
  default in-memory (JobManager) checkpoint storage. The checkpoint fails ("Size of the state is larger than
  the maximum permitted memory-backed state") and the job restarts in a loop.

`withIdleness` is therefore required, and **checkpoints must go to a file system**.

**Fixed (2026-11-18):** compose now sets `execution.checkpointing.storage: filesystem` and
`execution.checkpointing.dir: file:///flink-checkpoints`, on a `flink-checkpoints` volume shared by the JobManager
and TaskManagers. Phase D repeated with this setting: the window state grew to **36.6 MB** (about 381k open windows,
7× the old limit), checkpoints kept completing to `file:/flink-checkpoints/<job>/chk-N`, and the job had **0
failures** in 90 s. The REST API counted 1 failed checkpoint, which did not fail the job; its cause was not
checked. Phases A–C gave the same results as with in-memory storage: the same phase A digest, and a failover
restored from the file checkpoint lost 1.0% of the ticks (994 windows missing, 5,644 short, 2,993 re-emitted
and upserted).

### Setup issues found along the way

- `flink-connector-base` is already in `flink-dist` 2.3.0, so it is `provided`. Only the JDBC connector and
  the PostgreSQL driver are bundled.
- TimescaleDB in Docker needs `shm_size` above Docker's 64 MB default. Otherwise parallel queries fail with
  "could not resize shared memory segment … No space left on device".
- Flink 2.3's REST API reports where a subtask runs as `endpoint` (container hostname and port), not `host`.
- The REST `write-records` metric of the source vertex often read 0 while data was flowing. Don't rely on it.

## Results: `docs/pala.csv` repeated (first run, superseded)

Data: 200-row sample, repeated 10,000× = 2,000,000 messages.

| Run | Poll batch | Throughput | Duration |
|---|---:|---:|---:|
| Producer alone | – | 212k msg/s | 9.5 s |
| Drain, source only | 100 | ~210k msg/s | 9 s |
| Drain, source only | 1000 | ~540k msg/s | 3 s |
| Drain, source only | 5000 | ~735k msg/s | 2 s |
| Live, full pipeline + producer | 5000 | 153k msg/s | 13 s |

These numbers are lower and less reliable than the full-day results:
- The producer sends a partial batch at the end of each pass over the file, so a 200-row file gives
  200-message batches instead of 4000. The producer limited the live run.
- The counters report once per second, so the 2–3 s runs are accurate only to about ±30–50%.

Earlier, before the producer batched per partition, it sent about one message per call: about 3.3k msg/s.

## Observations

- **The poll batch size is the biggest lever.** Going from 100 (the connector default) to 5000 is
  4.6× faster. Set `IGGY_POLL_BATCH` to 1000 or more.
- **One reader is enough for this dataset.** At the live rate, one day (54M) takes about 65 s. The whole week
  (about 289M events) would take about 6 min, and price events only (about 59M) about 70 s. A real job will
  be slower, because windows and EMA state replace the counters.
- **Iggy is not the bottleneck.** It used only 20–60% of one core.
- **The idle reader wastes about one core.** In every run one TaskManager sat at about 100% CPU without
  reading anything. The connector's `isAvailable()` always returns a completed future, so a reader with
  no splits busy-polls.
- **The single reader thread is the likely limit** for the source-only drains. The active TaskManager
  ran at about 100–210% (reader thread plus network and checkpoint threads). This is not profiled.
- **The downstream pipeline costs about a third** (830k vs 1,214k msg/s). The verifier with parallelism 1
  sees every record, so it probably accounts for part of that. This is not measured separately.

- **The Rust producer is now the faster one** (1.92M vs 1.39–1.42M msg/s), but it doesn't change the end-to-end
  result: with either producer, the live pipeline is limited by the job at about 830k msg/s.

## Caveats

- Everything ran on one laptop, so producer, Iggy and Flink compete for the same cores and memory bus.
- **The clock source is HPET, so every clock read costs about 1.4 µs.** Anything that reads the clock once per
  record pays that cost on this host. That includes the ingestion timestamp and `emit_ts` planned for the
  latency measurements, and any `System.currentTimeMillis()` or `nanoTime()` call in Flink operators. Check
  the evaluation host with `cat /sys/devices/system/clocksource/clocksource0/current_clocksource` (it should
  say `tsc`), or read the clock once per batch.
- The reader drains ran 3 times (once per full-day run) and the producer comparison 3 rounds per variant.
  Every other configuration ran once.
- CPU values are single samples, not averages.
- Messages are CSV text, not the binary format planned in architecture.md §3.1.
