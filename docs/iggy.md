# Apache Iggy: tests, measurements and why it was replaced

From 2026-11-16 to 2026-11-20 the message broker was **Apache Iggy 0.10.0**, read and written by Flink through the
official connector `org.apache.iggy:flink-connector:0.10.0`. On 2026-11-20 it was replaced by **Apache Kafka**
(see [`architecture.md`](architecture.md) §1.1 and [`MEASUREMENTS.md`](../MEASUREMENTS.md)). This file keeps
what was tested and measured with Iggy. The code is in the git history before the switch.

## 1. What was used

| Part | Iggy setup |
|---|---|
| Broker | `apache/iggy:0.10.0`, one node. Stream `trading`; topics `ticks` (input) and `advisories` (output); consumer group `flink`. Needed explicit container settings: bind addresses `0.0.0.0` (the default is loopback), an explicit shard count (`IGGY_SHARDING_CPU_ALLOCATION`; the default NUMA-aware allocation fails inside Docker), core pinning off, `SYS_NICE`, unlimited memlock. |
| Setup | `iggy-setup` (Rust, same image as the producer): created the stream, the topics and the consumer group. The connector does not create the consumer group itself. |
| Producer | Rust, `iggy` crate 0.12.0: one thread, 4000 rows per batch, one `send_messages` call per partition, partition = Java `String.hashCode()` of the symbol. `origin_timestamp` set once per batch (§4, "Why the first Rust version was slow"). A Java producer was used until 2026-11-19. |
| Flink source | `IggySource`, poll batch `IGGY_POLL_BATCH` (5000 recommended; the default 100 is 4.6× slower). Checkpointing always on; `withIdleness` required. |
| Flink sink | `IggySink` for topic `advisories`, batch size 1 (`IGGY_ADVISORY_BATCH`), balanced partitioning. |
| Topic partitions | `ticks`: **1** (more partitions made events late, §3). `advisories`: **1** (the sink picks one partition per batch, not per message). |
| Metrics | Iggy's `/metrics` needs a login token in 0.10, so Prometheus did not scrape it. |

## 2. Why it was replaced

The Iggy server was never the problem: it used 20–60% of one core in every run. The limits were in the
Flink connector 0.10.0, and the project's next steps (scaling experiments, failover tests, exactly-once results)
depend on exactly those parts.

| Problem with the Iggy connector | Evidence | With Kafka (`flink-connector-kafka` 5.0.0-2.2) |
|---|---|---|
| **Source reading does not scale out.** The split enumerator assigns splits only when a reader registers, so the first reader gets every partition and the others stay idle. | Every run had one active reader (§3). In the 2026-11-20 full week, that reader was **busy 95% of the time** and was the bottleneck of the whole job (§5). | Partitions are spread over all source subtasks. Full week: 4 readers, each busy ~23% ([MEASUREMENTS.md](../MEASUREMENTS.md) §2). |
| **One watermark per reader, not per partition**, so the `ticks` topic had to have 1 partition. | With 4 partitions **54% of the price events arrived late** and half the windows were lost; results differed between runs (§4, Finding 1). | One watermark per partition (split). Full week with 8 partitions: output identical to the Iggy 1-partition run ([MEASUREMENTS.md](../MEASUREMENTS.md)). |
| **At-most-once delivery.** Offsets are committed to Iggy when a batch is polled; the offsets in the Flink checkpoint are not used for reading. | A TaskManager kill lost **1.0–1.5% of a day's price events** (§4, Finding 2). | Offsets are part of the checkpoint; a restore re-reads from them. Measured: 2 TaskManager kills, 0 events lost, results identical to a run without failure ([MEASUREMENTS.md](../MEASUREMENTS.md) §4). |
| **Failover not reliable.** Without checkpointing, `addSplitsBack` loses the splits; with it, 1 of 7 failovers still left the job RUNNING but reading nothing. | §3, §4 Finding 3. | Both failovers recovered ([MEASUREMENTS.md](../MEASUREMENTS.md) §4). |
| **An idle reader busy-polls** (`isAvailable()` always returns a completed future). | Each idle reader used about **one core** doing nothing (§3, §5). This also ruled out a runtime `subscriptions` topic (architecture §3.6). | Every reader has partitions (8 partitions, 4 readers); a reader without data waits instead of polling. |
| **The sink picks one partition per batch** and flushes a partial batch only at a checkpoint. | `advisories` had to have 1 partition and batch size 1. | Partition per record from its key (the symbol), records sent at once (`linger.ms=0`). |
| **Immature ecosystem.** | The connector logged every message at INFO; no broker metrics without a token; the Rust SDK read the clock per message (§4, Producer). | Widely used connector and tools (`kafka-consumer-groups.sh` for lag, console consumer). |

Writing a custom Iggy source (explicit offsets in checkpoint state, per-split watermarks, deterministic split
assignment) was the planned fallback (architecture §7). Switching to Kafka gave all of this with a maintained
connector, at the cost of rewriting the producer, the topic setup and the source/sink code.

## 3. Connector findings and first throughput test

### Connector findings (2026-11-16)

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

### Single-reader throughput, first run (2026-11-16)

`bench/source_throughput.py --repeat 10000` (removed on 2026-11-20 with the connector test pipelines; see git history). Setup: 2M messages (~110 B CSV rows),
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

## 4. Measurements (former MEASUREMENTS.md, 2026-11-16 to 2026-11-18)

### Question

Is a single source reader fast enough, given that the connector gives every partition to one reader?
(See §3: one reader gets all splits; the second stays idle.)

### Setup

| Item | Value |
|---|---|
| Date | 2026-11-16 |
| Host | Laptop, AMD Ryzen 7 5825U (8 cores / 16 threads), 35 GB RAM, Linux 7.3 (Fedora), Docker 29.10. All containers on one host. |
| Iggy | 0.10.0, 2 shards, cores not pinned, 1 topic `ticks` with 4 partitions (source and producer benchmarks; the sink test and the current default use 1 partition) |
| Flink | 2.3.0, 1 JobManager + 2 TaskManagers × 1 slot, parallelism 2, checkpoints every 10 s |
| Active source readers | 1 in every run (connector behaviour, see §3) |
| Producer | `replay-producer`: one thread, 4000 rows per batch, one send call per partition. The partition is chosen by symbol hash. Two implementations: Java (`producer/`) and Rust (`producer-rs/`, a drop-in replacement with the same CLI, env vars and partitioning), chosen with `PRODUCER_DIR`. *The Java producer, `PRODUCER_DIR` and `bench/producer_compare.sh` were removed on 2026-11-19; only the Rust producer remains. The Java results below are kept for reference.* |
| Clock source | **HPET.** The kernel marked the TSC unstable at boot, so every clock read costs ~1.4 µs instead of ~20 ns. This matters for the producer comparison below. |
| Message | One CSV row as UTF-8 text, about 87 B on average for the full-day file |

### Method

Script: `bench/source_throughput.py` (wipes the Iggy volume on each run). *The script and the `count` / `full`
connector test pipelines it used were removed on 2026-11-20; they are in the git history (commit `3982122`).
The results below are kept for reference.*

1. **Producer alone:** load the whole file into Iggy while Flink is stopped. This measures the producer.
2. **Drain:** for each poll batch size, create a new consumer group (so it reads the same backlog from
   offset 0). Start Flink with `PIPELINE=count` (source → counter → discard) and time how long the
   backlog takes to drain. This measures the reader alone.
3. **Live:** fresh Iggy, Flink with the full pipeline (source → counter → `keyBy(symbol)` counter on both
   TaskManagers, plus a verifier with parallelism 1 that checks duplicates and gaps per partition), and
   the producer at full speed at the same time.

The rate comes from the counter reports (one per second), from the first non-zero report to the report
that reaches the expected total. CPU is a single `docker stats` sample taken during the run.

### Results: full trading day (main result)

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

#### Repeat with the Rust producer

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

### Producer: Java vs Rust

Script: `bench/producer_compare.sh` (removed with the Java producer on 2026-11-19; see git history). Producer alone, full trading day
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

#### Why the first Rust version was slow

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

4. **A micro-benchmark** (`producer-rs/examples/microbench.rs`, removed with the Iggy SDK on 2026-11-20) showed
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

### Sink: Flink JDBC connector on Flink 2.3 with TimescaleDB (2026-11-18)

Question: does the Flink JDBC connector, whose newest release is `flink-connector-jdbc-core:4.1.0-2.2`
(built for Flink 2.2 and compiled against Flink 2.0.0), work on Flink 2.3?

Script `bench/jdbc_sink_test.py` (Iggy version in git history; the Kafka version was removed after commit
`0d3acfe`). Job: `PIPELINE=jdbc` in `JdbcSinkPipeline.java` (also removed):
- price events only (`Last` set); event time = `Date` + `Trading time`
- watermarks with 60 s bounded out-of-orderness and 10 s idleness
- `keyBy(symbol)`, 5-minute tumbling event-time windows that keep the last price by event time and a tick count
- `JdbcSink` (at-least-once) writing `INSERT … ON CONFLICT (symbol, window_end) DO UPDATE` into the hypertable `window_close`
  ([schema](../config/timescaledb/init.sql)). A `writes` column counts how many times each row was written.

Data: full trading day (53,994,554 rows, 11,856,732 price events), poll batch 5000, checkpoints every 10 s,
TimescaleDB 2.31.0 (PostgreSQL 17), PostgreSQL JDBC driver 42.7.12.

#### Result: the connector works on Flink 2.3

| Phase (1-partition topic) | Result |
|---|---|
| A: baseline | 381,395 window rows, each written once, 37 late events. **Identical digest in two separate fresh runs.** About 93 s from start to the last row. |
| B: same data again, table not truncated | Same 381,395 rows, **identical content**, and every row has `writes` = 2. The upsert is idempotent. |
| C: kill a TaskManager mid-run (recovered runs) | The sink behaved correctly: windows re-emitted after the restore were upserted (2,965 and 6,001 rows with `writes` > 1), with no duplicate keys and no extra rows. **But data was lost upstream** (see below). |
| D: no idleness (`IDLENESS_S=0`) | 0 rows. Details below. |

#### Finding 1: with several partitions, about half the events arrive late

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
uses only one reader anyway (§3).

#### Finding 2: failover loses data (at-most-once delivery, now measured)

Phase C compared with phase A, 1 partition, in two runs that recovered:

| Run | Restored from | Ticks lost | Windows missing | Windows with fewer ticks | Windows with more ticks |
|---|---|---:|---:|---:|---:|
| 1 | checkpoint 3 | 180,167 (1.5%) | 1,480 | 5,624 | 0 |
| 2 | checkpoint 4 | 128,064 (1.1%) | 287 | 5,657 | 0 |

Flink restores the window state from the checkpoint, but the connector resumes reading from the offset
already committed in the Iggy consumer group. Every event read between the checkpoint and the crash is
skipped. In run 1, all damaged windows fell between 10:35 and 11:25 in event time, the part of the data
being processed around the crash. This confirms the at-most-once behaviour that the connector code had
suggested (§3).

#### Finding 3: failover sometimes does not recover at all

In one of three runs, after the kill, the job reported RUNNING again (2 restarts, restored from the
checkpoint) but **stopped producing output**. It wrote 554 more rows and then stopped, leaving 261,149
windows missing. Four more kills, alternating between the two TaskManagers and capturing coordinator logs
in case of a stall, all recovered. **In total, 1 of 7 failovers stalled.** The cause is not known, because
the stall was not reproduced with diagnostics on. It looks like the same kind of problem as the split loss
without checkpointing (§3): the split is assigned to no reader, so the job is RUNNING but reads nothing.
This is unconfirmed. Check after any failover that output resumes, for example that the Iggy consumer-group
offset moves forward.

#### Finding 4: without idleness, windows never fire and checkpoints fail

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

#### Setup issues found along the way

- `flink-connector-base` is already in `flink-dist` 2.3.0, so it is `provided`. Only the JDBC connector and
  the PostgreSQL driver are bundled.
- TimescaleDB in Docker needs `shm_size` above Docker's 64 MB default. Otherwise parallel queries fail with
  "could not resize shared memory segment … No space left on device".
- Flink 2.3's REST API reports where a subtask runs as `endpoint` (container hostname and port), not `host`.
- The REST `write-records` metric of the source vertex often read 0 while data was flowing. Don't rely on it.

### Results: `docs/pala.csv` repeated (first run, superseded)

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

### Observations

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

### Caveats

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

## 5. Last full-week run with Iggy (2026-11-20)

Same laptop as §4. 1 JobManager, 8 TaskManagers × 1 slot, **parallelism 2** (so 6 TaskManagers had no work),
`ticks` with 1 partition, poll batch 5000, checkpoints every 10 s. The whole week (7 files) replayed into a
fresh stack while the job ran.

| Item | Result |
|---|---|
| Producer | 289,116,808 rows in about 3 min 5 s (per day 39.2, 34.4, 37.6, 31.9, 33.7, 0.02, 0.02 s) |
| Job | last `ema_window` row about 5 min after the replay started (replay started 19:36:13 UTC; the output had not changed for at least 40 s at 19:41:57, so the last row was written at about 19:41:07–19:41:17), about 2 min after the producer finished |
| Output | 1,855,645 `ema_window` rows for 4,970 symbols; 7,769 BUY, 3,580 SELL; data issues: 6,681 `TIME_SKEW`, 5,506 `LATE`, 638 `PRICE_JUMP`; 73 messages in topic `advisories`. Same as the earlier runs. |
| Errors | none in the JobManager and TaskManager logs |

Per-task metrics during the run (Prometheus, 19:36–19:41 UTC):

| Task | Busy (ms/s) | Backpressured (ms/s) | Records out/s |
|---|---:|---:|---:|
| Source + quality gate, subtask 0 | **947** | 53 | **213k** price events (≈ 1M input rows/s) |
| Source + quality gate, subtask 1 | 1000 | 0 | **0** (idle reader busy-polling) |
| 5-min window, ×2 | ~110 | ~50 | ~3.3k |
| EMA + TimescaleDB writer, ×2 | ~680 | 0 | – |
| Data-issues writer, ×2 | <10 | 0 | – |

CPU (cAdvisor, average): TimescaleDB 135%, the active-source TaskManager 114%, the idle-reader TaskManager
93%, Iggy 47%, the other six TaskManagers about 2% each.

**Bottleneck: the single Iggy source reader.** It was busy 95% of the time, while the next operator (EMA +
JDBC writer) was at 68% and the window operator only occasionally backpressured. More partitions or more
TaskManagers could not help: more partitions make events late (§4, Finding 1), and more readers get no splits.

The output of this run (`ema_window`, `data_issue`) was kept and compared with the first Kafka run: identical.
