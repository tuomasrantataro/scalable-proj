# Architecture: Detecting Trading Trends in Financial Tick Data

Companion to [`requirements.md`](requirements.md). Requirement IDs (FR-x, NFR-x) refer to that document.

Last updated 2026-11-23, after the full-week run with corrected measurements. Findings:
[`MEASUREMENTS.md`](../MEASUREMENTS.md) (Kafka; current results in §7), [`iggy.md`](iggy.md) (Iggy tests and why it was replaced),
[`reference/README.md`](../reference/README.md).

## 1. Technology Decisions

### 1.1 Decided
| Concern | Choice | Rationale |
|---|---|---|
| Message streaming | **Apache Kafka**, one broker in KRaft mode | Persistent, partitioned topics. Decouples data replay from processing, so input rates can be controlled for load tests. Chosen for its Flink connector: the source spreads partitions over all readers, keeps one watermark per partition and stores offsets in Flink checkpoints. **Replaced Apache Iggy on 2026-11-20**: the Iggy connector had one active reader (the job's bottleneck), one watermark per reader (1-partition topic), at-most-once delivery and unreliable failover ([`iggy.md`](iggy.md)). |
| Stream processing | **Apache Flink** | Event-time tumbling windows, watermarks for out-of-order data, keyed state for the EMAs, exactly-once checkpointing, and rescaling of parallelism. |
| Deployment | **Docker Compose** | Single-command, reproducible local or single-VM setup. Flink TaskManagers can be scaled with `docker compose up --scale`. |
| Versions | **Kafka 4.3.2** (`apache/kafka` image), **Flink 2.3.0**, **`flink-connector-kafka` 5.0.0-2.2** (kafka-clients 4.2.0) | No connector release for Flink 2.3 exists yet; 5.0.0-2.2 (built for Flink 2.2) works on 2.3: full week identical to the earlier results, failover tested ([`MEASUREMENTS.md`](../MEASUREMENTS.md)). Same situation as the JDBC connector. |
| Language | **Java 17** for the Flink job. Replay producer and `kafka-setup` in **Rust** (`producer-rs/`, `rdkafka` 0.39 = librdkafka 2.12, linked statically) | Kept from the Iggy setup, where Rust was faster than Java ([`iggy.md`](iggy.md) §4). librdkafka reads the clock for every record; on this laptop (HPET clock source) each read is a ~1.4 µs system call, so the producer uses 8 sender threads: 1.41M msg/s alone for one trading day, 1 thread only 0.30M ([`MEASUREMENTS.md`](../MEASUREMENTS.md)). |
| Kafka → Flink integration | **`KafkaSource`** (`flink-connector-kafka`), topic `ticks` with 8 partitions, checkpointing on | One split per partition, spread over all source subtasks; one watermark per split, so the partition count does not change the results (§3.4); offsets are part of the checkpoint, so a restore re-reads exactly from it (§4.3). Starts from the consumer group's committed offsets (committed at each completed checkpoint), or from the earliest offset for a new group. `max.poll.records` 5000. |
| Result storage | **TimescaleDB 2.31 (PostgreSQL 17)**, written by the Flink JDBC sink (`flink-connector-jdbc-core:4.1.0-2.2` + PostgreSQL driver 42.7.12) with `INSERT … ON CONFLICT (symbol, window_end) DO UPDATE` | Exact, idempotent upserts on a real primary key, so replays after a restart are harmless. Plain SQL for the pandas correctness comparison, and a built-in Grafana data source. Tested on Flink 2.3 (no build for 2.3 exists yet): identical results across runs, idempotent re-runs, and re-emitted windows absorbed after failover ([`MEASUREMENTS.md`](../MEASUREMENTS.md)). Rejected: InfluxDB (no maintained Flink connector, awkward for relational checks) and ClickHouse (upserts only eventually deduplicated; its scale is not needed: at most ~1.6M rows/day). |
| Reference implementation | **Python + pandas batch script** ([`reference/`](../reference/README.md)), pyarrow for reading the CSV files, matplotlib for figures | Written from the requirements alone, not from the Flink code, so it can find errors in it (NFR-9). Whole week in ~2.5 min on 4 cores. EMAs evaluated in the same order as FR-11, so values can be compared exactly. Checked against unit tests and an independent `pandas.ewm` computation (identical). It models the stream's late-event rule (watermark = max event time − D), so its advisories are what a correct stream must emit. |
| Flink state backend | **HashMap (heap)**, `state.backend.type: hashmap` in `docker-compose.yml`, with file-system checkpoint storage on the shared volume | State is small: per symbol a few doubles (EMAs, previous EMAs) plus the open 5-min window, for about 5,500 symbols, so it fits easily on the TaskManager heap, and heap access avoids RocksDB's serialization on every read and write. Explicit even though it is Flink 2.3's default, so the choice is visible and easy to switch. Verified: the job reports `HashMapStateBackend` with `FileSystemCheckpointStorage` (REST `/jobs/<id>/checkpoints/config`), and checkpoints complete. Rejected: RocksDB (needed only when state exceeds memory, or for incremental checkpoints of large state). |
| Visualization and metrics | **Grafana 12.2** (provisioned from `config/grafana/`) over TimescaleDB and **Prometheus 3.7**, which scrapes the Flink Prometheus reporter (JobManager and every TaskManager, port 9249) and **cAdvisor 0.52** (CPU and memory per container). Three dashboards (§5.1) | Quick to build, no code, and the results are already in SQL. Grafana shows the market data at its stored local time by fixing the dashboard time zone to UTC. Limits: no push to the browser (refresh polling), and cross-symbol views are SQL over 1.9M rows, so heavy panels take seconds. The Kafka broker is not scraped (it exposes JMX only); rows read and consumer lag come from the Flink Kafka source's metrics. cAdvisor reads containers through containerd, since Docker here uses the containerd image store, which its Docker handler cannot read; Compose labels are not visible there, so every service sets `SERVICE=<name>`. Network counters are not reported this way. | Custom web app (§1.2) |
| Advisory delivery | **Kafka topic `advisories`** (4 partitions, created by `kafka-setup`), written by `KafkaSink` (at-least-once, `linger.ms=0`): one record per subscribed trader and advisory (per-trader lists in `config/subscriptions.txt`, §3.6), key = symbol, value = hand-written JSON | Advisories must be emitted immediately (FR-15/16, NFR-3): each record is sent as soon as it is produced, and the sink flushes at every checkpoint. The symbol key keeps a symbol's advisories in window order within one partition. Exactly-once would need Kafka transactions, which publish only at checkpoints (up to 10 s later), so at-least-once is used; (trader, symbol, window_end) identifies a message, so consumers can drop repeats. Verified on the full week: exactly the 73 expected messages. The topic uses `message.timestamp.type=LogAppendTime` (set by `kafka-setup`): the sink would otherwise stamp records with their 2021 event time, so retention deleted them within minutes and the timestamp said nothing about delivery. Latency: §5. |

### 1.2 Proposed (not yet decided)
| Concern | Proposal | Alternatives |
|---|---|---|
| Visualization (future) | Custom web app with WebSocket push, which may replace the Grafana trading dashboard (§5.1) | — |

---

## 2. System Overview

```
                ┌──────────────────────────── docker compose ─────────────────────────────┐
                │                                                                         │
 CSV files ──►  │  replay-producer ──► Kafka broker ─────────► Flink JobManager           │
 (read-only     │  (rate control,     (KRaft, 1 node)          │                          │
  volume)       │   8 threads)        topic:  ticks (8 part.)   ▼                          │
                │                     topic:  advisories ◄── Flink TaskManager × k         │
                │                             (4 part.)        (scalable)                 │
                │                                               │                          │
                │                                               ▼                          │
                │                                         TimescaleDB ──► Grafana ◄── Prometheus
                │                                         (EMA, advisories)  (viz + metrics) ▲
                │                                                                 cAdvisor ──┘
                └─────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Components
| Service | Responsibility |
|---|---|
| `replay-producer` | Reads the daily CSV files in order and skips the `#` comment lines and the header. Drops irrelevant events early, or forwards everything (configurable, see §3.1). Sends one Kafka record per row (key = symbol ID), with the partition chosen **from the symbol ID hash**; librdkafka batches and lz4-compresses per partition. One reader thread and 8 sender threads, each partition always on the same sender (§3.1). Supports a speed-up factor (1×, 10×, …, max) and stamps an ingestion timestamp for latency measurement. *Implemented: partitioning, `RATE` (msg/s), `REPEAT`, `PRODUCER_THREADS`, replay at a multiple of the feed's pace (`SPEEDUP`) and of a part of a day (`START_TIME`/`END_TIME`), both by the feed's clock ([`feed.rs`](../producer-rs/src/feed.rs), §3.1), and evenly spread artificial symbols for skew comparisons (`UNIFORM_SYMBOLS`). Not yet implemented: event filtering, ingestion timestamp (the record timestamp is the send time, read once per chunk of up to 4096 rows).* |
| `kafka-setup` | One-shot job (same image as the producer). Creates the input topic `ticks` (8 partitions) and the output topic `advisories` (4 partitions), idempotently: existing topics are kept. Auto-creation of topics is off in the broker. Consumer groups need no setup. |
| `kafka` | One Kafka broker in KRaft mode (broker and controller in one process), `kafka:9092` for the containers and `localhost:9094` for the host. Single node: internal topics have replication factor 1. Data on the `kafka-data` volume. |
| `flink-jobmanager` | Runs the job in **standalone application mode**. |
| `flink-taskmanager` | Workers. Replicated with `--scale taskmanager=k`. |
| `timescaledb` | Stores EMA results and advisories (`ema_window`, view `advisories`) and the data quality report (`data_issue`, view `data_issue_summary`), for the visualization and correctness checks. Schema: [`config/timescaledb/init.sql`](../config/timescaledb/init.sql). All symbols are stored (FR-3), also for correctness checks; the subscription filter applies to advisory delivery only (§3.6). |
| `db-setup` | One-shot job: applies the idempotent schema, so new tables also reach an existing database volume (the image's init scripts run only on an empty volume). |
| `grafana` | Dashboards for the query results (smart visualization), correctness and system metrics (§5.1). http://localhost:3001, admin/admin, anonymous viewers allowed. |
| `prometheus`, `cadvisor` | Collect throughput, backpressure, checkpoint and resource metrics (Flink reporter, per-container CPU and memory). |

---

## 3. Data Flow and Processing

### 3.1 Ingestion (replay-producer)
1. Stream the CSV files one day at a time (Mon → Sun).
2. Parse each line and infer the event type from its non-NULL attributes. A **price event** is one with `Last` set. Other events are counted (for dataset analysis in the report) and optionally forwarded.
3. Build the event time from `Trading date` + `Trading time`.
4. Serialize to a compact binary format. Send to Kafka topic `ticks`, partitioned by hash of `ID.[Exchange]` (Java `String.hashCode()`, as in the earlier producers), with the symbol as record key. This preserves per-symbol order within a partition; each partition is always sent by the same producer thread, and idempotence (`enable.idempotence`) keeps the order across retries. *Currently the messages are CSV text, and binary serialization is still to do.*

**The feed's clock.** A row's `Time` is not its arrival time: early in each trading day's file there are snapshot rows with a much later `Time` (8 Nov, file row 41: 09:12:16). Pacing by it made the producer wait for those rows and send everything before them as one burst. The clock is now the median `Time` of blocks of 1,000 rows, smoothed over neighbouring blocks, and never decreases. On the five trading days it never jumps more than 1.0 s between 09:00 and 17:30, and the sweep's 10× and 100× points take the time they should (179.9 s of 180, 17.99 s of 18; [`MEASUREMENTS.md`](../MEASUREMENTS.md) §7).

Filtering non-price events in the producer reduces load substantially: about 59 M price events out of 289 M total. The filter can be switched to the Flink side to compare both designs.

### 3.2 Flink job topology
```
KafkaSource (one split per partition, spread over all source subtasks; one watermark per split, see §3.4)
  → parse (price rows only: `Last` set; byte-level, non-price rows create no objects)
  → assign timestamps + watermarks (bounded out-of-orderness)
  → quality gate ── dropped / suspicious events → data_issue (§3.8)
  → keyBy(symbol)
  → TumblingEventTimeWindows(5 min)  ── aggregate: last price by event time (Close), tick count
  │                                     late events → data_issue
  → KeyedProcessFunction (EMA38, EMA100, crossover detection; keyed state) ── price jumps → data_issue
  ├─► JDBC sinks → TimescaleDB (EMA per window incl. advisories, all symbols: own task; data issues)
  └─► advisories only → subscription filter: one copy per subscribed trader (§3.6)
        → Kafka sink → topic `advisories` (key symbol, JSON value; one record per trader and advisory)
```
*Implemented in [`flink-job/src/main/java/scalable/flink/ema/`](../flink-job/src/main/java/scalable/flink/ema/)). Full week (289 M rows, 59.4 M usable price events) in 4 min 36–39 s on one laptop, with the producer running at the same time; the job kept up with the producer. From a stored backlog: 82–84 s, about 3.5M rows/s, since the database writer sends multi-row upserts ([`MEASUREMENTS.md`](../MEASUREMENTS.md) §12; 135–145 s before, §7).*

The `ema_window` sink runs in its own task, so that its synchronous batches do not run in the EMA task's thread. The two are still coupled: the EMA task sends to both branches, so when the writer's input buffers are full it waits, and advisories wait with it (§4.1). The JDBC URL has `reWriteBatchedInserts=true`, so each batch becomes multi-row upserts; a batch is flushed at 1,000 rows or after 1 s, so at normal load `written_at` is up to about 1 s after the EMA result (advisories do not wait for that). `EMA_SINK=discard` leaves the database out, to measure what it costs.

### 3.3 Windowing (FR-5 to FR-8)
- 5-minute **event-time tumbling windows**.
- Timestamps in the data are local exchange time without a zone. They are parsed as if they were UTC, so Flink's epoch-aligned windows start exactly at **00:00 local** (FR-6) without any offset handling.
- A window fires when the watermark passes its end. This happens after the next window has started (FR-8).
- The window function keeps only the **last price by event time** (not by arrival order), so out-of-order events within a window are handled correctly.

### 3.4 Watermarks and late events (NFR-7)
- Strategy: `forBoundedOutOfOrderness(D)`, with D configurable (`OUT_OF_ORDERNESS_S`, default 1 s; 60 s until 24 Nov 2026).
- **Measured disorder (reference implementation).** In file order, price events are in event-time order per symbol, with one exception: 5,506 rows at the end of 10 Nov re-send the open, day high and day low of 1,812 Paris indices with `Trading time 01:00`, 17.9 h late. Against one global watermark, 189 Xetra `Last = 0` records (`Trading time 00:00`, at the end of each day's file) are late too. Any D from 0 s to 17.9 h therefore gives identical results. A stream must drop these rows: kept, they become the close of a 01:00 window and change the EMAs of 1,812 symbols and about 1,790 advisories. D can be chosen for latency alone. *Confirmed in Flink (`MEASUREMENTS.md` §8): the full week gives the reference digest and 5,506 late events with D = 60, 5, 1, 0.5 and 0 s from a stored backlog, and with 1 and 0.5 s in live runs.*
- `withIdleness` so that idle partitions (nights, weekend, end of the replay) do not stall the watermark. *With the Iggy connector it was required in any case: its second reader never got a split and held the watermark at `Long.MIN_VALUE` ([`iggy.md`](iggy.md)). With Kafka every reader has partitions; idleness is still needed so that the last windows fire when the input stops.*
- **One watermark per partition.** The Kafka source tracks the watermark of each split and emits the minimum, so a partition that is ahead does not make the others late. *Measured: the full week with 8 partitions gives byte-identical `ema_window` and `data_issue` contents to the earlier 1-partition Iggy run. (The Iggy connector had one watermark per reader: with 4 partitions, 54% of the price events were late.)*
- **Watermark alignment (`WATERMARK_ALIGNMENT_S`, 300 s).** A partition whose watermark gets more than 5 min of event time ahead of the slowest partition is paused (checked every second). Without it, results depended on read timing when the job read a stored backlog: partitions drift apart by hours of event time, the slowest holds the watermark back, and events that are late in file order get through (runs counted 600 to 1,287 fewer late events than the reference, likely among the 5,506 late index rows of 10 Nov, and wrote up to 425 extra windows). In live runs the producer paces all partitions, so they stay together. Three runs without alignment gave two different results; every aligned run gave exactly the reference results, and alignment cost no measurable time ([`MEASUREMENTS.md`](../MEASUREMENTS.md) §5). Any drift below the 17.9 h of the late rows would give the same results for this dataset.
- Events later than D plus allowed lateness go to a **side output** and are counted. The rate of late events is reported in the evaluation. *Implemented: late events are stored in `data_issue` (reason `LATE`). Flink full week with every D tested (0 to 60 s): exactly the 5,506 re-sent index rows of 10 Nov were late.*
- **The last windows of an input need later events.** A window fires when the watermark passes its end, so when the input stops, windows ending after the last event time − D stay open (7 windows after a run of 8 Nov only with D = 1 s, 14 with 60 s). In the full week the next day's rows close them, and the weekend's zero prices close Friday's.
- **Every event with a valid event time advances the watermark, including rejected ones** (e.g. zero prices). The weekend files contain only zero prices; without them the watermark would stop at Friday evening and Friday's last windows would never be evaluated. Events with a malformed or implausible time (more than a day from the row's `Date`) get no timestamp, so a corrupt date cannot push the watermark ahead.
- **Choice of D: 1 s** ([`MEASUREMENTS.md`](../MEASUREMENTS.md) §8). In real time, an advisory reached the trader 60.7 s (p50) after its window's end with D = 60 s, 5.6 s with 5 s and 1.6 s with 1 s, and the number of late events is the same for every D. So on this data a larger D only costs latency.
  - D = 0 also gives identical results, but leaves no tolerance: an event that arrives in the wrong order across a window boundary would be lost. A feed with more disorder needs a larger D (a setting, no code change).
  - A partition that is silent for `IDLENESS_S` (10 s) no longer holds the watermark back. Events that it then delivers can be late. With D = 60 s, part of such a stall was covered by the bound; with 1 s it is not. Not seen in any run, but a risk for a feed that stalls per partition.
  - The delay above D was about 0.5–0.6 s at 1 s. It includes Flink's 200 ms watermark interval; the rest is not explained (the source's `fetch.min.bytes` of 64 KiB with Kafka's default 500 ms wait is a possible cause, not tested).

### 3.5 EMA state and crossover detection (FR-10 to FR-18)
Per-symbol keyed state:
```
ema38, ema100        (double, initial 0.0)
prevEma38, prevEma100
```
On each window result for symbol `s` with close `C`:
```
prevEma38 = ema38; prevEma100 = ema100
ema38  = C · 2/39  + ema38  · (1 − 2/39)
ema100 = C · 2/101 + ema100 · (1 − 2/101)
if ema38 > ema100 and prevEma38 <= prevEma100 → BUY
if ema38 < ema100 and prevEma38 >= prevEma100 → SELL
```
- **Empty windows (decision):** if a symbol has no price event in a window, its EMAs are not updated and no crossover is evaluated. To be confirmed; see requirements §7.
- **Zero prices (decision: excluded).** Every symbol gets a `Last = 0` reset record at `Trading time 00:00` each day (316,382 rows in the week, plus some at Xetra's 09:00 and 17:30). They are not trades, so the Flink job drops them and counts them (log line `quality … zero_price=`). Taken literally (FR-9), they would close 46,733 windows at 0; each one drags EMA38 below EMA100 and causes a SELL at 00:05 and a BUY the next morning (25,834 advisories in the week instead of 11,349). The reference can still compute the literal variant (`ema_reference.py` without `--exclude-zero-prices`).
- **Initialization artifact:** the first positive close of a symbol always gives BUY, since both previous EMAs are 0. Such advisories are flagged `initial` (4,970 in the week, one per symbol).
- *Implemented in `EmaState` (plain Java, unit-tested) inside `EmaFunction`. Windows of one key arrive in window order; an older or repeated window would be refused and reported (`WINDOW_OUT_OF_ORDER`, none seen). Full week: 1,855,645 windows for 4,970 symbols, 7,769 BUY (4,970 initial) and 3,580 SELL. SQL checks on the output: every row follows FR-11 from its previous row, starts at 0 (FR-12), and has an advisory exactly when FR-15/16 hold. Compared with the pandas reference ([`compare.py`](../reference/compare.py)): all windows, closes, EMAs, advisories and `initial` marks are identical bit for bit, in a backlog and in a live run.*
- The state survives day boundaries and is included in checkpoints.

### 3.6 Subscriptions (FR-3, FR-4)
- EMAs and advisories are computed for **all** symbols (and all are stored in `ema_window`). Only advisory delivery to the Kafka topic `advisories` is filtered.
- **Per-trader lists in a config file (decision).** [`config/subscriptions.txt`](../config/subscriptions.txt): one line per simulated trader, `trader: SYMBOL SYMBOL, ...`, or `trader: *` for every symbol. The job reads it once at start (`SUBSCRIPTIONS_FILE`, mounted read-only into the JobManager) and ships a symbol → traders map with the job graph. Each advisory is sent once per subscribed trader, with a `trader` field; a symbol nobody subscribes to sends nothing. Malformed lines stop the job with the file and line number. Changing subscriptions = edit the file and restart the job. *Caveat: recreating the JobManager starts a new job; checkpoints are not retained and there is no HA, so the EMA state starts again from 0 while the Kafka source resumes from the last offsets committed at a checkpoint. To keep the state, stop the job with a savepoint and start the new one from it (not yet scripted).*
- Why not a `subscriptions` topic with broadcast state (the earlier plan): the course project and the evaluation criteria ask for updates only for a subscribed subset and for "more subscribers" as a scalability dimension, but nothing asks for runtime changes. With the Iggy connector it would also have cost a busy-polling core and lost changes on redeploy ([`iggy.md`](iggy.md)); with Kafka a compacted topic read from the beginning would work, and can still be added later on top of the file.
- *Verified on the full week with the default file (5 traders, 17 subscriptions, 14 symbols): of 11,349 advisories, 61 were for subscribed symbols and became exactly the 73 expected messages (overlapping traders), with identical fields and no duplicates. Repeated on 2026-11-23 for (trader, symbol, window, type) against the reference advisories: 73 of 73, none missing or unexpected, backlog and live.*
- Scalability experiments: vary the number of traders and symbols per trader (generated files, `SUBSCRIPTIONS_FILE`), with `bench/subscriber_sweep.py`, which also checks the number of deliveries. *Run with 1 to 1,000 traders on 24 Nov 2026: every delivery correct, results and limits in [`MEASUREMENTS.md`](../MEASUREMENTS.md) §11.*

### 3.7 Output records
| Record | Fields |
|---|---|
| EMA result (`ema_window`) | symbol, sec_type, window_start, window_end, close, close_time, ticks, ema38, ema100, prev_ema38, prev_ema100, advisory (BUY/SELL/null), initial, emitted_at. *Planned: ingestion_ts of the closing event, once the producer stamps it.* |
| Advisory (view `advisories`; Kafka topic `advisories` as JSON, plus `trader` in the message; key = symbol) | symbol, type (BUY/SELL), window_start, window_end, due_at (= window_end, FR-8), close, ema38, ema100, prev_ema38, prev_ema100, initial, emitted_at. In the JSON message, market times are local exchange time without a zone (`"2021-11-10T09:35:00"`) and `emitted_at` is a UTC instant. |
| Data issue (`data_issue`) | reason, symbol, event_time, price, kafka_partition, kafka_offset, detail |

### 3.8 Data quality report
Suspicious input is reported, not silently dropped. Each issue goes to `data_issue` (keyed by the event's Kafka partition and offset, so replays add nothing); counts per subtask are logged (`scalable.report`). Full week:

| Reason | Handling | Week | What it was |
|---|---|---|---|
| `ZERO_PRICE` | dropped, counted only | 316,382 | Daily `Last = 0` reset records, one per symbol at 00:00, plus some at Xetra's 09:00 / 17:30 |
| `LATE` | dropped | 5,506 | 1,812 Paris indices (e.g. CAC 40) re-sending open / high / low at 18:56 on 10 Nov with `Trading time 01:00` |
| `TIME_SKEW` (> 1 h) | kept | 6,681 | The same 5,506 rows (18 h before system time), plus 1,175 Xetra closing-auction trades at 17:30 whose system time is 1–8.5 h earlier |
| `PRICE_JUMP` (> 10% between closes) | kept | 638 | 138 symbols, mostly illiquid Paris equities alternating between two prices (`ALNEV.FR` 146×, `CGGBS.FR` 0.0062 ↔ 0.0108); index `IECRP.FR` −55% at Monday's open |
| `MALFORMED`, `NEGATIVE_PRICE`, `IMPLAUSIBLE_TIME`, `TRADING_DATE_MISMATCH`, `WINDOW_OUT_OF_ORDER` | dropped / kept | 0 | — |

Thresholds: `SKEW_REPORT_S` (3600), `PRICE_JUMP_REPORT` (0.10).

---

## 4. Scalability

### 4.1 Parallelism model
- **Partition by symbol** in both Kafka and Flink: no cross-symbol coordination is needed, so the work is embarrassingly parallel.
- **Source reading scales out.** The Kafka source spreads the partitions of `ticks` over all source subtasks: with 8 partitions and parallelism 4, each reader has 2. In the live full-week run all 4 readers read about 250k rows/s each and were busy only ~24% of the time (with Iggy, one reader did everything and was busy 95%; [`iggy.md`](iggy.md) §5).
- **Where the limit is now** (parallelism 4, [`MEASUREMENTS.md`](../MEASUREMENTS.md) §7, §12). Live, the producer (5.4 cores): the job keeps up, with at most 8,397 records of `pendingRecords` lag (which leaves out records already fetched). From a stored backlog, the database was the limit until its writer sent multi-row upserts (busy 78–92%, EMA task backpressured 56–73%); now it is busy 36–45% and no task is saturated. Without the database the week is read at most about 8% faster, close to the difference between runs.
- **The single database** stores about **50,000 rows/s** with 4 writers. Its load is rows (one per symbol with a price per 5-min window), not events: the real feed produces about 12 rows/s, and each close of 3,100–3,700 rows is stored within 1.2 s of 300 s at real-time pace. EMA results begin to wait between 10,000 and 20,000 rows per close. It limits throughput when the job would produce about 50,000 rows/s: about 0.6M events/s with 40,000 equally frequent symbols, about 8M (extrapolated, above what the job reads) for the real mix.
- **Effect on latency.** At 22k–514k events/s advisories arrive 10–41 ms (p50, at most 105 ms) after their window fired. A saturated backlog run took 336–763 ms (p50) before the multi-row upserts and 27–76 ms with them (max 0.4–0.9 s), nearly all of it before the EMA result: the writer's full buffers block the EMA task and windows queue in front of it (without the database the EMA task is not backpressured, §12). The feed's real peak is 4,593 events/s (10 Nov 15:30). For far more symbols: several writers or databases, or EMA results via Kafka (not tested).
- **Scaling with resources.** Measured once per configuration before the sink got its own task: 347 s at parallelism 1, 153 s at 4, 136 s at 8 (2.6×), limited beyond 4 by TimescaleDB and the shared host (§5 there; before the multi-row upserts, so not valid for the current writer). To be repeated with `bench/scaling_matrix.py`. Skew of the keyed work alone does not explain the flattening: the real hash assignment would allow about 7× at parallelism 8.
- The partition count of `ticks` (8) is the upper bound on source parallelism. Set it at least as high as the largest number of slots to test; it cannot be lowered later without recreating the topic. Operators after `keyBy(symbol)` can use more parallelism than the source.
- **Long-tail skew.** Hot symbols are spread across partitions and subtasks by hashing; Flink's 128 key groups give fine-grained rebalancing when rescaling. The skew shrinks along the pipeline, because only price events are keyed and windows count each symbol once per 5 min (`bench/symbol_distribution.py`, whole week):

  | | All rows (producer, Kafka, source) | Price events (window operator) | Windows (EMA, database) |
  |---|---|---|---|
  | Share of the top 1% of the symbols | 47.3% | 13.5% | 2.2% |
  | Gini coefficient | 0.86 | 0.68 | 0.41 |
  | Most loaded / average, 8 partitions; 4 subtasks (measured) | 1.33 (partitions), 1.05 (readers) | 1.10 | 1.03 |

  The measured balance of the partitions, price events and windows equals the one predicted from the hashes. The least even stage is reading Kafka, and more partitions make it worse (1.49 at 16, 2.02 at 32). A single symbol cannot be split, but the hottest reaches only 22 price events/s.

### 4.2 Scaling in Docker Compose
Docker Compose has no built-in autoscaling. The plan in increasing ambition:

1. **Manual scale-out with Flink Reactive Mode.** The job runs in standalone application mode with `scheduler-mode: reactive`. Running `docker compose up --scale taskmanager=k` adds or removes TaskManagers. Flink automatically rescales the job to use all available slots, restoring from the latest checkpoint. *Not yet tested. The Kafka source re-assigns partitions to the new set of readers from the checkpointed offsets; up to 8 source subtasks get partitions.*
2. **Custom autoscaler (stretch goal).** A small controller service polls the Flink REST API and Prometheus (busy time, backpressure, Kafka consumer lag). It calls `docker compose up --scale taskmanager=k` (via the Docker socket) when thresholds are crossed. Combined with Reactive Mode, this gives automatic scaling within one host. It is also a good "creative exploration" item.

Limits: scaling is bounded by a single host's resources. The Kafka setup is a single broker, so it scales vertically only (more brokers would need replication settings). Both limits are documented in the report.

### 4.3 Fault tolerance
- Flink checkpoints every 30 s (configurable) to a shared Docker volume. *Configured: `execution.checkpointing.storage: filesystem` with `execution.checkpointing.dir: file:///flink-checkpoints` on the `flink-checkpoints` volume (10 s interval for experiments). The earlier in-memory default is capped at 5 MB, and growing window state exceeded it.*
- **Exactly-once processing of the input (measured).** The Kafka source stores its offsets in the checkpoint; after a TaskManager kill the job restores state and offsets together and re-reads from the checkpoint. In two failover runs (one trading day) the result was identical to a run without failure: 0 missing, 0 changed rows; the 62k–86k windows emitted again were absorbed by the upsert ([`MEASUREMENTS.md`](../MEASUREMENTS.md) §4). *With the Iggy connector the same test lost 1.0–1.5% of the ticks (at-most-once), and 1 of 7 failovers stalled ([`iggy.md`](iggy.md)).*
- Offsets are also committed to the Kafka consumer group when a checkpoint completes. They are used only by a new job (no checkpoint to restore) and for monitoring the lag (`kafka-consumer-groups.sh --describe --group flink`).
- Advisories to Kafka are at-least-once (§1.1): after a restore, advisories after the checkpoint are sent again.
- Output sinks are idempotent (upsert on symbol + window) to tolerate replays.

---

## 5. Evaluation and Observability

| Aspect | Method |
|---|---|
| Correctness | `reference/compare.py`: table `ema_window` against the pandas reference, bit for bit (windows, ticks, close, EMAs, advisories). Every benchmark run also prints a digest of its results; the reference's is `19413a50…`, so each run with it is verified. |
| Throughput | Load the week into Kafka once, then replay the stored backlog into the job (`bench/backlog.py`), so the producer is not part of the measurement: full-week time, read rate, busy time per task. Live runs (producer at max rate) with the Kafka consumer lag (`pendingRecords`; the Kafka client's `records_lag_max` stays at old values) show whether the job keeps up. |
| Latency | From the end of a window to a trader's system receiving the advisory (`bench/advisory_latency.py`, run by `bench/backlog.py run` and `scripts/run-live.sh`): `fired_at` (wall clock when the window fired, i.e. the watermark passed its end) → `emitted_at` (EMA function) → Kafka append time (topic `advisories` uses `LogAppendTime`) → receive time in a consumer (`advisory-latency` service). p50, p95, p99, max per stage. Not yet covered: the time from an event's ingestion to the window firing (needs the planned ingestion timestamp, §3.7). Load/latency curve: `bench/latency_sweep.py`. A saturated backlog run measures queueing, so latency is reported from the sweep. Results: [`MEASUREMENTS.md`](../MEASUREMENTS.md) §7; the experiments that moved the `ema_window` sink into its own task: §6 there. |
| Scalability | Vary the TaskManager count (1, 2, 4, …) and the Kafka partition count. Scripts: `bench/scaling_matrix.py` (resources, repeated runs), `bench/peak_test.py` (busiest minute overall and per exchange, at several speed-ups), `bench/subscriber_sweep.py` (1 to 1,000 traders). *Written, not run yet.* |
| Resource use | CPU and memory per service (cAdvisor), TaskManager heap, Flink busy and backpressured time from the accumulated counters (the per-second gauges are moving averages and start at 1000). |
| Long tail | `bench/symbol_distribution.py` (offline: concentration, balance over partitions and subtasks); each benchmark run records rows per partition, records and busy time per subtask (max/mean). Comparison with evenly spread symbols: `--workload uniform`, *not run yet*. |

Throughput, lag, resource use and per-subtask load are also visible in Grafana dashboards; advisory latency, the digest and the comparison are only printed by the scripts. Raw results are exported for the report.

### 5.1 Grafana dashboards
Provisioned from [`config/grafana/dashboards/`](../config/grafana/dashboards/) (JSON; edits in the UI last until Grafana restarts, so export the JSON to keep them). Market times are local exchange time stored as UTC (§3.3), so the market dashboards use the UTC time zone and default to the dataset's week.

| Dashboard | Requirements | Contents |
|---|---|---|
| **Trading trends** (home) | FR-14 to FR-19 | Filters: exchange, equity/index, hide or show `initial` advisories, symbol. Market overview: BUY/SELL counts, advisories per hour (BUY up, SELL down, so market-wide moves stand out), the 15 symbols with most crossovers, latest advisories (click a symbol to open it). Per symbol: close, EMA38 and EMA100 with BUY/SELL markers and the EMA warm-up shaded (88 / 231 windows, as the reference plots); the spread (EMA38 − EMA100) / EMA100, where each zero crossing is an advisory; price events per window; the symbol's advisories. |
| **Correctness and data quality** | NFR-7 to NFR-9 | Output counts; SQL checks on every stored row: FR-11 formula, start at 0 and carried state (FR-12/13), advisory if and only if a crossover (FR-15/16), window bounds (FR-5 to FR-7), no zero closes, `initial` flag. Each must be 0 (all 0 on the full week; about 5 s). Data issues by reason, per hour, the symbols with most price jumps, and the latest issues. |
| **System metrics** | NFR-2, NFR-5, NFR-10 | Records/s at source, after the quality gate, into the EMA function and as advisories; records per subtask (load balance under skew); busy and backpressured time per task and subtask (accumulated counters, over 30 s); watermark progress (replay speed); checkpoints; JVM heap; CPU and memory per container; results written per second (from `emitted_at`). Live, 10 s refresh. |

Not covered yet: per-trader delivery (subscriptions are in a file, not in the database), the comparison with the pandas reference (offline), and advisory latency (benchmark scripts only). Latency from an event's arrival to its window firing is not measured at all.

Source throughput baselines with Iggy (single reader, poll batch sizes, duplicates and gaps) are in [`iggy.md`](iggy.md) §4.

---

## 6. Repository Layout
```
docker-compose.yml   kafka, kafka-setup, timescaledb, db-setup, jobmanager, taskmanager ×4, replay-producer,
                     advisory-latency,
                     grafana, prometheus, cadvisor
MEASUREMENTS.md      measurements with Kafka (producer, full week, bottleneck, failover, scaling, latency; current results: §7)
producer-rs/         replay producer + kafka-setup + advisory latency consumer (Rust, rdkafka, Cargo, Dockerfile)
flink-job/           Flink application (ema/: EMA, advisories, data quality; unit tests in src/test)
bench/               benchmark scripts (backlog.py, scaling_matrix.py, latency_sweep.py, peak_test.py, subscriber_sweep.py,
                     advisory_latency.py), their helpers (workload.py, measure.py) and the data set analysis
                     symbol_distribution.py; raw results in results/
docs/                requirements, architecture, report; iggy.md (the earlier Iggy setup: tests, measurements,
                     why it was replaced); figures/reference/ (reference result plots)
reference/           pandas reference implementation, comparison with the job's results (compare.py), unit tests, plots
config/              subscriptions.txt (per-trader advisory subscriptions), timescaledb/init.sql (schema),
                     grafana/ (data sources, dashboards), prometheus/prometheus.yml
data/                CSV files (git-ignored)

planned:
autoscaler/          (stretch) custom scaling controller
```

---

## 7. Risks
| Risk | Mitigation |
|---|---|
| Message broker connector limits results or scaling | *Happened with Iggy* (one active reader, at-most-once, one watermark per reader; [`iggy.md`](iggy.md)), so the broker was replaced with Kafka on 2026-11-20. The Kafka connector has no Flink 2.3 build yet; 5.0.0-2.2 is tested on 2.3 ([`MEASUREMENTS.md`](../MEASUREMENTS.md)). |
| Producer becomes the bottleneck | *It is now (full week):* librdkafka reads the clock per record, a ~1.4 µs system call on this laptop (HPET). Mitigated with 8 sender threads (1.41M msg/s alone). Further options: a host with a TSC clock source, or several rows per Kafka record. |
| TimescaleDB writer limits throughput and, when saturated, delays advisories | *Measured (§4.1):* not with the real mix since the multi-row upserts (up to 1.15M events/s no backpressure). It is the limit from about 50,000 rows/s or more than about 10,000 symbols per close. Options, not tested: more writers, larger batches (`JDBC_BATCH_SIZE`), EMA results via Kafka. |
| Readers without data stall the event-time watermark | `withIdleness`, plus monitoring of watermark lag. *Confirmed with Iggy (no window ever fired without idleness); with Kafka every reader has partitions, but idleness is still needed when the input stops.* |
| Single-host limits hide scaling effects | Use a larger VM for evaluation runs, and report the results relative to the host's cores. |
| Tight schedule (deadline 27.11.2026) | Implement in order: correctness → metrics → visualization → scaling. Each step is independently demonstrable. |
