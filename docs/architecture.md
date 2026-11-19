# Architecture: Detecting Trading Trends in Financial Tick Data

Companion to [`requirements.md`](requirements.md). Requirement IDs (FR-x, NFR-x) refer to that document.

Last updated 2026-11-19, after the pandas reference implementation. Findings: [`README.md`](../README.md),
[`reference/README.md`](../reference/README.md).
Throughput numbers: [`MEASUREMENTS.md`](../MEASUREMENTS.md).

## 1. Technology Decisions

### 1.1 Decided
| Concern | Choice | Rationale |
|---|---|---|
| Message streaming | **Apache Iggy** | Lightweight, high-throughput persistent streaming server written in Rust. Partitioned topics and consumer groups. Decouples data replay from processing, so input rates can be controlled for load tests. |
| Stream processing | **Apache Flink** | Event-time tumbling windows, watermarks for out-of-order data, keyed state for the EMAs, exactly-once checkpointing, and rescaling of parallelism. |
| Deployment | **Docker Compose** | Single-command, reproducible local or single-VM setup. Flink TaskManagers can be scaled with `docker compose up --scale`. |
| Versions | **Iggy 0.10.0** (server and Java SDK), **Flink 2.3.0** | The Iggy Flink connector 0.10.0 is built against Flink 2.3.0, so the versions are pinned together. |
| Language | **Java 17** for the Flink job. Replay producer and `iggy-setup` in **Rust** (`producer-rs/`) | The Rust producer was faster than the earlier Java one: 1.92M vs 1.39–1.42M msg/s for one trading day. Only one implementation is kept (the Java producer was removed 2026-11-19), so setup and replay changes are made once. The job limits end-to-end throughput at ~830k msg/s anyway. The Rust SDK reads the clock once per message, which is slow on hosts whose clock source is HPET, so the producer timestamps once per batch ([`MEASUREMENTS.md`](../MEASUREMENTS.md)). |
| Iggy → Flink integration | **Official connector `org.apache.iggy:flink-connector:0.10.0`**, with checkpointing always on and poll batch ≥ 1000 | Works with several TaskManagers and delivers every message without loss or duplicates in normal operation. Limits: one active reader; one watermark per reader, so the topic needs 1 partition (§3.4); at-most-once, which lost 1.0–1.5% of a day's price events per TaskManager failure; no recovery without checkpointing; and 1 of 7 failovers stalled (see §4.1, §4.3, §7). A **custom Flink source** remains the fallback if exactly-once is required. |
| Result storage | **TimescaleDB 2.31 (PostgreSQL 17)**, written by the Flink JDBC sink (`flink-connector-jdbc-core:4.1.0-2.2` + PostgreSQL driver 42.7.12) with `INSERT … ON CONFLICT (symbol, window_end) DO UPDATE` | Exact, idempotent upserts on a real primary key, so replays after a restart are harmless. Plain SQL for the pandas correctness comparison, and a built-in Grafana data source. Tested on Flink 2.3 (no build for 2.3 exists yet): identical results across runs, idempotent re-runs, and re-emitted windows absorbed after failover ([`MEASUREMENTS.md`](../MEASUREMENTS.md)). Rejected: InfluxDB (no maintained Flink connector, awkward for relational checks) and ClickHouse (upserts only eventually deduplicated; its scale is not needed: at most ~1.6M rows/day). |
| Reference implementation | **Python + pandas batch script** ([`reference/`](../reference/README.md)), pyarrow for reading the CSV files, matplotlib for figures | Written from the requirements alone, not from the Flink code, so it can find errors in it (NFR-9). Whole week in ~2.5 min on 4 cores. EMAs evaluated in the same order as FR-11, so values can be compared exactly. Checked against unit tests and an independent `pandas.ewm` computation (identical). It models the stream's late-event rule (watermark = max event time − D), so its advisories are what a correct stream must emit. |
| Flink state backend | **HashMap (heap)**, `state.backend.type: hashmap` in `docker-compose.yml`, with file-system checkpoint storage on the shared volume | State is small: per symbol a few doubles (EMAs, previous EMAs) plus the open 5-min window, for about 5,500 symbols, so it fits easily on the TaskManager heap, and heap access avoids RocksDB's serialization on every read and write. Explicit even though it is Flink 2.3's default, so the choice is visible and easy to switch. Verified: the job reports `HashMapStateBackend` with `FileSystemCheckpointStorage` (REST `/jobs/<id>/checkpoints/config`), and checkpoints complete. Rejected: RocksDB (needed only when state exceeds memory, or for incremental checkpoints of large state). |
| Visualization and metrics | **Grafana 12.2** (provisioned from `config/grafana/`) over TimescaleDB and **Prometheus 3.7**, which scrapes the Flink Prometheus reporter (JobManager and every TaskManager, port 9249) and **cAdvisor 0.52** (CPU and memory per container). Three dashboards (§5.1) | Quick to build, no code, and the results are already in SQL. Grafana shows the market data at its stored local time by fixing the dashboard time zone to UTC. Limits: no push to the browser (refresh polling), and cross-symbol views are SQL over 1.9M rows, so heavy panels take seconds. Iggy is not scraped: its `/metrics` needs a login token in 0.10. cAdvisor reads containers through containerd, since Docker here uses the containerd image store, which its Docker handler cannot read; Compose labels are not visible there, so every service sets `SERVICE=<name>`. Network counters are not reported this way. | Custom web app (§1.2) |
| Advisory delivery | **Iggy topic `advisories`** (1 partition, created by `iggy-setup`), written by the connector's `IggySink` with a hand-written JSON serializer: one message per subscribed trader and advisory (per-trader lists in `config/subscriptions.txt`, §3.6), batch size 1 (`IGGY_ADVISORY_BATCH`) | Advisories must be emitted immediately (FR-15/16, NFR-3). The sink sends a batch only when it is full or at a checkpoint (its 5 s time flush is checked only on the next write), so a larger batch could hold an advisory for up to a checkpoint interval; advisories are few (~11k per week), so sending each one is cheap. One partition because the sink picks one partition per batch (round-robin, or from the first message's key), not per message. Delivery is at-least-once (flush at checkpoints); (symbol, window_end) identifies an advisory, so consumers can drop repeats. Verified on the full week: the topic holds exactly the 11,349 advisories of `ema_window`, no duplicates, identical fields, each symbol in window order; `emitted_at` → stored in Iggy p50 0.8 ms, p99 1.7 ms. | Custom Iggy sink that partitions per message by symbol, if consumers ever need several partitions |

### 1.2 Proposed (not yet decided)
| Concern | Proposal | Alternatives |
|---|---|---|
| Visualization (future) | Custom web app with WebSocket push, which may replace the Grafana trading dashboard (§5.1) | — |

---

## 2. System Overview

```
                ┌──────────────────────────── docker compose ─────────────────────────────┐
                │                                                                         │
 CSV files ──►  │  replay-producer ──► Iggy server ──────────► Flink JobManager           │
 (read-only     │  (rate control,     stream: trading          │                          │
  volume)       │   event typing)     topic:  ticks (N part.)   ▼                          │
                │                     topic:  advisories ◄── Flink TaskManager × k         │
                │                     topic:  subscriptions ─►  (scalable)                 │
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
| `replay-producer` | Reads the daily CSV files in order and skips the `#` comment lines and the header. Drops irrelevant events early, or forwards everything (configurable, see §3.1). Picks the partition **from the symbol ID hash** and sends one batch per partition (4000 rows per flush). Supports a speed-up factor (1×, 10×, …, max) and stamps an ingestion timestamp for latency measurement. *Implemented: partitioning, batching, `RATE` (msg/s) and `REPEAT`. Not yet implemented: event filtering, event-time replay speed-up, ingestion timestamp.* |
| `iggy-setup` | One-shot job (same image as the producer). Creates the stream, the input topic `ticks` with the Flink consumer group, and the output topic `advisories`, idempotently: existing topics are kept. The connector does not create the consumer group itself. *`subscriptions` not created yet.* |
| `iggy` | Stream `trading`, with topics `ticks` (input, N partitions, **1 by default** while using the connector, see §3.4), `advisories` (output, 1 partition, see §1.2) and `subscriptions` (control). Iggy 0.10 needs explicit container settings: bind addresses `0.0.0.0` (the default is loopback), an explicit shard count (`IGGY_SHARDING_CPU_ALLOCATION`; the default NUMA-aware allocation fails inside Docker), and core pinning off. |
| `flink-jobmanager` | Runs the job in **standalone application mode**. |
| `flink-taskmanager` | Workers. Replicated with `--scale taskmanager=k`. |
| `timescaledb` | Stores EMA results and advisories (`ema_window`, view `advisories`) and the data quality report (`data_issue`, view `data_issue_summary`), for the visualization and correctness checks. Schema: [`config/timescaledb/init.sql`](../config/timescaledb/init.sql). All symbols are stored (FR-3), also for correctness checks; the subscription filter applies to advisory delivery only (§3.6). |
| `db-setup` | One-shot job: applies the idempotent schema, so new tables also reach an existing database volume (the image's init scripts run only on an empty volume). |
| `grafana` | Dashboards for the query results (smart visualization), correctness and system metrics (§5.1). http://localhost:3001 (port 3000 is Iggy's HTTP API), admin/admin, anonymous viewers allowed. |
| `prometheus`, `cadvisor` | Collect throughput, backpressure, checkpoint and resource metrics (Flink reporter, per-container CPU and memory). |

---

## 3. Data Flow and Processing

### 3.1 Ingestion (replay-producer)
1. Stream the CSV files one day at a time (Mon → Sun).
2. Parse each line and infer the event type from its non-NULL attributes. A **price event** is one with `Last` set. Other events are counted (for dataset analysis in the report) and optionally forwarded.
3. Build the event time from `Trading date` + `Trading time`.
4. Serialize to a compact binary format. Send in batches to Iggy topic `ticks`, partitioned by hash of `ID.[Exchange]`. This preserves per-symbol order within a partition. The partition is chosen in the producer, not with Iggy's message-key partitioning: a message key applies to a whole send call, so key-based sends would allow only one symbol per batch. *Currently the messages are CSV text, and binary serialization is still to do. Batching per partition raised the producer from 3.3k to 1.5M msg/s.*

Filtering non-price events in the producer reduces load substantially: about 59 M price events out of 289 M total. The filter can be switched to the Flink side to compare both designs.

### 3.2 Flink job topology
```
IggySource (official connector, one split per partition; in practice one active reader, see §4.1;
           topic `ticks` must have 1 partition for correct event time, see §3.4)
  → parse (price rows only: `Last` set; byte-level, non-price rows create no objects)
  → assign timestamps + watermarks (bounded out-of-orderness)
  → quality gate ── dropped / suspicious events → data_issue (§3.8)
  → keyBy(symbol)
  → TumblingEventTimeWindows(5 min)  ── aggregate: last price by event time (Close), tick count
  │                                     late events → data_issue
  → KeyedProcessFunction (EMA38, EMA100, crossover detection; keyed state) ── price jumps → data_issue
  ├─► JDBC sink → TimescaleDB (EMA per window incl. advisories, all symbols; data issues)
  └─► advisories only → subscription filter: one copy per subscribed trader (§3.6)
        → Iggy sink → topic `advisories` (JSON, one message per trader and advisory)
```
*Implemented as `PIPELINE=ema` ([`flink-job/src/main/java/scalable/flink/ema/`](../flink-job/src/main/java/scalable/flink/ema/), the default in `docker-compose.yml`). Full week (289 M rows, 59.4 M usable price events) in about 6 min on one laptop, with the producer running at the same time.*

### 3.3 Windowing (FR-5 to FR-8)
- 5-minute **event-time tumbling windows**.
- Timestamps in the data are local exchange time without a zone. They are parsed as if they were UTC, so Flink's epoch-aligned windows start exactly at **00:00 local** (FR-6) without any offset handling.
- A window fires when the watermark passes its end. This happens after the next window has started (FR-8).
- The window function keeps only the **last price by event time** (not by arrival order), so out-of-order events within a window are handled correctly.

### 3.4 Watermarks and late events (NFR-7)
- Strategy: `forBoundedOutOfOrderness(D)`, with D configurable (initial value of a few seconds; to be tuned from a dataset analysis of the actual disorder).
- **Measured disorder (reference implementation).** In file order, price events are in event-time order per symbol, with one exception: 5,506 rows at the end of 10 Nov re-send the open, day high and day low of 1,812 Paris indices with `Trading time 01:00`, 17.9 h late. Against one global watermark, 189 Xetra `Last = 0` records (`Trading time 00:00`, at the end of each day's file) are late too. Any D from 0 s to 17.9 h therefore gives identical results. A stream must drop these rows: kept, they become the close of a 01:00 window and change the EMAs of 1,812 symbols and about 1,790 advisories. D can be chosen for latency alone.
- `withIdleness` so that idle partitions (nights, weekend) do not stall the watermark. *Required in any case with the connector: the second source reader never gets a split, and without idleness the watermark stays at `Long.MIN_VALUE` and no window ever fires (measured).*
- **Use a 1-partition topic with the connector.** It has one watermark per reader, not per partition. With 4 partitions, 54% of the price events arrived late (60 s bound) and half the windows were lost; with 1 partition, 37 events were late (41 in file order). One partition costs nothing, since only one reader is active anyway. A custom source with per-split watermarks would remove this limit.
- Events later than D plus allowed lateness go to a **side output** and are counted. The rate of late events is reported in the evaluation. *Implemented: late events are stored in `data_issue` (reason `LATE`). Flink full week with D = 60 s: exactly the 5,506 re-sent index rows of 10 Nov were late.*
- **Every event with a valid event time advances the watermark, including rejected ones** (e.g. zero prices). The weekend files contain only zero prices; without them the watermark would stop at Friday evening and Friday's last windows would never be evaluated. Events with a malformed or implausible time (more than a day from the row's `Date`) get no timestamp, so a corrupt date cannot push the watermark ahead.
- The latency/correctness trade-off of D is a design exploration item: measure the late-event count against output latency for several values of D.

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
- *Implemented in `EmaState` (plain Java, unit-tested) inside `EmaFunction`. Windows of one key arrive in window order; an older or repeated window would be refused and reported (`WINDOW_OUT_OF_ORDER`, none seen). Full week: 1,855,645 windows for 4,970 symbols, 7,769 BUY (4,970 initial) and 3,580 SELL. SQL checks on the output: every row follows FR-11 from its previous row, starts at 0 (FR-12), and has an advisory exactly when FR-15/16 hold.*
- The state survives day boundaries and is included in checkpoints.

### 3.6 Subscriptions (FR-3, FR-4)
- EMAs and advisories are computed for **all** symbols (and all are stored in `ema_window`). Only advisory delivery to the Iggy topic `advisories` is filtered.
- **Per-trader lists in a config file (decision).** [`config/subscriptions.txt`](../config/subscriptions.txt): one line per simulated trader, `trader: SYMBOL SYMBOL, ...`, or `trader: *` for every symbol. The job reads it once at start (`SUBSCRIPTIONS_FILE`, mounted read-only into the JobManager) and ships a symbol → traders map with the job graph. Each advisory is sent once per subscribed trader, with a `trader` field; a symbol nobody subscribes to sends nothing. Malformed lines stop the job with the file and line number. Changing subscriptions = edit the file and restart the job. *Caveat: recreating the JobManager starts a new job; checkpoints are not retained and there is no HA, so the EMA state starts again from 0 while Iggy resumes after the last committed offset. To keep the state, stop the job with a savepoint and start the new one from it (not yet scripted).*
- Why not the Iggy `subscriptions` topic with broadcast state (the earlier plan): the course project and the evaluation criteria ask for updates only for a subscribed subset and for "more subscribers" as a scalability dimension, but nothing asks for runtime changes. With the connector, an idle subscriptions reader busy-polls about one core all the time (§4.1), which would also distort the CPU measurements, and a fresh deployment would skip earlier changes because offsets are committed on poll. A runtime topic can still be added later on top of the file.
- *Verified on the full week with the default file (5 traders, 17 subscriptions, 14 symbols): of 11,349 advisories, 61 were for subscribed symbols and became exactly the 73 expected messages (overlapping traders), with identical fields and no duplicates.*
- Scalability experiments: vary the number of traders and symbols per trader (generated files, `SUBSCRIPTIONS_FILE`).

### 3.7 Output records
| Record | Fields |
|---|---|
| EMA result (`ema_window`) | symbol, sec_type, window_start, window_end, close, close_time, ticks, ema38, ema100, prev_ema38, prev_ema100, advisory (BUY/SELL/null), initial, emitted_at. *Planned: ingestion_ts of the closing event, once the producer stamps it.* |
| Advisory (view `advisories`; Iggy topic `advisories` as JSON, plus `trader` in the message) | symbol, type (BUY/SELL), window_start, window_end, due_at (= window_end, FR-8), close, ema38, ema100, prev_ema38, prev_ema100, initial, emitted_at. In the JSON message, market times are local exchange time without a zone (`"2021-11-10T09:35:00"`) and `emitted_at` is a UTC instant. |
| Data issue (`data_issue`) | reason, symbol, event_time, price, iggy_partition, iggy_offset, detail |

### 3.8 Data quality report
Suspicious input is reported, not silently dropped. Each issue goes to `data_issue` (keyed by the event's Iggy position, so replays add nothing); counts per subtask are logged (`scalable.report`). Full week:

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
- **Partition by symbol** in both Iggy and Flink: no cross-symbol coordination is needed, so the work is embarrassingly parallel.
- **Source reading does not scale out with the connector.** Its split enumerator assigns splits only when a reader registers, so the first reader gets every partition and the other readers stay idle. An idle reader also busy-polls and uses about one core.
- **One reader is enough for this dataset.** On a laptop, one reader drained 1.21M msg/s (source only, poll batch 5000) and 830k msg/s with the full pipeline. One trading day (54M events) took about 65 s. Details: [`MEASUREMENTS.md`](../MEASUREMENTS.md). The connector's default poll batch of 100 gives only 265k msg/s, so set `IGGY_POLL_BATCH` to 1000 or more.
- **Parallelism therefore starts at `keyBy(symbol)`.** Scaling experiments vary the TaskManagers after the shuffle, not the readers. Partitioning by symbol in Iggy is kept for per-symbol order, and so that a custom source could read partitions in parallel later.
- Iggy partition count N is the upper bound on source parallelism if the source is ever replaced. Set it higher than the expected maximum number of TaskManager slots.
- Long-tail skew: hot symbols are spread across partitions and subtasks by hashing. Flink's key groups (max parallelism, e.g. 128) give fine-grained rebalancing when rescaling. A single symbol cannot be split, but the per-symbol rate is well within one core's capacity.

### 4.2 Scaling in Docker Compose
Docker Compose has no built-in autoscaling. The plan in increasing ambition:

1. **Manual scale-out with Flink Reactive Mode.** The job runs in standalone application mode with `scheduler-mode: reactive`. Running `docker compose up --scale taskmanager=k` adds or removes TaskManagers. Flink automatically rescales the job to use all available slots, restoring from the latest checkpoint. *Not yet tested with the connector. Rescaling hands splits back to the enumerator, the same code path that loses splits without checkpointing (§4.3).*
2. **Custom autoscaler (stretch goal).** A small controller service polls the Flink REST API and Prometheus (busy time, backpressure, Iggy consumer lag). It calls `docker compose up --scale taskmanager=k` (via the Docker socket) when thresholds are crossed. Combined with Reactive Mode, this gives automatic scaling within one host. It is also a good "creative exploration" item.

Limits: scaling is bounded by a single host's resources. The Iggy server is single-node, so it scales vertically only. Both limits are documented in the report.

### 4.3 Fault tolerance
- Flink checkpoints every 30 s (configurable) to a shared Docker volume. *Configured: `execution.checkpointing.storage: filesystem` with `execution.checkpointing.dir: file:///flink-checkpoints` on the `flink-checkpoints` volume (10 s interval for experiments). The earlier in-memory default is capped at 5 MB, and growing window state exceeded it.*
- **Checkpointing is required.** Without it, if a TaskManager is lost the job returns to RUNNING but reads nothing: the connector's `addSplitsBack` loses the splits. With checkpointing, the job recovers from the checkpoint and the splits move to the surviving reader (tested by killing a TaskManager).
- **Delivery is at-most-once, not exactly-once (measured).** The connector reads with Iggy consumer-group offsets (`PollingStrategy.next()`), commits them when a batch is fetched, and ignores the offsets in the Flink checkpoint. After a TaskManager kill, the job restored its window state from the checkpoint but resumed reading past it: 1.1–1.5% of a day's price events were lost (128k–180k ticks, 287–1,480 windows missing, about 5,600 windows short). Exactly-once needs a source that seeks to the checkpointed offsets (§7).
- **Failover is not always reliable.** 1 of 7 failovers left the job RUNNING but producing nothing (cause not found). Monitor that the consumer offset or output keeps moving after a restart.
- Output sinks are idempotent (upsert on symbol + window) to tolerate replays.

---

## 5. Evaluation and Observability

| Aspect | Method |
|---|---|
| Correctness | Run the pandas reference implementation on the same data. Compare EMA values (with a tolerance) and the exact set of advisories per symbol. |
| Throughput | Events/s at the source and sink (Flink metrics) with the producer at max rate. Report the full-week processing time. |
| Latency | `emit_ts − ingestion_ts` of the event that triggered the window. Report p50, p95 and p99 under several replay rates. |
| Scalability | Vary the TaskManager count (1, 2, 4, …) and the Iggy partition count. Vary the input: one exchange vs. all three, and replay speed-up. |
| Resource use | CPU and memory per container (cAdvisor), Flink busy time and backpressure. |
| Long tail | Per-subtask record counts and busy time, to show the load balance under skew. |

All of these metrics are visible in Grafana dashboards, except latency (needs the planned ingestion timestamp, §3.7). Raw results are exported for the report.

### 5.1 Grafana dashboards
Provisioned from [`config/grafana/dashboards/`](../config/grafana/dashboards/) (JSON; edits in the UI last until Grafana restarts, so export the JSON to keep them). Market times are local exchange time stored as UTC (§3.3), so the market dashboards use the UTC time zone and default to the dataset's week.

| Dashboard | Requirements | Contents |
|---|---|---|
| **Trading trends** (home) | FR-14 to FR-19 | Filters: exchange, equity/index, hide or show `initial` advisories, symbol. Market overview: BUY/SELL counts, advisories per hour (BUY up, SELL down, so market-wide moves stand out), the 15 symbols with most crossovers, latest advisories (click a symbol to open it). Per symbol: close, EMA38 and EMA100 with BUY/SELL markers and the EMA warm-up shaded (88 / 231 windows, as the reference plots); the spread (EMA38 − EMA100) / EMA100, where each zero crossing is an advisory; price events per window; the symbol's advisories. |
| **Correctness and data quality** | NFR-7 to NFR-9 | Output counts; SQL checks on every stored row: FR-11 formula, start at 0 and carried state (FR-12/13), advisory if and only if a crossover (FR-15/16), window bounds (FR-5 to FR-7), no zero closes, `initial` flag. Each must be 0 (all 0 on the full week; about 5 s). Data issues by reason, per hour, the symbols with most price jumps, and the latest issues. |
| **System metrics** | NFR-2, NFR-5, NFR-10 | Records/s at source, after the quality gate, into the EMA function and as advisories; records per subtask (load balance under skew); busy and backpressured time per task and subtask; watermark progress (replay speed); checkpoints; JVM heap; CPU and memory per container; results written per second (from `emitted_at`). Live, 10 s refresh. |

Not covered yet: per-trader delivery (subscriptions are in a file, not in the database), the comparison with the pandas reference (offline), and end-to-end latency.

Source throughput baseline (done): [`bench/source_throughput.py`](../bench/source_throughput.py) loads a backlog into Iggy, measures how fast the reader drains it at different poll batch sizes, and then runs producer and job together. A verifier operator with parallelism 1 checks for duplicates and gaps per partition. Results are in [`MEASUREMENTS.md`](../MEASUREMENTS.md).

---

## 6. Repository Layout
```
docker-compose.yml   iggy, iggy-setup, timescaledb, db-setup, jobmanager, taskmanager ×2, replay-producer,
                     grafana, prometheus, cadvisor
MEASUREMENTS.md      throughput measurements
producer-rs/         replay producer + iggy-setup (Rust, Cargo, Dockerfile)
flink-job/           Flink application: PIPELINE=ema (ema/: EMA, advisories, data quality; unit tests in src/test),
                     plus the connector / JDBC test pipelines (full, count, jdbc)
bench/               benchmark scripts (source_throughput.py)
docs/                requirements, architecture, report; figures/reference/ (reference result plots)
reference/           pandas reference implementation, unit tests, plots
config/              subscriptions.txt (per-trader advisory subscriptions), timescaledb/init.sql (schema),
                     grafana/ (data sources, dashboards), prometheus/prometheus.yml
data/                CSV files (git-ignored)

planned:
reference/           comparison script (Flink output in TimescaleDB vs reference)
autoscaler/          (stretch) custom scaling controller
```

---

## 7. Risks
| Risk | Mitigation |
|---|---|
| Iggy Flink connector is immature. *Confirmed: it is released (0.10.0), but has one active reader, at-most-once delivery, loses splits without checkpointing, and busy-polls when idle.* | Always enable checkpointing, and use poll batch ≥ 1000. Accept a single reader, since it is measured to be fast enough (§4.1). If exactly-once or rescaling is required, write a custom source with the Iggy Java SDK: explicit offsets kept in checkpoint state, `SourceReaderBase`, splits assigned to subtasks deterministically, and per-partition watermarks. Keep the source behind an interface so it can be swapped. |
| Producer or a single Iggy node becomes the bottleneck | *Not a bottleneck so far:* the producer sends 1.5M msg/s on one thread, and Iggy used 20–60% of one core. If needed later: binary serialization, several producer threads, or the Rust producer. |
| Readers without partitions stall the event-time watermark | *Confirmed:* without `withIdleness` the watermark stays at `Long.MIN_VALUE`, no window fires, and state grows until checkpoints fail. Always use `withIdleness`. |
| One watermark for several partitions makes events late | *Confirmed:* with 4 partitions, 54% of price events were late. Use a 1-partition topic while using the connector, or a custom source with per-split watermarks. |
| Watermark stalls on idle partitions | `withIdleness`, plus monitoring of watermark lag. |
| Single-host limits hide scaling effects | Use a larger VM for evaluation runs, and report the results relative to the host's cores. |
| Tight schedule (deadline 27.11.2026) | Implement in order: correctness → metrics → visualization → scaling. Each step is independently demonstrable. |
