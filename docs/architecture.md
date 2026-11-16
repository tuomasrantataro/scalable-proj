# Architecture: Detecting Trading Trends in Financial Tick Data

Companion to [`requirements.md`](requirements.md). Requirement IDs (FR-x, NFR-x) refer to that document.

Last updated 2026-11-16, after the Iggy → Flink connector experiment. Findings: [`README.md`](../README.md).
Throughput numbers: [`MEASUREMENTS.md`](../MEASUREMENTS.md).

## 1. Technology Decisions

### 1.1 Decided
| Concern | Choice | Rationale |
|---|---|---|
| Message streaming | **Apache Iggy** | Lightweight, high-throughput persistent streaming server written in Rust. Partitioned topics and consumer groups. Decouples data replay from processing, so input rates can be controlled for load tests. |
| Stream processing | **Apache Flink** | Event-time tumbling windows, watermarks for out-of-order data, keyed state for the EMAs, exactly-once checkpointing, and rescaling of parallelism. |
| Deployment | **Docker Compose** | Single-command, reproducible local or single-VM setup. Flink TaskManagers can be scaled with `docker compose up --scale`. |
| Versions | **Iggy 0.10.0** (server and Java SDK), **Flink 2.3.0** | The Iggy Flink connector 0.10.0 is built against Flink 2.3.0, so the versions are pinned together. |
| Language | **Java 17** for the Flink job. Replay producer in **Java or Rust** (drop-in replacements, same CLI and partitioning) | The Rust producer is faster: 1.92M vs 1.39–1.42M msg/s for one trading day. Either is fast enough, because the job limits end-to-end throughput at ~830k msg/s. The Rust SDK reads the clock once per message, which is slow on hosts whose clock source is HPET, so the Rust producer timestamps once per batch ([`MEASUREMENTS.md`](../MEASUREMENTS.md)). |
| Iggy → Flink integration | **Official connector `org.apache.iggy:flink-connector:0.10.0`**, with checkpointing always on and poll batch ≥ 1000 | Works with several TaskManagers and delivered every message without loss or duplicates in our tests. Limits: one active reader, at-most-once, no recovery without checkpointing (see §4.1, §4.3, §7). A **custom Flink source** remains the fallback if exactly-once is required. |

### 1.2 Proposed (not yet decided)
| Concern | Proposal | Alternatives |
|---|---|---|
| Result storage | TimescaleDB (PostgreSQL), written by the Flink JDBC sink | InfluxDB, ClickHouse |
| Advisory delivery | Iggy output topic `advisories` | — |
| Visualization | Grafana dashboards over TimescaleDB | Custom web app with WebSocket push |
| Metrics | Prometheus + Grafana (Flink Prometheus reporter, Iggy metrics endpoint, cAdvisor for container CPU and memory) | — |
| Reference implementation | Python + pandas batch script over the CSV files | — |
| Flink state backend | HashMap (heap) state backend. State is tiny: about 5,500 symbols with a few doubles each. | RocksDB |

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
| `iggy-setup` | One-shot job (same image as the producer). Creates the stream, the topics and the Flink consumer group. The connector does not create the consumer group itself. |
| `iggy` | Stream `trading`, with topics `ticks` (input, N partitions, currently 4), `advisories` (output) and `subscriptions` (control). Iggy 0.10 needs explicit container settings: bind addresses `0.0.0.0` (the default is loopback), an explicit shard count (`IGGY_SHARDING_CPU_ALLOCATION`; the default NUMA-aware allocation fails inside Docker), and core pinning off. |
| `flink-jobmanager` | Runs the job in **standalone application mode**. |
| `flink-taskmanager` | Workers. Replicated with `--scale taskmanager=k`. |
| `timescaledb` | Stores EMA results and advisories for subscribed symbols, for the visualization and correctness checks. |
| `grafana` | Dashboards for the query results (smart visualization) and system metrics. |
| `prometheus`, `cadvisor` | Collect throughput, latency, backpressure and resource metrics. |

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
IggySource (official connector, one split per partition; in practice one active reader, see §4.1)
  → parse / filter (price events only)
  → assign timestamps + watermarks (bounded out-of-orderness)
  → keyBy(symbol)
  → TumblingEventTimeWindows(5 min)  ── reduce: keep last price (Close)
  → KeyedProcessFunction (EMA38, EMA100, crossover detection; keyed state)
  → connect with broadcast(subscriptions)  ── filter to subscribed symbols
  ├─► JDBC sink → TimescaleDB (EMA per window, advisories)
  └─► Iggy sink → topic `advisories`
```

### 3.3 Windowing (FR-5 to FR-8)
- 5-minute **event-time tumbling windows**.
- Timestamps in the data are local exchange time without a zone. They are parsed as if they were UTC, so Flink's epoch-aligned windows start exactly at **00:00 local** (FR-6) without any offset handling.
- A window fires when the watermark passes its end. This happens after the next window has started (FR-8).
- The window function keeps only the **last price by event time** (not by arrival order), so out-of-order events within a window are handled correctly.

### 3.4 Watermarks and late events (NFR-7)
- Strategy: `forBoundedOutOfOrderness(D)`, with D configurable (initial value of a few seconds; to be tuned from a dataset analysis of the actual disorder).
- `withIdleness` so that idle partitions (nights, weekend) do not stall the watermark.
- Events later than D plus allowed lateness go to a **side output** and are counted. The rate of late events is reported in the evaluation.
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
- The state survives day boundaries and is included in checkpoints.

### 3.6 Subscriptions (FR-3, FR-4)
- EMAs are computed for **all** symbols. Only output for subscribed symbols is emitted.
- The initial subscription list is loaded from a config file. Changes are published to Iggy topic `subscriptions` and distributed to all Flink tasks as **broadcast state**.

### 3.7 Output records
| Record | Fields |
|---|---|
| EMA result | symbol, window_start, window_end, close, ema38, ema100, ingestion_ts (of the closing event), emit_ts |
| Advisory | symbol, window_end, type (BUY/SELL), ema38, ema100, emit_ts |

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
- Flink checkpoints every 30 s (configurable) to a shared Docker volume. *The experiment used 10 s, in-memory.*
- **Checkpointing is required.** Without it, if a TaskManager is lost the job returns to RUNNING but reads nothing: the connector's `addSplitsBack` loses the splits. With checkpointing, the job recovers from the checkpoint and the splits move to the surviving reader (tested by killing a TaskManager).
- **Delivery is at-most-once, not exactly-once.** The connector reads with Iggy consumer-group offsets (`PollingStrategy.next()`) and commits them when a batch is fetched. It does not read from the offsets stored in the Flink checkpoint. Records fetched but not processed before a crash would be skipped. This follows from the connector code; it was not reproduced, because the job was idle when the TaskManager was killed. Exactly-once needs a source that seeks to the checkpointed offsets (§7).
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

All of these metrics are visible in Grafana dashboards. Raw results are exported for the report.

Source throughput baseline (done): [`bench/source_throughput.py`](../bench/source_throughput.py) loads a backlog into Iggy, measures how fast the reader drains it at different poll batch sizes, and then runs producer and job together. A verifier operator with parallelism 1 checks for duplicates and gaps per partition. Results are in [`MEASUREMENTS.md`](../MEASUREMENTS.md).

---

## 6. Repository Layout
```
docker-compose.yml   iggy, iggy-setup, jobmanager, taskmanager ×2, replay-producer
MEASUREMENTS.md      throughput measurements
producer/            replay producer + iggy-setup (Java, Maven, Dockerfile)
flink-job/           Flink application (currently the connector test job: counters + verifier)
bench/               benchmark scripts (source_throughput.py)
docs/                requirements, architecture, report
data/                CSV files (git-ignored)

planned:
config/              subscriptions, Flink conf, Prometheus, Grafana provisioning
reference/           pandas reference implementation + comparison script
autoscaler/          (stretch) custom scaling controller
```

---

## 7. Risks
| Risk | Mitigation |
|---|---|
| Iggy Flink connector is immature. *Confirmed: it is released (0.10.0), but has one active reader, at-most-once delivery, loses splits without checkpointing, and busy-polls when idle.* | Always enable checkpointing, and use poll batch ≥ 1000. Accept a single reader, since it is measured to be fast enough (§4.1). If exactly-once or rescaling is required, write a custom source with the Iggy Java SDK: explicit offsets kept in checkpoint state, `SourceReaderBase`, splits assigned to subtasks deterministically, and per-partition watermarks. Keep the source behind an interface so it can be swapped. |
| Producer or a single Iggy node becomes the bottleneck | *Not a bottleneck so far:* the producer sends 1.5M msg/s on one thread, and Iggy used 20–60% of one core. If needed later: binary serialization, several producer threads, or the Rust producer. |
| Readers without partitions stall the event-time watermark | With the connector, the idle reader never gets a split. Use `withIdleness` in the watermark strategy, or the idle reader holds back the watermark and windows never fire. To be verified when windows are added. |
| Watermark stalls on idle partitions | `withIdleness`, plus monitoring of watermark lag. |
| Single-host limits hide scaling effects | Use a larger VM for evaluation runs, and report the results relative to the host's cores. |
| Tight schedule (deadline 27.11.2026) | Implement in order: correctness → metrics → visualization → scaling. Each step is independently demonstrable. |
