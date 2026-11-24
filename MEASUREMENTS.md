# Measurements

Measurements of the Kafka → Flink → TimescaleDB pipeline. Until 2026-11-20 the broker was Apache Iggy; those
measurements, the connector findings and the reasons for replacing it are in [`docs/iggy.md`](docs/iggy.md).

**The current results are in §7** (2026-11-23: full week, correctness, latency, skew, memory). §1–§6 are the earlier
measurements; where a later finding changes them, a note says so.

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

Kafka consumer lag: on average about 45k records per subtask (≈ 0.2 s of input), at most 116k. **Not reliable:** it
probably came from the metric that §7 found stuck at old values. The busy times and CPU are confirmed by the live run
in §7.

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

**Outdated in two ways:** these runs are from before the `ema_window` sink got its own task (§6), and the busy and
backpressured times come from a gauge that overstates them (§7). Times, digests and CPU are as measured, one run each.
To be repeated with `bench/scaling_matrix.py`.

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

## 6. Advisory latency: from window close to a trader's system

Date: 2026-11-22. Same host and setup as above.

### Method

Each advisory carries two wall-clock stamps from the job and gets two more on the way; all four come from the same
host clock (every container runs on one machine):

| Stamp | Set by | Where it is |
|---|---|---|
| `fired_at` | `ClosePrice.ToPriceWindow`, when the 5-min window fires (the watermark passed its end) | advisory JSON, `ema_window.fired_at` |
| `emitted_at` | `EmaFunction`, when the EMA result is produced | advisory JSON, `ema_window.emitted_at` |
| stored | the broker, when it appends the record (topic `advisories` uses `LogAppendTime`) | Kafka record timestamp |
| received | the `advisory-latency` service (producer-rs, mode `latency`), a consumer standing in for a trader's system | its output, read by [`bench/advisory_latency.py`](bench/advisory_latency.py) |

Latency is reported as p50/p95/p99/max per stage and in total (`fired_at` → received). It starts when the job
considers the window ended, which in market time is at least `OUT_OF_ORDERNESS_S` (60 s in this section and §7, 1 s now) after the window's end; §8 measures it.
The time from an event's arrival to the window firing is not covered (needs the planned ingestion timestamp).

[`bench/latency_sweep.py`](bench/latency_sweep.py) measures it against load: every point replays the same slice
(8 Nov, 09:00–09:30, 3.9 M rows, 6 window closes) into a fresh job, paced by the feed's own `Time` column at a
speed-up (`SPEEDUP`; 1× = real time, about 2,200 events/s in this slice, over 4,000/s at the open), with every
symbol subscribed (`config/subscriptions-all.txt`) so every BUY/SELL is a sample. Each configuration ran once.

> **Correction (2026-11-23): the 10× and 100× input rates in this section are not steady loads.** The producer paced
> from the first matching row by its raw `Time`, and early in the file there are stray rows (file row 41: 09:12:16, then
> 09:21:07). It waited for those and sent everything before them as one burst (§7). The latencies are real, but belong
> to a pause followed by a burst; the rates are averages over both. The "max" rates are not paced and were confirmed
> (514k events/s in §7). Baseline and unchained got the same input, so comparing them still makes sense. The slice also
> contained Xetra closing trades stamped 17:30, which moved the watermark and closed the last window, hence 260
> advisories here and 245 in §7.

### Fix: advisories expired minutes after they were written

Flink's Kafka sink gives each record the Flink record's timestamp, which for window results is the event time
(2021). With the topic's default `CreateTime` and 7-day retention, the broker deleted every advisory at its next
retention check, about a minute after writing it (broker log: `Deleting segment ... due to log retention time
604800000ms breach`, `largestRecordTimestamp=1636362299999`). A trader's system that connected later received
nothing, and the record timestamp said nothing about delivery. `kafka-setup` now creates `advisories` with
`message.timestamp.type=LogAppendTime` (and sets it on an existing topic). After the fix all 73 advisories of the
week were still in the topic after the run.

### Finding: the first window close after a job start dominated the first measurements

The first sweeps reported ~250–400 ms p50 even at low load, in every configuration. Split by window close, the
cause was the first close after the job starts:

| Window close (10-min slice, sink unchained, 10×) | Windows | Advisories (all "initial") | fired → emitted p50 | max |
|---|---|---|---|---|
| 09:05, first after the job start | 3,652 | 3,652 | 541 ms | 704 ms |
| 09:10 | 3,267 | 135 | **7 ms** | **25 ms** |

With empty EMA state, every symbol's first window gives an initial BUY, so the first close sends ~3,650 advisories
at once from a job that has just started (code not yet JIT-compiled, connections being opened). That one burst
was 96% of the samples. The scripts now leave out the first window close after a job start and report it
separately as cold start (`cold_*` in the results).

The earlier whole-system runs included that first close too: the full-week backlog run gave 771 ms p50 and the
live run (`scripts/run-live.sh`, producer at full speed) 318 ms p50, both over the 73 advisories of the normal
subscriptions.

### Experiments: where the time goes

Four candidate causes were tested with switches in the job (removed afterwards; see below):

| Switch | What it changed |
|---|---|
| sink unchained | the `ema_window` JDBC sink in its own task (`disableChaining()`) instead of running in the EMA function's thread |
| buffer timeout 5 ms | `env.setBufferTimeout(5)`: partly filled network buffers flushed every 5 ms instead of Flink's default 100 ms |
| no `ema_window` sink | the rows went to a `DiscardingSink` instead of TimescaleDB |
| unchained + 5 ms | both |

First round, 10-min slice, first close **included** (so mostly the cold start), window close → received,
p50 / p99 (ms):

| Configuration | 10× | 100× | max |
|---|---|---|---|
| baseline | 257 / 396 | 338 / 401 | 1,097 / 1,386 |
| sink unchained | 253 / 371 | 331 / 591 | 471 / 626 |
| buffer timeout 5 ms | 310 / 872 | 515 / 795 | 577 / 1,462 |
| no `ema_window` sink | 265 / 367 | 345 / 474 | 619 / 846 |
| unchained + 5 ms | 330 / 461 | 349 / 515 | 556 / 740 |

Second round, 30-min slice, first close **left out** (260 advisories and 16,202 windows per point):

| Speed (input rate) | Window close → received, p50 / p95 / max (ms): baseline | sink unchained |
|---|---|---|
| 10× (34 k events/s) | 12 / 39 / 92 | 11 / 46 / 109 |
| 100× (152 k events/s) | 6 / 222 / 237 | 8 / 21 / 96 |
| max (~540 k events/s) | 49 / 628 / 718 | 26 / 64 / 103 |

| Speed | Window close → EMA result over all windows, p50 / p99 (ms): baseline | sink unchained |
|---|---|---|
| 10× | 4 / 91 | 3 / 106 |
| 100× | 3 / 220 | 3 / 95 |
| max | 18 / 709 | 10 / 91 |

Per stage (second round, both configurations): EMA result → stored in Kafka p50 2–7 ms (max 39 ms), stored →
received p50 1–4 ms (max 14 ms). Cold start (the first close, left out above): 300–620 ms p50 chained, 310–480
ms unchained.

- **The computation is not the cost.** A window close is ~3,500 windows of a few floating-point operations each
  (two EMA updates and a comparison), microseconds of work; the median window took 3–4 ms from close to EMA result.
- **The JDBC sink in the EMA thread made the tail.** It upserts synchronously in batches of up to 1,000 rows, and
  chained it blocked the EMA function, so closed windows queued behind each batch: p95 grew from 39 ms to 628 ms
  with the input rate. In its own task the tail stayed around 100 ms up to the maximum rate.
- **A shorter buffer timeout did not help**; it was the same or worse, possibly from the extra flushes.
- **Kafka delivery is a few ms** in every configuration.
- **Change:** the `ema_window` sink now runs in its own task by default (`EmaPipeline`, `.disableChaining()`).

The switches were environment variables read by a small class in the job (`EXP_EMA_SINK=unchained|discard`,
`EXP_BUFFER_TIMEOUT_MS`), passed through `docker-compose.yml` and set with `bench/latency_sweep.py --set`. To repeat
an experiment, apply the change from the table above in `EmaPipeline` and run the sweep with `--name`. Raw
results: `bench/results/latency_sweep/*-exp-*` (first round), `*-exp2-*` (second round), each with
`results.jsonl` and a chart (`latency.html`).

Check after the change (switches removed, sink unchained in the code), same sweep, window close → received
p50 / p95 / max: 10× 10 / 56 / 99 ms, 100× 11 / 24 / 66 ms, max 32 / 228 / 232 ms (windows p99 at max: 198 ms,
vs 709 ms chained). The max point's tail varies between single runs (64 vs 228 ms p95). The full-week backlog run
gave the reference digest `19413a50…` and drained in 128.3 s (140.6 and 151.6 s in the last two runs with the sink
chained). At its read rate of ~2.25 M events/s, about 4× the sweep's maximum, the job falls behind: its 72
advisories after the first close took p50 590 ms, max 1.33 s.

Not measured yet: the real-time point (1×) and other speeds than 10×/100×/max on the 30-min slice (the default
sweep, about an hour), repeated runs, and latency from an event's arrival to the window firing.

## 7. Full week on 2026-11-23: correctness, latency, skew, memory

Same host and setup as above (clock source still HPET), 4 TaskManagers × 1 slot, parallelism 4, 8 partitions. The job
logic has not changed since §6; the producer's pacing, the benchmark scripts and their metrics have.

### Correctness: identical to the pandas reference

The digest `19413a50…` that earlier sections call the reference came from Flink runs. It is now also the digest of the
independent pandas implementation ([`reference/`](reference/README.md), zero prices excluded, 60 s lateness):
[`reference/compare.py`](reference/compare.py) writes the pandas result in the same format and gets
`19413a509d53044973e3e6b76c177af1`. So every run above and below with that digest is verified against it.

| Check | Backlog (3 runs) | Live run |
|---|---|---|
| Digest | `19413a50…` in all 3 | `19413a50…` |
| Windows, ticks, close, EMA38, EMA100 (compared bit for bit after runs 1 and 3 and the live run) | 1,855,645, none different | same |
| BUY / SELL / of which initial | 7,769 / 3,580 / 4,970, none different | same |
| Data issues (TIME_SKEW / LATE / PRICE_JUMP) | 6,681 / 5,506 / 638 | same |
| Messages in `advisories`: expected / received / missing / unexpected (after run 3 and the live run) | 73 / 73 / 0 / 0 | 73 / 73 / 0 / 0 |
| Rows written more than once | not checked | 0 |

The delivery check compared (trader, symbol, window, type) with the reference advisories times the subscriptions, with
a one-off script. A run of 8 Nov only matches the reference except for 14 windows that were still open when the input
ended (last event 22:35:05, so the watermark never passed 22:35); `compare.py --through` leaves such windows out.

### Throughput and resources

| | Backlog, run 1 | run 2 | run 3 | Earlier aligned backlog runs at parallelism 4 | Live, now | Live, §1–§2 |
|---|---:|---:|---:|---|---:|---:|
| Time (s) | 139.6 | 134.8 | 145.0 | 128.3–152.9 (5 runs) | 276.4 | 279 |
| Records/s (overall) | 2.07M | 2.14M | 1.99M | 1.89–2.25M | 1.05M | 1.04M |
| CPU %: TaskManagers | 464 | 514 | 468 | 389–533 | 283 | 251 |
| CPU %: TimescaleDB | 261 | 270 | 259 | 220–277 | 164 | 184 |
| CPU %: Kafka | 106 | 105 | 105 | 103–113 | 96 | 111 |
| CPU %: producer | – | – | – | – | 540 | 596 |
| Memory, max (MB): TaskManagers / their JVM heap | 3,701 / 1,384 | 3,744 / 1,373 | 3,896 / 1,447 | not measured | 3,482 / 1,306 | not measured |
| Memory, max (MB): Kafka / TimescaleDB | 1,187 / 182 | 1,264 / 237 | 827 / 216 | not measured | 1,143 / 187 | not measured |

Live: the producer sends each trading day in 48–59 s, and the last row is written about when it finishes. Loading the
week into Kafka with Flink stopped took 204 s (1.42M msg/s).

Busy / backpressured (ms/s, averages over subtasks; new method, see below):

| Task | Backlog, run 1 | run 2 | run 3 | Live |
|---|---|---|---|---|
| `ema_window` writer (TimescaleDB) | **815** / 0 | **885** / 0 | **784** / 0 | 486 / 0 |
| Kafka source + quality gate | 376 / 98 | 428 / 159 | 353 / 118 | 240 / 1 |
| 5-min window | 187 / 186 | 214 / 168 | 178 / 183 | 126 / 2 |
| EMA + advisories | 23 / **563** | 26 / **624** | 23 / **556** | 22 / 58 |
| Issues writer | 3 / 0 | 9 / 0 | 5 / 0 | 8 / 0 |

The results are in the range of the earlier runs. The bottleneck of the backlog run is the TimescaleDB writer; the
computation (EMA task) is nearly idle and waits for it. Live, the producer is the limit as before.

### Latency: depends on the load, mainly on how fast windows close

Window fired → received (ms), without the first close. Sweep: 8 Nov 09:00–09:30, every symbol subscribed, 245
advisories per point. Week: the normal subscriptions, 72 advisories per run, so p99 is the maximum.

| Run | Events/s | Window rows/s (average) | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| Sweep 10× | 21,863 | 0.1k | **10** | 19 | 65 | 72 |
| Sweep 100× | 218,631 | 0.9k | **11** | 55 | 104 | 105 |
| Sweep max | 514,140 | 2.2k | **41** | 55 | 70 | 70 |
| Live week | 1.05M | 6.7k | 29 | 790 | 1,064 | 1,064 |
| Backlog week, run 1 | 2.07M | 13.3k | 336 | 941 | 1,869 | 1,869 |
| Backlog week, run 2 | 2.14M | 13.8k | 607 | 1,405 | 1,744 | 1,744 |
| Backlog week, run 3 | 1.99M | 12.8k | 617 | 2,138 | 2,599 | 2,599 |

Earlier runs without the first close: live 36 / 1,273 / 1,608 (p50 / p95 / max), backlog 590 / 1,131 / 1,332 and
443 / 1,101 / 1,366. The median is similar; the backlog maximum is higher now (1.7–2.6 s). With 72 samples in an overloaded job
this may be noise, which was not tested. The feed's busiest minute on 8 Nov is 4,141 events/s (09:00), so the
10× point is already above the real peak. The real-time point (1×) has not been run.

Stages, p50 / max (ms):

| | fired → EMA result | EMA result → stored in Kafka | stored → received |
|---|---|---|---|
| Sweep 10× | 4 / 70 | 3 / 10 | 2 / 7 |
| Sweep 100× | 4 / 100 | 2 / 8 | 2 / 6 |
| Sweep max | 16 / 66 | 9 / 38 | 3 / 10 |
| Live week | 11 / 1,061 | 3 / 120 | 1 / 14 |
| Backlog week, run 1 | 316 / 1,864 | 2 / 97 | 1 / 9 |

Cold start (the first close, 3,652 advisories in the sweep) p50 / max: 10× 235 / 352 ms, 100× 426 / 623 ms, max
508 / 756 ms.

- **The sweep is the latency of the system; the backlog run is latency at saturation.** With input always waiting, it
  shows how deep the queues are.
- **Likely cause of the saturated latency (inferred, not tested):** the writer cannot keep up, its input buffers fill,
  and the EMA task, which sends to the writer and to the advisory branch, blocks on it. Windows then queue in front of
  it. The measurements fit: the delay is almost all before the EMA result, and the EMA task is 2% busy but 56–62%
  backpressured. Own task or not (§6), the database delays advisories once the buffers are full. Repeating the
  run with the sink discarded would test it.
- All values start when the window fires, so they exclude the out-of-orderness wait (60 s in these runs). §8 includes it.

### Skew

`bench/symbol_distribution.py` (offline, `bench/results/symbol_distribution/`), whole week:

| | All rows | Price events | Windows |
|---|---:|---:|---:|
| Symbols with any | 5,502 | 4,970 | 4,970 |
| Share of the top 1% of the symbols | 47.3% | 13.5% | 2.2% |
| Symbols that make up half | 63 | 306 | 1,383 |
| Gini coefficient | 0.86 | 0.68 | 0.41 |

| Max / mean | Predicted from the hashes | Measured (backlog runs) |
|---|---:|---:|
| Rows per Kafka partition (8) | 1.33 | 1.33 |
| Rows per source subtask (4) | – | 1.05 |
| Price events per window subtask (4) | 1.10 | 1.10 |
| Windows per EMA subtask (4) | 1.03 | 1.03 |
| Busy time per writer subtask (4) | – | 1.06 |

The long tail is strongest in the raw rows and mostly gone in the windows: filtering to price events and grouping into
windows flattens it, and the writer, the bottleneck, is evenly loaded. Balance worsens with more partitions (rows:
1.49 at 16, 2.02 at 32) and subtasks (price events: 1.15 at 8, 1.48 at 32). One symbol never gets more than one subtask, but the hottest (`MC.FR`) peaks at
22 price events/s in real time. The comparison with evenly spread symbols (`--workload uniform`) has not been run.

### Producer pacing fix

`SPEEDUP` now follows the feed's clock (`producer-rs/src/feed.rs`: median `Time` of blocks of 1,000 rows, smoothed over
neighbouring blocks) instead of the raw `Time` of the first matching row. 8 Nov 09:00–09:30 at 30×, against a
separate Kafka broker with nothing else running:

| | Old | New |
|---|---|---|
| Send time (should be 60.0 s) | 46.0 s | 59.97 s |
| First 34 s | almost nothing | about 172k rows in the first second, then 53–91k rows/s |
| Then | about 600k rows/s for 7 s | 50–78k rows/s |

In the sweep the rates match the feed: 3,933,169 rows in 179.9 s at 10× (should be 180) and 17.99 s at 100× (18). On
the five trading days the clock never jumps more than 1.0 s between 09:00 and 17:30. The slice has 67 rows fewer than
before, and the last window (09:30) is not closed any more, because nothing later in the slice passes it; `--end 09:36`
would include it.

### Metrics corrected

- **Busy and backpressured time** (scripts and Grafana): from Flink's accumulated counters, as shares of busy +
  backpressured + idle. The
  `...TimeMsPerSecond` gauges are moving averages and most subtasks read 1000 in the first sample, so idle tasks showed
  about 70 ms/s in full-week runs (now 3–9) and the source about 550 instead of 353–428. The writer (about 855)
  hardly changed.
- **Lag:** `records_lag_max` stayed at 29.9M after everything had been read. `pendingRecords` is used instead (also in
  Grafana): on average 237 and at most 8,397 records in the live run. It leaves out records already fetched, so it
  cannot be compared with the 45k in §2.
- **Memory:** Prometheus keeps the last value of removed containers for minutes, which doubled the sum; only containers
  cAdvisor has just seen are counted.
- In `bench/results/backlog_runs.jsonl`, `backlog-20261123T201209` (busy time) and `backlog-20261123T201654` (memory)
  are one-day test runs from before these corrections; do not use them.

Not run yet: `scaling_matrix.py`, `peak_test.py`, `subscriber_sweep.py`, the uniform workload and the real-time (1×)
sweep point. So scaling, per-exchange load and subscriber growth have no current results.

## 8. Out-of-orderness bound (2026-11-24): 60 s → 1 s

The job waits `OUT_OF_ORDERNESS_S` (D) of market time after a window's end. It was 60 s, which the latency of §6 and §7
leaves out. A smaller D is safe only if it does not make more events late.

### Correctness: every D gives the reference result

Full week, `WEEK_DIGEST` and 5,506 late events in every run:

| D (s) | Stored backlog, runs | Live week (`run-live.sh`) |
|---|---|---|
| 60 | 1 | §7 |
| 5 | 1 | not run |
| 1 | 3 (one with the default, also compared bit for bit) | identical bit for bit (`compare.py`), 73 of 73 advisories delivered, 0 unexpected |
| 0.5 | 3 | identical bit for bit, 73 of 73, 0 unexpected |
| 0 | 2 | not run |

- The backlog runs are the more lenient test, because partitions may drift apart by up to 300 s of event time
  (alignment), and the slowest holds the watermark back. In the live runs the partitions stay together.
- The pandas reference with `--lateness 1` gives the same windows and advisories as with 60 s, and 5,506 late events.
- 8 Nov alone (1 s): identical to the reference, digest `a7ed875d2291c71a8151275953a15be4`, 7 windows still open at the end of
  the input. With 60 s it was `fffd77b9…` and 14 windows.
- Durations (125.9–135.1 s) are not comparable: the first runs were made partly on battery power.
- `fired → received` p50 / p95 / max: 13 / 1,676 / 1,967 ms (D = 1 s) and 23 / 1,468 / 1,537 ms (0.5 s), against
  29 / 790 / 1,064 ms in §7 (60 s), one run and 72 samples each. Four more live runs at 1 s (§9) had p95 854–1,360 ms, so one
  run at 60 s cannot show whether 1 s made the tail higher. That needs repeated runs at 60 s.

### Delay in market time

[`bench/window_delay.py`](bench/window_delay.py) replays 8 Nov 09:04–09:17 at 1× with every symbol subscribed. In real time
a window's end has a wall-clock time, so the whole delay can be measured. One run per value, 2 window closes (09:10
and 09:15, without the cold start), 501 advisories (`bench/results/window_delay/20261123T225620-final/`):

| D (s) | End → fired, the two closes (ms) | End → received p50 / p95 / max (ms) | Late events |
|---|---|---|---|
| 60 | 60,688 / 60,580 | 60,706 / 60,791 / 60,792 | 0 |
| 5 | 5,545 / 5,315 | 5,559 / 5,600 / 5,602 | 0 |
| 1 | 1,633 / 1,472 | 1,649 / 1,737 / 1,741 | 0 |

- **1 s instead of 60 s removes 59 s of the delay.** The percentiles come from two closes, so they describe
  those two bursts, not a distribution of window delays.
- **D = 0.5 s was run, but its delay (614 ms p50) is not valid, and is left out.** `Time`, which paces the replay, has whole
  seconds in 99.9% of the rows, while `Trading time` has milliseconds and is up to 1 s later. A second's events are sent
  together at its start, so the event that closes a window with D = 0.5 s arrives 2–7 ms after the window's end, before it
  happened. One window fired 357 ms after its end, less than D. For whole seconds the events that pass the bound
  arrive at end + D (checked per partition in the records of the last run), so those values hold.
  Expected for 0.5 s: about 1 s, not measured.
- The delay above D was 0.3–0.7 s (D = 1: 0.5–0.6 s). Flink's watermark interval (200 ms) explains part of it.
  `fetch.min.bytes` = 64 KiB with Kafka's default wait of 500 ms would fit the rest at this low rate; not tested.

**Default: 1 s.** The smallest value whose delay could be measured, and above 0, so a small disorder is tolerated
(see [architecture §3.4](docs/architecture.md#34-watermarks-and-late-events-nfr-7) for what it gives up).

## 9. Every symbol subscribed (2026-11-24): no clear effect

Full week, 4 TaskManagers × 1 slot, D = 1 s: the default subscriptions (73 messages, 14 symbols) against every symbol
subscribed (`config/subscriptions-all.txt`, 11,349 messages, one trader). [`bench/subscription_test.py`](bench/subscription_test.py)
ran the settings alternately (ABBA) after a discarded warm-up: 3 backlog and 2 live runs each, on mains power at the
start and the end (`bench/results/subscription_test/20261124T084256-all-vs-default/`).

**Correctness:** all 10 runs gave `WEEK_DIGEST` and 5,506 late events. With every symbol subscribed, 11,349 of 11,349
advisories were delivered in each of the 5 runs, none missing or unexpected, so delivery is now checked for every
symbol with an advisory. With the default file, 73 of 73 in each of the 5 runs.

Per-run values, default → all. Latencies exclude the first close:

| | Stored backlog (3 runs each) | Live (2 runs each) |
|---|---|---|
| Processing time (s) | 125.7 / 128.3 / 128.1 → 126.3 / 130.2 / 130.3 | 257.0 / 258.2 → 254.1 / 255.9 |
| CPU TaskManager (% of a core) | 509 / 511 / 536 → 532 / 521 / 545 | 278 / 280 → 286 / 282 |
| CPU Kafka | 105 / 103 / 104 → 107 / 107 / 110 | 92 / 94 → 96 / 95 |
| CPU TimescaleDB | 283 / 269 / 270 → 284 / 277 / 271 | 164 / 173 → 171 / 172 |
| EMA task busy (ms/s) | 24 / 24 / 25 → 32 / 33 / 32 | 21 / 19 → 24 / 25 |
| Delivered messages (72 → 11,337), fired → received p50 (ms) | 727 / 609 / 600 → 570 / 584 / 511 | 32 / 33 → 38 / 37 |
| Same, p95 | 1,350 / 1,128 / 1,351 → 1,840 / 2,054 / 1,212 | 854 / 1,360 → 875 / 1,196 |
| The same 11,337 advisories in both settings, fired → EMA result p95 | 1,377 / 1,344 / 1,434 → 1,824 / 2,035 / 1,203 | 824 / 1,624 → 855 / 1,180 |
| All 1.86 M windows, fired → EMA result p95 | 1,166 / 1,150 / 1,169 → 1,203 / 1,206 / 1,149 | 564 / 566 → 488 / 460 |
| Stored → received p95 | 5 / 10 / 6 → 20 / 24 / 18 | 7 / 8 → 21 / 26 |

- **Throughput, memory and typical latency did not change clearly**, with 155 times the messages. The p50 and p95 of delivered
  messages describe different advisories in the two settings; the two rows below them do not.
- **Small, consistent costs:** Kafka CPU about 2–4% higher, EMA task 3–9 ms/s busier (it sends the messages), and
  `stored → received` p95 10–20 ms higher. Bursts read by one consumer are a likely cause of the last (not tested).
- **Unclear: p95 at saturation.** It was higher in 2 of the 3 backlog runs with every symbol, and lower than in every default
  run in the third, so it may be noise. Three runs cannot rule out a small effect. The live runs show no slowdown, but the two
  runs per setting differ a lot (824 and 1,624 ms in the default setting).

## Caveats

- Everything runs on one laptop, so producer, Kafka, Flink and TimescaleDB compete for the same 16 threads.
- The HPET clock source makes any per-record clock read expensive (§3); this also applies to the planned
  ingestion timestamp and to `System.currentTimeMillis()` in Flink operators.
- CPU and busy-time values are averages over one run; the full week ran twice with Kafka, the failover test twice (2 kills;
  Iggy had 7).
- Messages are CSV text, one row per Kafka record. Several rows per record would cut the per-record cost but
  would change offsets from one per row to one per batch.
