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
| Iggy | 0.10.0, 2 shards, cores not pinned, 1 topic `ticks` with 4 partitions |
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
