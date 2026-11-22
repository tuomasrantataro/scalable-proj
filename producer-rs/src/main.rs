//! Replay producer and Kafka topic setup.
//!
//! Modes: `setup` (create the topics `ticks` and `advisories`), `replay` (send CSV rows to `ticks`) and
//! `latency` (read `advisories` like a trader's system would and print one JSON line per message with its
//! receive time, for bench/advisory_latency.py).
//! Each row is one record: key = symbol ID, value = the CSV row. The partition is chosen here, as Java's
//! `String.hashCode()` of the symbol floor-mod the partition count (the partitioning used since the first
//! producer), so all rows of a symbol go to one partition in file order. librdkafka batches and compresses
//! the records per partition in the background (`linger.ms`, `batch.size`, `compression.type`).
//!
//! Threads: one reads the file and hands rows to `PRODUCER_THREADS` sender threads, which share one producer.
//! Each partition is always sent by the same thread (partition mod threads), so a symbol's rows stay in file
//! order, and idempotence keeps that order across librdkafka's retries. Several senders are needed because
//! librdkafka reads the monotonic clock for every record (enqueue time), and on a host whose clock source is
//! HPET each read is a ~1.4 us system call: one thread then sends only ~250k records/s (MEASUREMENTS.md).

use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{DeliveryResult, Message, Timestamp};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

type Res<T> = Result<T, Box<dyn Error>>;

struct Config {
    bootstrap: String,
    topic: String,
    partitions: i32,
    /// Output topic for BUY/SELL advisories (written by the Flink job), and its partition count.
    advisories_topic: String,
    advisories_partitions: i32,
    /// `retention.ms` of the input topic (-1 = keep forever).
    retention_ms: String,
    csv_file: String,
    repeat: u32,
    rate: u64,
    /// Replay at this multiple of the feed's own pace (the `Time` column; 1 = real time). 0 = off.
    speedup: f64,
    /// Only rows whose `Time` is in [start_time, end_time), in ms since midnight (`START_TIME`/`END_TIME`, HH:MM).
    start_time: Option<i64>,
    end_time: Option<i64>,
    compression: String,
    linger_ms: String,
    batch_bytes: String,
    threads: usize,
}

impl Config {
    fn from_env() -> Res<Self> {
        Ok(Self {
            bootstrap: env("KAFKA_BOOTSTRAP", "kafka:9092"),
            topic: env("KAFKA_TOPIC", "ticks"),
            partitions: env("KAFKA_PARTITIONS", "8").parse()?,
            advisories_topic: env("KAFKA_ADVISORIES_TOPIC", "advisories"),
            advisories_partitions: env("KAFKA_ADVISORIES_PARTITIONS", "4").parse()?,
            retention_ms: env("KAFKA_RETENTION_MS", "-1"),
            csv_file: env("CSV_FILE", "/data/pala.csv"),
            repeat: env("REPEAT", "1").parse()?,
            rate: env("RATE", "0").parse()?,
            speedup: env("SPEEDUP", "0").parse()?,
            start_time: clock_env("START_TIME")?,
            end_time: clock_env("END_TIME")?,
            compression: env("KAFKA_COMPRESSION", "lz4"),
            linger_ms: env("KAFKA_LINGER_MS", "20"),
            batch_bytes: env("KAFKA_BATCH_BYTES", "1000000"),
            threads: env("PRODUCER_THREADS", "8").parse()?,
        })
    }
}

fn env(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => default.to_string(),
    }
}

/// "HH:MM" or "HH:MM:SS[.mmm]" from an env var → ms since midnight; unset or empty → None.
fn clock_env(key: &str) -> Res<Option<i64>> {
    let v = env(key, "");
    if v.is_empty() {
        return Ok(None);
    }
    let full = if v.len() == 5 { format!("{v}:00") } else { v.clone() };
    clock_ms(full.as_bytes()).map(Some).ok_or_else(|| format!("{key}={v}: expected HH:MM").into())
}

/// "HH:MM:SS" or "HH:MM:SS.mmm" → ms since midnight.
fn clock_ms(t: &[u8]) -> Option<i64> {
    let num = |b: &[u8]| -> Option<i64> {
        if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(b).ok()?.parse().ok()
    };
    if t.len() < 8 || t[2] != b':' || t[5] != b':' {
        return None;
    }
    let (h, m, sec) = (num(&t[0..2])?, num(&t[3..5])?, num(&t[6..8])?);
    let ms = match t.get(8) {
        Some(b'.') => num(&t[9..])? * 10i64.pow(3u32.saturating_sub((t.len() - 9) as u32)),
        None => 0,
        _ => return None,
    };
    Some(((h * 60 + m) * 60 + sec) * 1000 + ms)
}

/// The `Time` column (4th field: ID,SecType,Date,Time,...) of a CSV row, in ms since midnight.
fn row_time(line: &[u8]) -> Option<i64> {
    let mut fields = line.split(|&b| b == b',');
    clock_ms(fields.nth(3)?)
}

macro_rules! log {
    ($($arg:tt)*) => { println!("[replay-producer] {}", format!($($arg)*)) };
}

fn main() -> Res<()> {
    let cfg = Config::from_env()?;
    let mode = std::env::args().nth(1).unwrap_or_else(|| "replay".to_string());
    match mode.as_str() {
        "setup" => setup(&cfg),
        "replay" => replay(&cfg),
        "latency" => latency(&cfg),
        other => Err(format!("unknown mode: {other} (setup|replay|latency)").into()),
    }
}

/// Advisory records are stamped by the broker when stored (LogAppendTime). The Flink Kafka sink would otherwise
/// give them the record's event time (2021), so they carry no send time and the 7-day retention deletes them
/// minutes after they are written.
const ADVISORY_TIMESTAMP: (&str, &str) = ("message.timestamp.type", "LogAppendTime");

/// Creates both topics if they do not exist. An existing topic is kept as is, even with another partition count;
/// only the timestamp type of `advisories` is set on it.
fn setup(cfg: &Config) -> Res<()> {
    let admin: AdminClient<DefaultClientContext> =
        ClientConfig::new().set("bootstrap.servers", &cfg.bootstrap).create()?;
    let topics = [
        NewTopic::new(&cfg.topic, cfg.partitions, TopicReplication::Fixed(1)).set("retention.ms", &cfg.retention_ms),
        NewTopic::new(&cfg.advisories_topic, cfg.advisories_partitions, TopicReplication::Fixed(1))
            .set(ADVISORY_TIMESTAMP.0, ADVISORY_TIMESTAMP.1),
    ];
    let opts = AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(10))));
    let mut attempt = 1;
    let results = loop {
        match futures::executor::block_on(admin.create_topics(&topics, &opts)) {
            Ok(r) => break r,
            Err(e) if attempt < 30 => {
                log!("kafka not ready ({e}), retrying...");
                std::thread::sleep(Duration::from_secs(1));
                attempt += 1;
            }
            Err(e) => return Err(e.into()),
        }
    };
    for r in results {
        match r {
            Ok(name) => log!("created topic {name}"),
            Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => log!("topic {name} exists, kept as is"),
            Err((name, code)) => return Err(format!("creating topic {name}: {code}").into()),
        }
    }
    // An advisories topic created before LogAppendTime was introduced. (A non-incremental AlterConfigs replaces
    // all dynamic settings of the topic; this topic has no others.)
    let alter = AlterConfig::new(ResourceSpecifier::Topic(&cfg.advisories_topic))
        .set(ADVISORY_TIMESTAMP.0, ADVISORY_TIMESTAMP.1);
    for r in futures::executor::block_on(admin.alter_configs([&alter], &opts))? {
        r.map_err(|(_, code)| format!("setting {} on {}: {code}", ADVISORY_TIMESTAMP.0, cfg.advisories_topic))?;
    }
    let probe: BaseProducer = ClientConfig::new().set("bootstrap.servers", &cfg.bootstrap).create()?;
    for name in [&cfg.topic, &cfg.advisories_topic] {
        log!("setup done: topic {} with {} partitions", name, partition_count(&probe, name)?);
    }
    Ok(())
}

fn partition_count<C: ProducerContext>(producer: &BaseProducer<C>, topic: &str) -> Res<i32> {
    let md = producer.client().fetch_metadata(Some(topic), Duration::from_secs(10))?;
    let t = md.topics().first().ok_or("no topic metadata")?;
    if let Some(e) = t.error() {
        return Err(format!("topic {topic}: {:?}, run setup first", RDKafkaErrorCode::from(e)).into());
    }
    Ok(t.partitions().len() as i32)
}

/// Counts failed deliveries; librdkafka retries transient errors itself, so these are final.
struct Deliveries {
    failed: AtomicU64,
}

impl ClientContext for Deliveries {}

impl ProducerContext for Deliveries {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        if let Err((e, _)) = result {
            if self.failed.fetch_add(1, Relaxed) < 5 {
                log!("delivery failed: {e}");
            }
        }
    }
}

/// Rows read per hand-over to a sender thread (and per pacing check).
const CHUNK: usize = 4096;

/// Rows for one sender thread: (partition, symbol length, row).
type Chunk = Vec<(i32, usize, Vec<u8>)>;

fn replay(cfg: &Config) -> Res<()> {
    let producer: BaseProducer<Deliveries> = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("enable.idempotence", "true")
        .set("compression.type", &cfg.compression)
        .set("linger.ms", &cfg.linger_ms)
        .set("batch.size", &cfg.batch_bytes)
        .set("queue.buffering.max.messages", "2000000")
        .set("queue.buffering.max.kbytes", "1048576")
        .create_with_context(Deliveries { failed: AtomicU64::new(0) })?;
    let partitions = partition_count(&producer, &cfg.topic)?;
    let threads = cfg.threads.clamp(1, partitions as usize);
    log!(
        "connected to {}, topic {} with {} partitions, {} sender threads",
        cfg.bootstrap, cfg.topic, partitions, threads
    );

    let start = Instant::now();
    let (sent, symbols, active) = std::thread::scope(|scope| -> Res<(u64, usize, f64)> {
        let mut senders = Vec::with_capacity(threads);
        let mut workers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let (tx, rx) = sync_channel::<Chunk>(16);
            senders.push(tx);
            let producer = &producer;
            let topic = cfg.topic.as_str();
            workers.push(scope.spawn(move || send_rows(producer, topic, rx)));
        }
        let read = read_rows(cfg, partitions, start, |thread, chunk| {
            senders[thread].send(chunk).map_err(|_| "sender thread stopped".into())
        });
        drop(senders); // closes the channels, so the workers finish
        for w in workers {
            w.join().map_err(|_| "sender thread panicked")??;
        }
        read
    })?;
    producer.flush(Timeout::Never)?;

    let failed = producer.context().failed.load(Relaxed);
    log!(
        "replay done: sent {} messages ({} distinct symbols) in {:.2} s (first to last row sent: {:.2} s){}",
        sent,
        symbols,
        start.elapsed().as_secs_f64(),
        active,
        if failed > 0 { format!(", {failed} FAILED") } else { String::new() }
    );
    if failed > 0 {
        return Err(format!("{failed} records were not delivered").into());
    }
    Ok(())
}

/// Reads the CSV file `repeat` times and hands rows, grouped by sender thread, to `hand_over`.
/// Returns (rows read, distinct symbols).
fn read_rows(
    cfg: &Config,
    partitions: i32,
    start: Instant,
    mut hand_over: impl FnMut(usize, Chunk) -> Res<()>,
) -> Res<(u64, usize, f64)> {
    let threads = cfg.threads.clamp(1, partitions as usize);
    let mut chunks: Vec<Chunk> = (0..threads).map(|_| Vec::with_capacity(CHUNK)).collect();
    let mut read: u64 = 0;
    let mut symbols: HashSet<Vec<u8>> = HashSet::new();
    let filtered = cfg.start_time.is_some() || cfg.end_time.is_some();
    let mut first_time: Option<i64> = None; // feed time of the first row sent, for SPEEDUP
    let mut first_sent: Option<Instant> = None;
    let mut last_sent = start;
    let mut flush = |chunks: &mut Vec<Chunk>| -> Res<()> {
        for (t, c) in chunks.iter_mut().enumerate() {
            if !c.is_empty() {
                hand_over(t, std::mem::replace(c, Vec::with_capacity(CHUNK)))?;
            }
        }
        Ok(())
    };
    for _ in 0..cfg.repeat {
        let mut reader = BufReader::with_capacity(1 << 20, File::open(&cfg.csv_file)?);
        loop {
            let mut line = Vec::with_capacity(128);
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            // strip "\n" / "\r\n", like Java's readLine()
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            // DEBS files: '#' comment lines (licence, column descriptions) and the "ID,SecType,..." header
            if line.iter().all(u8::is_ascii_whitespace) || line.starts_with(b"#") || line.starts_with(b"ID,") {
                continue;
            }
            if filtered || cfg.speedup > 0.0 {
                let t = row_time(&line);
                if filtered {
                    let Some(t) = t else { continue };
                    if cfg.start_time.is_some_and(|s| t < s) || cfg.end_time.is_some_and(|e| t >= e) {
                        continue;
                    }
                }
                // Paced by the feed's own clock: a row is due (t - first) / speedup after the start. Rows
                // already buffered are handed over before waiting, so nothing sits in a chunk while we sleep.
                if let (Some(t), true) = (t, cfg.speedup > 0.0) {
                    let first = *first_time.get_or_insert(t);
                    let paced_from = *first_sent.get_or_insert_with(Instant::now);
                    let due = paced_from + Duration::from_secs_f64((t - first).max(0) as f64 / 1000.0 / cfg.speedup);
                    let now = Instant::now();
                    last_sent = now.max(due);
                    if due > now + Duration::from_millis(1) {
                        flush(&mut chunks)?;
                        std::thread::sleep(due - now);
                    }
                }
            }
            let symbol_len = line.iter().position(|&b| b == b',').unwrap_or(line.len());
            let symbol = &line[..symbol_len];
            if !symbols.contains(symbol) {
                symbols.insert(symbol.to_vec());
            }
            let p = java_string_hash(symbol).rem_euclid(partitions);
            chunks[p as usize % threads].push((p, symbol_len, line));
            read += 1;
            first_sent.get_or_insert_with(Instant::now);
            if read % CHUNK as u64 == 0 {
                flush(&mut chunks)?;
                // the clock is read per chunk, not per row (~1.4 us per read on HPET hosts); paced rows set it above
                if cfg.speedup <= 0.0 {
                    last_sent = Instant::now();
                }
                pace(start, read, cfg.rate);
            }
        }
    }
    flush(&mut chunks)?;
    // Without a time filter or pacing, rows are sent until the end of the file
    let last = if filtered || cfg.speedup > 0.0 { last_sent } else { Instant::now() };
    let active = first_sent.map_or(0.0, |f| last.saturating_duration_since(f).as_secs_f64());
    Ok((read, symbols.len(), active))
}

/// Sender thread: enqueues each row as a record of its partition, in the order received.
fn send_rows(producer: &BaseProducer<Deliveries>, topic: &str, rx: Receiver<Chunk>) -> Result<(), String> {
    for chunk in rx {
        // Record timestamp (CreateTime) once per chunk: without one, librdkafka reads the wall clock per record.
        let timestamp = now_ms();
        for (p, symbol_len, line) in &chunk {
            let mut record = BaseRecord::to(topic)
                .partition(*p)
                .key(&line[..*symbol_len])
                .payload(&line[..])
                .timestamp(timestamp);
            loop {
                match producer.send(record) {
                    Ok(()) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                        record = r;
                        producer.poll(Duration::from_millis(10)); // wait for the queue to drain
                    }
                    Err((e, _)) => return Err(e.to_string()),
                }
            }
        }
        producer.poll(Duration::ZERO); // serve delivery callbacks
    }
    Ok(())
}

/// Reads every partition of the advisories topic from the start, without a consumer group (no rebalance delay),
/// and prints one JSON line per message: receive time, the broker's timestamp and the advisory itself. Starts
/// with a `{"ready_ms":...}` line: messages stored before it were not received live. Runs until stopped.
fn latency(cfg: &Config) -> Res<()> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", "advisory-latency") // required by librdkafka; nothing is committed
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "100")
        .create()?;
    let md = consumer.fetch_metadata(Some(&cfg.advisories_topic), Duration::from_secs(10))?;
    let t = md.topics().first().ok_or("no topic metadata")?;
    if t.error().is_some() || t.partitions().is_empty() {
        return Err(format!("topic {} not found, run setup first", cfg.advisories_topic).into());
    }
    let mut tpl = TopicPartitionList::new();
    for p in t.partitions() {
        tpl.add_partition_offset(&cfg.advisories_topic, p.id(), Offset::Beginning)?;
    }
    consumer.assign(&tpl)?;
    eprintln!("[advisory-latency] reading {} partitions of {}", t.partitions().len(), cfg.advisories_topic);
    println!("{{\"ready_ms\":{}}}", now_ms());
    loop {
        let Some(m) = consumer.poll(Duration::from_secs(1)) else { continue };
        let m = m?;
        let received = now_ms();
        let (ts_type, ts) = match m.timestamp() {
            Timestamp::LogAppendTime(t) => ("LogAppendTime", t),
            Timestamp::CreateTime(t) => ("CreateTime", t),
            Timestamp::NotAvailable => ("none", -1),
        };
        let payload = std::str::from_utf8(m.payload().unwrap_or_default()).unwrap_or("null");
        // stdout is line-buffered, so each line is out as soon as it is written
        println!(
            "{{\"received_ms\":{received},\"kafka_ts\":{ts},\"ts_type\":\"{ts_type}\",\"partition\":{},\"offset\":{},\"advisory\":{payload}}}",
            m.partition(),
            m.offset()
        );
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

/// Java's `String.hashCode()` (s[0]*31^(n-1) + ... + s[n-1], wrapping i32, over UTF-16 units),
/// so each symbol goes to the same partition as with the original Java producer.
fn java_string_hash(bytes: &[u8]) -> i32 {
    String::from_utf8_lossy(bytes)
        .encode_utf16()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

fn pace(start: Instant, sent: u64, rate: u64) {
    if rate == 0 {
        return;
    }
    let target = start + Duration::from_secs_f64(sent as f64 / rate as f64);
    let now = Instant::now();
    if target > now {
        std::thread::sleep(target - now);
    }
}

#[cfg(test)]
mod tests {
    use super::{clock_ms, java_string_hash, row_time};

    /// (symbol, Java `s.hashCode()`, Java `Math.floorMod(s.hashCode(), 4)`), produced with JDK 25.
    const JAVA: &[(&str, i32, i32)] = &[
        ("hello", 99162322, 2),
        ("IACWE.FR", -2065065823, 1),
        ("A2ASZ7.ETR", 528887029, 1),
        ("SGFI.FR", -1547068413, 3),
        ("ZZZZZZZZZZZZ.ETR", -236369387, 1), // overflows i32
    ];

    #[test]
    fn parses_feed_times() {
        assert_eq!(clock_ms(b"09:05:01.250"), Some(((9 * 60 + 5) * 60 + 1) * 1000 + 250));
        assert_eq!(clock_ms(b"09:05:01.5"), Some(((9 * 60 + 5) * 60 + 1) * 1000 + 500));
        assert_eq!(clock_ms(b"09:05:01"), Some(((9 * 60 + 5) * 60 + 1) * 1000));
        assert_eq!(clock_ms(b""), None);
        assert_eq!(clock_ms(b"9:05"), None);
        assert_eq!(row_time(b"A2ASZ7.ETR,E,08-11-2021,01:01:07.000,,,"), Some(3_667_000));
        assert_eq!(row_time(b"A2ASZ7.ETR,E"), None);
    }

    #[test]
    fn matches_java_string_hashcode_and_partition() {
        assert_eq!(java_string_hash(b""), 0);
        for &(s, hash, partition) in JAVA {
            assert_eq!(java_string_hash(s.as_bytes()), hash, "{s}");
            assert_eq!(java_string_hash(s.as_bytes()).rem_euclid(4), partition, "{s}");
        }
    }
}
