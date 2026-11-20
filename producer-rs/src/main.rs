//! Replay producer and Kafka topic setup.
//!
//! Modes: `setup` (create the topics `ticks` and `advisories`) and `replay` (send CSV rows to `ticks`).
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

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
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

macro_rules! log {
    ($($arg:tt)*) => { println!("[replay-producer] {}", format!($($arg)*)) };
}

fn main() -> Res<()> {
    let cfg = Config::from_env()?;
    let mode = std::env::args().nth(1).unwrap_or_else(|| "replay".to_string());
    match mode.as_str() {
        "setup" => setup(&cfg),
        "replay" => replay(&cfg),
        other => Err(format!("unknown mode: {other} (setup|replay)").into()),
    }
}

/// Creates both topics if they do not exist. An existing topic is kept as is, even with another partition count.
fn setup(cfg: &Config) -> Res<()> {
    let admin: AdminClient<DefaultClientContext> =
        ClientConfig::new().set("bootstrap.servers", &cfg.bootstrap).create()?;
    let topics = [
        NewTopic::new(&cfg.topic, cfg.partitions, TopicReplication::Fixed(1)).set("retention.ms", &cfg.retention_ms),
        NewTopic::new(&cfg.advisories_topic, cfg.advisories_partitions, TopicReplication::Fixed(1)),
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
    let (sent, symbols) = std::thread::scope(|scope| -> Res<(u64, usize)> {
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
        "replay done: sent {} messages ({} distinct symbols) in {:.2} s{}",
        sent,
        symbols,
        start.elapsed().as_secs_f64(),
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
) -> Res<(u64, usize)> {
    let threads = cfg.threads.clamp(1, partitions as usize);
    let mut chunks: Vec<Chunk> = (0..threads).map(|_| Vec::with_capacity(CHUNK)).collect();
    let mut read: u64 = 0;
    let mut symbols: HashSet<Vec<u8>> = HashSet::new();
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
            let symbol_len = line.iter().position(|&b| b == b',').unwrap_or(line.len());
            let symbol = &line[..symbol_len];
            if !symbols.contains(symbol) {
                symbols.insert(symbol.to_vec());
            }
            let p = java_string_hash(symbol).rem_euclid(partitions);
            chunks[p as usize % threads].push((p, symbol_len, line));
            read += 1;
            if read % CHUNK as u64 == 0 {
                flush(&mut chunks)?;
                pace(start, read, cfg.rate);
            }
        }
    }
    flush(&mut chunks)?;
    Ok((read, symbols.len()))
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
    use super::java_string_hash;

    /// (symbol, Java `s.hashCode()`, Java `Math.floorMod(s.hashCode(), 4)`), produced with JDK 25.
    const JAVA: &[(&str, i32, i32)] = &[
        ("hello", 99162322, 2),
        ("IACWE.FR", -2065065823, 1),
        ("A2ASZ7.ETR", 528887029, 1),
        ("SGFI.FR", -1547068413, 3),
        ("ZZZZZZZZZZZZ.ETR", -236369387, 1), // overflows i32
    ];

    #[test]
    fn matches_java_string_hashcode_and_partition() {
        assert_eq!(java_string_hash(b""), 0);
        for &(s, hash, partition) in JAVA {
            assert_eq!(java_string_hash(s.as_bytes()), hash, "{s}");
            assert_eq!(java_string_hash(s.as_bytes()).rem_euclid(4), partition, "{s}");
        }
    }
}
