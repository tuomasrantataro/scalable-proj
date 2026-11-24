//! Replay producer and Kafka topic setup.
//!
//! Modes: `setup` (create the topics `ticks` and `advisories`), `replay` (send CSV rows to `ticks`) and
//! `latency` (read `advisories` like a trader's system would and print one JSON line per message with its
//! receive time, for bench/advisory_latency.py; with LATENCY_OUTPUT=summary it aggregates instead, for
//! bench/measure.py, where a line per message would make the consumer the bottleneck).
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

mod feed;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::BufReader;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{DeliveryResult, Message, Timestamp};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use feed::{FeedClock, clock_ms, read_row};

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
    /// Replay at this multiple of the feed's own pace (feed.rs; 1 = real time). 0 = off.
    speedup: f64,
    /// Only rows the feed delivered in [start_time, end_time), in ms since midnight (`START_TIME`/`END_TIME`, HH:MM).
    start_time: Option<i64>,
    end_time: Option<i64>,
    compression: String,
    linger_ms: String,
    batch_bytes: String,
    threads: usize,
    /// Replace the symbols by this many equally frequent ones (bench/backlog.py --workload uniform). 0 = off.
    uniform_symbols: u64,
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
            uniform_symbols: env("UNIFORM_SYMBOLS", "0").parse()?,
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

/// Creates both topics if they do not exist. An existing topic is kept as is, even with other settings.
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
/// Returns (rows read, distinct symbols, seconds from the first to the last row).
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
    let paced = cfg.speedup > 0.0;
    let needs_clock = paced || cfg.start_time.is_some() || cfg.end_time.is_some();
    let (mut price_rows, mut other_rows) = (0u64, 0u64); // only counted for uniform_symbols
    let mut first_sent: Option<Instant> = None;
    let mut now = start; // last reading of the clock, for pacing
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
        let mut feed = needs_clock.then(FeedClock::default);
        let mut origin: Option<(i64, Instant)> = None; // feed time and send time of the first row of this pass
        loop {
            let next = match feed.as_mut() {
                Some(clock) => clock.next_row(&mut reader)?,
                None => read_row(&mut reader)?.map(|line| (line, 0)),
            };
            let Some((mut line, feed_time)) = next else { break };
            if cfg.start_time.is_some_and(|s| feed_time < s) {
                continue;
            }
            if cfg.end_time.is_some_and(|e| feed_time >= e) {
                break; // feed times never decrease
            }
            if paced {
                // A row is due (feed time since the first row) / speedup after the first row. The clock is read
                // only for rows not yet due at the last reading (~1.4 us per read on HPET hosts). Rows already
                // buffered are handed over before waiting, so nothing sits in a chunk while we sleep.
                let (first, from) = *origin.get_or_insert_with(|| (feed_time, Instant::now()));
                let due = from + Duration::from_secs_f64((feed_time - first) as f64 / 1000.0 / cfg.speedup);
                if due > now {
                    now = Instant::now();
                }
                if due > now + Duration::from_millis(1) {
                    flush(&mut chunks)?;
                    std::thread::sleep(due - now);
                    now = Instant::now();
                }
            }
            first_sent.get_or_insert_with(Instant::now);
            if cfg.uniform_symbols > 0 {
                // counted separately, so price events are spread evenly too
                let n = if is_price_row(&line) { &mut price_rows } else { &mut other_rows };
                line = with_symbol(&line, *n % cfg.uniform_symbols);
                *n += 1;
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
    let active = first_sent.map_or(0.0, |f| f.elapsed().as_secs_f64());
    Ok((read, symbols.len(), active))
}

/// Whether column `Last` (22nd field) is set.
fn is_price_row(line: &[u8]) -> bool {
    line.split(|&b| b == b',').nth(21).is_some_and(|last| !last.trim_ascii().is_empty())
}

/// The row with its symbol replaced by the n-th artificial one.
fn with_symbol(line: &[u8], n: u64) -> Vec<u8> {
    let rest = line.iter().position(|&b| b == b',').unwrap_or(line.len());
    let mut out = format!("U{n:05}.FR").into_bytes();
    out.extend_from_slice(&line[rest..]);
    out
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
/// With LATENCY_OUTPUT=summary it prints a cumulative `{"summary":...}` line instead, whenever new messages
/// have arrived and then none for `IDLE_POLLS` seconds, so printing never competes with receiving.
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
    let ready = now_ms();
    println!("{{\"ready_ms\":{ready}}}");
    let mut summary = match env("LATENCY_OUTPUT", "messages").as_str() {
        "messages" => None,
        "summary" => Some(Summary::new(ready)),
        other => return Err(format!("LATENCY_OUTPUT={other}: expected messages or summary").into()),
    };
    let mut idle = 0;
    loop {
        let Some(m) = consumer.poll(Duration::from_secs(1)) else {
            idle += 1;
            if let Some(s) = summary.as_mut().filter(|s| s.dirty && idle >= IDLE_POLLS) {
                s.dirty = false;
                println!("{}", s.json());
            }
            continue;
        };
        idle = 0;
        let m = m?;
        let received = now_ms();
        let (ts_type, ts) = match m.timestamp() {
            Timestamp::LogAppendTime(t) => ("LogAppendTime", t),
            Timestamp::CreateTime(t) => ("CreateTime", t),
            Timestamp::NotAvailable => ("none", -1),
        };
        let payload = std::str::from_utf8(m.payload().unwrap_or_default()).unwrap_or("null");
        if let Some(s) = summary.as_mut() {
            s.add(received, ts_type, ts, payload);
            continue;
        }
        // stdout is line-buffered, so each line is out as soon as it is written
        println!(
            "{{\"received_ms\":{received},\"kafka_ts\":{ts},\"ts_type\":\"{ts_type}\",\"partition\":{},\"offset\":{},\"advisory\":{payload}}}",
            m.partition(),
            m.offset()
        );
    }
}

/// Seconds without a message before a summary is printed. Shorter than the scripts wait for a stable output.
const IDLE_POLLS: u32 = 3;

/// Same stages and rules as bench/advisory_latency.py: repeats count once, messages stored before the consumer
/// was ready have no latency, and the first window close after the job start (cold start) is kept apart.
const STAGES: [&str; 4] = ["fired_to_emitted", "emitted_to_stored", "stored_to_received", "fired_to_received"];

struct Summary {
    ready: i64,
    dirty: bool,
    read: u64,
    unparsed: u64,
    repeats: u64,
    before_ready: u64,
    /// Hashes of (trader, symbol, window_end)
    seen: HashSet<u64>,
    traders: HashSet<String>,
    ts_types: BTreeSet<&'static str>,
    /// Messages per wall-clock second
    per_second: HashMap<i64, u64>,
    /// window_end (ISO local time, so it sorts as a string) → latencies in ms per stage
    closes: BTreeMap<String, [Vec<i64>; 4]>,
}

impl Summary {
    fn new(ready: i64) -> Self {
        Self {
            ready,
            dirty: false,
            read: 0,
            unparsed: 0,
            repeats: 0,
            before_ready: 0,
            seen: HashSet::new(),
            traders: HashSet::new(),
            ts_types: BTreeSet::new(),
            per_second: HashMap::new(),
            closes: BTreeMap::new(),
        }
    }

    fn add(&mut self, received: i64, ts_type: &'static str, stored: i64, advisory: &str) {
        self.dirty = true;
        self.read += 1;
        self.ts_types.insert(ts_type);
        *self.per_second.entry(received.div_euclid(1000)).or_default() += 1;
        let field = |key| json_str(advisory, key);
        let instant = |key| field(key).and_then(instant_ms);
        let (Some(trader), Some(symbol), Some(end), Some(fired), Some(emitted)) =
            (field("trader"), field("symbol"), field("window_end"), instant("fired_at"), instant("emitted_at"))
        else {
            self.unparsed += 1;
            return;
        };
        let mut h = DefaultHasher::new();
        (trader, symbol, end).hash(&mut h);
        if !self.seen.insert(h.finish()) {
            self.repeats += 1;
            return;
        }
        if !self.traders.contains(trader) {
            self.traders.insert(trader.to_string());
        }
        if stored < self.ready {
            self.before_ready += 1;
            return;
        }
        if !self.closes.contains_key(end) {
            self.closes.insert(end.to_string(), Default::default());
        }
        let stages = self.closes.get_mut(end).expect("inserted above");
        for (v, ms) in stages.iter_mut().zip([emitted - fired, stored - emitted, received - stored, received - fired]) {
            v.push(ms);
        }
    }

    fn json(&self) -> String {
        let first = self.closes.keys().next().cloned();
        let part = |cold: bool| {
            let closes: Vec<_> = self.closes.iter().filter(|(k, _)| (Some(*k) == first.as_ref()) == cold).collect();
            let messages: usize = closes.iter().map(|(_, s)| s[0].len()).sum();
            let stages: Vec<String> = (0..STAGES.len())
                .map(|i| {
                    let mut v: Vec<i64> = closes.iter().flat_map(|(_, s)| s[i].iter().copied()).collect();
                    v.sort_unstable();
                    let q = |p: f64| percentile(&v, p).map_or("null".to_string(), |x| x.to_string());
                    format!(
                        "\"{}\":{{\"p50\":{},\"p95\":{},\"p99\":{},\"max\":{}}}",
                        STAGES[i], q(50.0), q(95.0), q(99.0), q(100.0)
                    )
                })
                .collect();
            format!("{{\"closes\":{},\"messages\":{messages},\"ms\":{{{}}}}}", closes.len(), stages.join(","))
        };
        let ts_types: Vec<String> = self.ts_types.iter().map(|t| format!("\"{t}\"")).collect();
        format!(
            "{{\"summary\":{{\"read\":{},\"distinct\":{},\"repeats\":{},\"unparsed\":{},\"before_ready\":{},\"traders\":{},\"ts_types\":[{}],\"max_per_s\":{},\"steady\":{},\"cold\":{}}}}}",
            self.read, self.seen.len(), self.repeats, self.unparsed, self.before_ready, self.traders.len(),
            ts_types.join(","), self.per_second.values().max().copied().unwrap_or(0), part(false), part(true)
        )
    }
}

/// Nearest-rank percentile of sorted values.
fn percentile(sorted: &[i64], p: f64) -> Option<i64> {
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.max(1) - 1).copied()
}

/// Value of a string field of the flat advisory JSON (AdvisorySerializer). The fields read here (names,
/// times) never contain escapes, so no JSON library is needed.
fn json_str<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let start = json.find(&format!("\"{key}\":\""))? + key.len() + 4;
    let len = json[start..].find('"')?;
    Some(&json[start..start + len])
}

/// Java `Instant.toString()` with at most milliseconds ("2026-11-22T21:56:09.123Z") → epoch ms.
fn instant_ms(s: &str) -> Option<i64> {
    let (date, time) = s.strip_suffix('Z')?.split_once('T')?;
    let mut d = date.split('-').map(|x| x.parse::<i64>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    // days since 1970-01-01, counting years from March (leap day last)
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let days = 365 * y + y / 4 - y / 100 + y / 400 + (153 * m + 2) / 5 + day - 1 - 719_468;
    Some(days * 86_400_000 + clock_ms(time.as_bytes())?)
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
    use super::{Summary, instant_ms, is_price_row, java_string_hash, json_str, percentile, with_symbol};

    /// (symbol, Java `s.hashCode()`, Java `Math.floorMod(s.hashCode(), 4)`), produced with JDK 25.
    const JAVA: &[(&str, i32, i32)] = &[
        ("hello", 99162322, 2),
        ("IACWE.FR", -2065065823, 1),
        ("A2ASZ7.ETR", 528887029, 1),
        ("SGFI.FR", -1547068413, 3),
        ("ZZZZZZZZZZZZ.ETR", -236369387, 1), // overflows i32
    ];

    #[test]
    fn replaces_symbols_for_the_uniform_workload() {
        let price = b"TTE.FR,E,08-11-2021,09:00:00.000,,,,,,,,,,,,,,,,,,42.5,1,09:00:00.000,,,,";
        let other = b"TTE.FR,E,08-11-2021,09:00:00.000,,,,,,,,,,,,,,,,,,,,,,,,";
        assert!(is_price_row(price) && !is_price_row(other) && !is_price_row(b"TTE.FR,E"));
        assert!(with_symbol(price, 7).starts_with(b"U00007.FR,E,08-11-2021,09:00:00.000,"));
        assert!(is_price_row(&with_symbol(price, 7)));
    }

    #[test]
    fn parses_instants() {
        assert_eq!(instant_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(instant_ms("2026-11-22T21:56:09.123Z"), Some(1_795_384_569_123));
        assert_eq!(instant_ms("2024-02-29T12:00:00Z"), Some(1_709_208_000_000));
        assert_eq!(instant_ms("2024-03-01T00:00:00Z"), Some(1_709_251_200_000));
        assert_eq!(instant_ms("2026-11-22T21:56:09"), None);
    }

    #[test]
    fn nearest_rank_percentiles() {
        let v: Vec<i64> = (1..=100).collect();
        assert_eq!([50.0, 95.0, 99.0, 100.0].map(|p| percentile(&v, p)), [Some(50), Some(95), Some(99), Some(100)]);
        assert_eq!(percentile(&[7], 50.0), Some(7));
        assert_eq!(percentile(&[], 50.0), None);
    }

    #[test]
    fn summary_counts_repeats_once_and_separates_the_first_close() {
        let adv = |trader: &str, end: &str| {
            format!(
                "{{\"trader\":\"{trader}\",\"symbol\":\"TTE.FR\",\"type\":\"BUY\",\"window_end\":\"{end}\",\"close\":1.5,\
                 \"initial\":false,\"fired_at\":\"1970-01-01T00:00:01Z\",\"emitted_at\":\"1970-01-01T00:00:01.010Z\"}}"
            )
        };
        assert_eq!(json_str(&adv("t1", "e"), "window_end"), Some("e"));
        let mut s = Summary::new(1_000);
        s.add(1_030, "LogAppendTime", 1_020, &adv("t1", "2021-11-08T09:05:00"));
        s.add(1_050, "LogAppendTime", 1_020, &adv("t1", "2021-11-08T09:10:00"));
        s.add(1_060, "LogAppendTime", 1_020, &adv("t1", "2021-11-08T09:10:00")); // repeat
        s.add(1_060, "LogAppendTime", 999, &adv("t2", "2021-11-08T09:10:00")); // before ready
        s.add(1_060, "LogAppendTime", 1_020, "not json");
        let j = s.json();
        assert!(j.starts_with(
            "{\"summary\":{\"read\":5,\"distinct\":3,\"repeats\":1,\"unparsed\":1,\"before_ready\":1,\"traders\":2,"
        ), "{j}");
        assert!(j.contains("\"steady\":{\"closes\":1,\"messages\":1,\"ms\":{\"fired_to_emitted\":{\"p50\":10,"), "{j}");
        assert!(j.contains("\"fired_to_received\":{\"p50\":50,"), "{j}");
        assert!(j.contains("\"cold\":{\"closes\":1,\"messages\":1,"), "{j}");
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
