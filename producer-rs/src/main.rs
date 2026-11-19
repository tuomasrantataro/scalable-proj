//! Replay producer and Iggy setup.
//!
//! Modes: `setup` (stream, topics, consumer group) and `replay` (send CSV rows). Partitioning: Java
//! `String.hashCode()` of the symbol, floor-mod partition count; this keeps the partition assignment of the
//! original Java producer (removed 2026-11-19), so MEASUREMENTS.md stays comparable. Send loop: one thread,
//! read `BATCH_SIZE` rows, then one synchronous send call per partition.

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{Duration, Instant};

use bytes::Bytes;
use iggy::prelude::*;

type Res<T> = Result<T, Box<dyn Error>>;

/// Time breakdown for `PROFILE=true`: building messages vs. awaiting `send_messages`.
static BUILD_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SEND_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Config {
    host: String,
    port: String,
    user: String,
    pass: String,
    stream: String,
    topic: String,
    group: String,
    partitions: u32,
    /// Output topic for BUY/SELL advisories (written by the Flink job), and its partition count.
    advisories_topic: String,
    advisories_partitions: u32,
    csv_file: String,
    repeat: u32,
    rate: u64,
    batch: usize,
    nodelay: bool,
    /// Diagnostics: print a time breakdown; skip the sends (measures reading + message building only).
    profile: bool,
    dry_run: bool,
}

impl Config {
    fn from_env() -> Res<Self> {
        Ok(Self {
            host: env("IGGY_HOST", "iggy"),
            port: env("IGGY_PORT", "8090"),
            user: env("IGGY_USERNAME", "iggy"),
            pass: env("IGGY_PASSWORD", "iggy"),
            stream: env("IGGY_STREAM", "trading"),
            topic: env("IGGY_TOPIC", "ticks"),
            group: env("IGGY_CONSUMER_GROUP", "flink"),
            partitions: env("IGGY_PARTITIONS", "1").parse()?,
            advisories_topic: env("IGGY_ADVISORIES_TOPIC", "advisories"),
            // 1 by default: the connector's sink partitions round-robin only, so more partitions could
            // reorder a symbol's advisories.
            advisories_partitions: env("IGGY_ADVISORIES_PARTITIONS", "1").parse()?,
            csv_file: env("CSV_FILE", "/data/pala.csv"),
            repeat: env("REPEAT", "1").parse()?,
            rate: env("RATE", "0").parse()?,
            batch: env("BATCH_SIZE", "4000").parse()?,
            // The Rust SDK leaves Nagle on by default; the Java SDK sets TCP_NODELAY=true.
            // With request/response sends, Nagle makes the producer ~3x slower (see MEASUREMENTS.md).
            nodelay: env("IGGY_TCP_NODELAY", "true").parse()?,
            profile: env("PROFILE", "false").parse()?,
            dry_run: env("DRY_RUN", "false").parse()?,
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

#[tokio::main]
async fn main() -> Res<()> {
    let cfg = Config::from_env()?;
    let mode = std::env::args().nth(1).unwrap_or_else(|| "replay".to_string());
    let client = connect(&cfg).await?;
    match mode.as_str() {
        "setup" => setup(&client, &cfg).await?,
        "replay" => replay(&client, &cfg).await?,
        other => return Err(format!("unknown mode: {other} (setup|replay)").into()),
    }
    client.disconnect().await?;
    Ok(())
}

async fn connect(cfg: &Config) -> Res<IggyClient> {
    let url = format!(
        "iggy://{}:{}@{}:{}?nodelay={}",
        cfg.user, cfg.pass, cfg.host, cfg.port, cfg.nodelay
    );
    let mut attempt = 1;
    loop {
        let result = async {
            let client = IggyClient::from_connection_string(&url)?;
            client.connect().await?;
            // a cheap authenticated call, so login problems surface here and not mid-replay
            client.get_streams().await?;
            Ok::<_, IggyError>(client)
        }
        .await;
        match result {
            Ok(c) => {
                log!("connected to {}:{} (tcp nodelay={})", cfg.host, cfg.port, cfg.nodelay);
                return Ok(c);
            }
            Err(e) if attempt < 30 => {
                log!("iggy not ready ({e}), retrying...");
                tokio::time::sleep(Duration::from_secs(1)).await;
                attempt += 1;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn setup(client: &IggyClient, cfg: &Config) -> Res<()> {
    let stream = Identifier::named(&cfg.stream)?;
    let topic = Identifier::named(&cfg.topic)?;

    // Some lookups return None, others an error on "not found": treat both as absent.
    if !matches!(client.get_stream(&stream).await, Ok(Some(_))) {
        client.create_stream(&cfg.stream, &StreamCreateOptions::default()).await?;
        log!("created stream {}", cfg.stream);
    }
    ensure_topic(client, &stream, &cfg.topic, cfg.partitions).await?;
    let group = Identifier::named(&cfg.group)?;
    if !matches!(client.get_consumer_group(&stream, &topic, &group).await, Ok(Some(_))) {
        client.create_consumer_group(&stream, &topic, &cfg.group).await?;
        log!("created consumer group {}", cfg.group);
    }
    ensure_topic(client, &stream, &cfg.advisories_topic, cfg.advisories_partitions).await?;
    for name in [&cfg.topic, &cfg.advisories_topic] {
        let details = client
            .get_topic(&stream, &Identifier::named(name)?)
            .await?
            .ok_or("topic missing after setup")?;
        log!("setup done: topic {} with {} partitions", details.name, details.partitions_count);
    }
    Ok(())
}

/// Creates the topic if it does not exist. An existing topic is kept as is, even with another partition count.
async fn ensure_topic(client: &IggyClient, stream: &Identifier, name: &str, partitions: u32) -> Res<()> {
    if !matches!(client.get_topic(stream, &Identifier::named(name)?).await, Ok(Some(_))) {
        let options = TopicCreateOptions {
            partitions_count: Some(partitions),
            ..Default::default()
        };
        client.create_topic(stream, name, &options).await?;
        log!("created topic {} with {} partitions", name, partitions);
    }
    Ok(())
}

async fn replay(client: &IggyClient, cfg: &Config) -> Res<()> {
    let stream = Identifier::named(&cfg.stream)?;
    let topic = Identifier::named(&cfg.topic)?;
    let partitions = client
        .get_topic(&stream, &topic)
        .await?
        .ok_or("topic not found, run setup first")?
        .partitions_count;

    let start = Instant::now();
    let mut sent: u64 = 0;
    let mut symbols: HashSet<Vec<u8>> = HashSet::new();
    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(cfg.batch);

    for _ in 0..cfg.repeat {
        let mut reader = BufReader::with_capacity(1 << 20, File::open(&cfg.csv_file)?);
        let mut line = Vec::with_capacity(256);
        loop {
            line.clear();
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
            batch.push(line.clone());
            if batch.len() >= cfg.batch {
                sent += flush(client, cfg, &stream, &topic, partitions, &mut batch, &mut symbols).await?;
                pace(start, sent, cfg.rate).await;
            }
        }
        sent += flush(client, cfg, &stream, &topic, partitions, &mut batch, &mut symbols).await?;
    }

    log!(
        "replay done: sent {} messages ({} distinct symbols) in {:.2} s",
        sent,
        symbols.len(),
        start.elapsed().as_secs_f64()
    );
    if cfg.profile {
        use std::sync::atomic::Ordering::Relaxed;
        let total = start.elapsed().as_secs_f64();
        let build = BUILD_NS.load(Relaxed) as f64 / 1e9;
        let send = SEND_NS.load(Relaxed) as f64 / 1e9;
        log!(
            "profile: total {total:.2} s = build messages {build:.2} s + send_messages {send:.2} s + read/other {:.2} s{}",
            total - build - send,
            if cfg.dry_run { " (DRY_RUN: nothing sent)" } else { "" }
        );
    }
    Ok(())
}

/// One send call per partition.
async fn flush(
    client: &IggyClient,
    cfg: &Config,
    stream: &Identifier,
    topic: &Identifier,
    partitions: u32,
    lines: &mut Vec<Vec<u8>>,
    symbols: &mut HashSet<Vec<u8>>,
) -> Res<u64> {
    use std::sync::atomic::Ordering::Relaxed;
    let t0 = Instant::now();
    let n = lines.len() as u64;
    // One clock read per batch, not per message. `IggyMessage::from`/the builder calls
    // `IggyTimestamp::now()` for every message, which costs ~1.4 us when the kernel clock source is
    // HPET (TSC marked unstable, as on the laptop in MEASUREMENTS.md): 80 s for one trading day.
    // The Java SDK sends origin_timestamp = 0 and never reads the clock.
    let origin_timestamp = IggyTimestamp::now().as_micros();
    let mut by_partition: BTreeMap<u32, Vec<IggyMessage>> = BTreeMap::new();
    for l in lines.drain(..) {
        let symbol = &l[..l.iter().position(|&b| b == b',').unwrap_or(l.len())];
        let p = java_string_hash(symbol).rem_euclid(partitions as i32) as u32;
        if !symbols.contains(symbol) {
            symbols.insert(symbol.to_vec());
        }
        let header = IggyMessageHeader {
            origin_timestamp,
            payload_length: l.len() as u32,
            ..Default::default() // id 0: the SDK assigns a random id when encoding, as with the builder
        };
        let message = IggyMessage { header, payload: Bytes::from(l), user_headers: None };
        by_partition.entry(p).or_default().push(message);
    }
    let t1 = Instant::now();
    BUILD_NS.fetch_add((t1 - t0).as_nanos() as u64, Relaxed);
    if !cfg.dry_run {
        for (p, mut messages) in by_partition {
            client
                .send_messages(stream, topic, &Partitioning::partition_id(p), &mut messages)
                .await?;
        }
    }
    SEND_NS.fetch_add(t1.elapsed().as_nanos() as u64, Relaxed);
    Ok(n)
}

/// Java's `String.hashCode()` (s[0]*31^(n-1) + ... + s[n-1], wrapping i32, over UTF-16 units),
/// so each symbol goes to the same partition as with the original Java producer.
fn java_string_hash(bytes: &[u8]) -> i32 {
    String::from_utf8_lossy(bytes)
        .encode_utf16()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

async fn pace(start: Instant, sent: u64, rate: u64) {
    if rate == 0 {
        return;
    }
    let target = start + Duration::from_secs_f64(sent as f64 / rate as f64);
    let now = Instant::now();
    if target > now {
        tokio::time::sleep(target - now).await;
    }
}

#[cfg(test)]
mod tests {
    use super::java_string_hash;

    /// (symbol, Java `s.hashCode()`, Java `Math.floorMod(s.hashCode(), 4)`), produced with JDK 25.
    const JAVA: &[(&str, i32, u32)] = &[
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
            assert_eq!(java_string_hash(s.as_bytes()).rem_euclid(4) as u32, partition, "{s}");
        }
    }
}
