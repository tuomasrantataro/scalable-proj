//! Times each step of the producer's per-row loop. `cargo run --release --example microbench`
use std::collections::{BTreeMap, HashSet};
use std::hint::black_box;
use std::time::Instant;
use iggy::prelude::*;

fn java_string_hash(bytes: &[u8]) -> i32 {
    String::from_utf8_lossy(bytes).encode_utf16().fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

fn main() {
    const N: usize = 2_000_000;
    let line = b"A2ASZ7.ETR,E,08-11-2021,01:01:09.564,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,".to_vec();
    let time = |name: &str, f: &mut dyn FnMut()| {
        let t = Instant::now();
        for _ in 0..N { f(); }
        println!("{name:<32} {:>7.1} ns/row", t.elapsed().as_nanos() as f64 / N as f64);
    };
    time("clone line (Vec)", &mut || { black_box(line.clone()); });
    time("find symbol", &mut || { black_box(line.iter().position(|&b| b == b',')); });
    time("java hash", &mut || { black_box(java_string_hash(&line[..10])); });
    let mut set: HashSet<Vec<u8>> = HashSet::new(); set.insert(line[..10].to_vec());
    time("HashSet contains", &mut || { black_box(set.contains(&line[..10])); });
    time("IggyTimestamp::now", &mut || { black_box(IggyTimestamp::now()); });
    time("IggyMessage::from(Vec)", &mut || { black_box(IggyMessage::from(line.clone())); });
    let mut m: BTreeMap<u32, Vec<IggyMessage>> = BTreeMap::new();
    let mut i = 0u32;
    time("BTreeMap push", &mut || { i += 1; if i % 4000 == 0 { m.clear(); } m.entry(i % 4).or_default().push(IggyMessage::from(line.clone())); });
}
