//! Bounded defensive arbitrary-input exercise for our structural parser only.
//! No third-party decoder, transaction renderer or network client is invoked.

use std::time::{Duration, Instant};

use dsa_chain::cbor::Item;

fn main() {
    let seconds: u64 = std::env::args()
        .nth(1)
        .expect("duration in seconds")
        .parse()
        .expect("numeric duration");
    assert!((1..=3600).contains(&seconds));
    let start = Instant::now();
    let mut seed = 0x51d4_a386_febc_0091u64;
    let mut iterations = 0u64;
    let mut accepted = 0u64;
    let mut next_log = 60;
    let mut raw = Vec::new();
    while start.elapsed() < Duration::from_secs(seconds) {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let length = (seed as usize) % 65537;
        raw.clear();
        raw.extend((0..length).map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        }));
        if Item::parse(&raw).is_ok() {
            accepted += 1;
        }
        iterations += 1;
        if start.elapsed().as_secs() >= next_log {
            println!(
                "layer=own_structural_parser elapsed_seconds={} iterations={iterations} accepted={accepted}",
                start.elapsed().as_secs()
            );
            next_log += 60;
        }
    }
    let rss = std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .unwrap_or("VmHWM: unavailable")
        .to_owned();
    println!(
        "complete elapsed_seconds={} iterations={iterations} accepted={accepted} seed={seed:016x} {rss}",
        start.elapsed().as_secs()
    );
}
