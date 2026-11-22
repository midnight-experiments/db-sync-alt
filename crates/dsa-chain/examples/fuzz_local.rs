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
    let cpu_mode = std::env::args().nth(2).as_deref() == Some("cpu");
    let cpu_start = cpu_time();
    let start = Instant::now();
    let mut measured = Duration::ZERO;
    let mut seed = if cpu_mode {
        0xe2f3_7739_514a_6bc1u64
    } else {
        0x51d4_a386_febc_0091u64
    };
    let mut iterations = 0u64;
    let mut accepted = 0u64;
    let mut next_log = 60;
    let mut raw = Vec::new();
    while measured < Duration::from_secs(seconds) {
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
        if iterations.is_multiple_of(1024) {
            measured = if cpu_mode {
                cpu_time() - cpu_start
            } else {
                start.elapsed()
            };
        }
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
        "complete measured_seconds={} wall_seconds={} cpu_mode={cpu_mode} iterations={iterations} accepted={accepted} seed={seed:016x} {rss}",
        measured.as_secs(),
        start.elapsed().as_secs()
    );
}

fn cpu_time() -> Duration {
    let record = std::fs::read_to_string("/proc/thread-self/schedstat")
        .expect("Linux thread runtime accounting");
    Duration::from_nanos(
        record
            .split_whitespace()
            .next()
            .expect("thread runtime")
            .parse()
            .expect("runtime nanoseconds"),
    )
}
