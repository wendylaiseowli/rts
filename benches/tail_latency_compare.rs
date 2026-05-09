use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::mpsc as std_mpsc;
use std::sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tokio::runtime::Builder;
use tokio::sync::mpsc as tokio_mpsc;

const CHANNEL_CAPACITY: usize = 32;
const BURST_SIZE: usize = 256;
const BURST_COUNT: usize = 24;
const QUIET_GAP: Duration = Duration::from_millis(3);
const PROCESSING_SPIN: u64 = 2_000;
static PRINTED_THREAD: AtomicBool = AtomicBool::new(false);
static PRINTED_ASYNC: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy)]
struct LatencySample {
    latency: Duration,
}

#[derive(Clone, Copy)]
struct Summary {
    count: usize,
    mean_ns: u128,
    p50_ns: u128,
    p95_ns: u128,
    p99_ns: u128,
    max_ns: u128,
}

fn simulate_hot_path() {
    let mut acc = 0_u64;
    for i in 0..PROCESSING_SPIN {
        acc = acc.wrapping_add(i ^ (acc.rotate_left(3)));
    }
    black_box(acc);
}

fn run_threaded_spike_load() -> Vec<LatencySample> {
    let (tx, rx) = std_mpsc::sync_channel::<Instant>(CHANNEL_CAPACITY);
    let samples = Arc::new(Mutex::new(Vec::with_capacity(BURST_SIZE * BURST_COUNT)));
    let samples_for_worker = Arc::clone(&samples);

    let worker_handle = thread::spawn(move || {
        while let Ok(sent_at) = rx.recv() {
            simulate_hot_path();
            let latency = sent_at.elapsed();
            samples_for_worker
                .lock()
                .unwrap()
                .push(LatencySample { latency });
        }
    });

    for spike in 0..BURST_COUNT {
        for _ in 0..BURST_SIZE {
            tx.send(Instant::now()).unwrap();
        }
        if spike + 1 != BURST_COUNT {
            thread::sleep(QUIET_GAP);
        }
    }

    drop(tx);
    let _ = worker_handle.join();

    let mut guard = samples.lock().unwrap();
    guard.sort_unstable_by_key(|sample| sample.latency.as_nanos());
    std::mem::take(&mut *guard)
}

async fn run_async_spike_load() -> Vec<LatencySample> {
    let (tx, mut rx) = tokio_mpsc::channel::<Instant>(CHANNEL_CAPACITY);
    let samples = Arc::new(Mutex::new(Vec::with_capacity(BURST_SIZE * BURST_COUNT)));
    let samples_for_worker = Arc::clone(&samples);

    let worker = tokio::spawn(async move {
        while let Some(sent_at) = rx.recv().await {
            simulate_hot_path();
            let latency = sent_at.elapsed();
            samples_for_worker
                .lock()
                .unwrap()
                .push(LatencySample { latency });
        }
    });

    for spike in 0..BURST_COUNT {
        for _ in 0..BURST_SIZE {
            tx.send(Instant::now()).await.unwrap();
        }
        if spike + 1 != BURST_COUNT {
            tokio::time::sleep(QUIET_GAP).await;
        }
    }

    drop(tx);
    let _ = worker.await;

    let mut guard = samples.lock().unwrap();
    guard.sort_unstable_by_key(|sample| sample.latency.as_nanos());
    std::mem::take(&mut *guard)
}

fn summarize(samples: &[LatencySample]) -> Summary {
    let count = samples.len();
    let mut values: Vec<u128> = samples
        .iter()
        .map(|sample| sample.latency.as_nanos())
        .collect();
    values.sort_unstable();

    let sum: u128 = values.iter().copied().sum();
    let mean_ns = if count == 0 { 0 } else { sum / count as u128 };
    let p50_ns = percentile(&values, 50);
    let p95_ns = percentile(&values, 95);
    let p99_ns = percentile(&values, 99);
    let max_ns = values.last().copied().unwrap_or(0);

    Summary {
        count,
        mean_ns,
        p50_ns,
        p95_ns,
        p99_ns,
        max_ns,
    }
}

fn percentile(values: &[u128], percentile: u32) -> u128 {
    if values.is_empty() {
        return 0;
    }

    let rank = ((percentile as usize * values.len()) + 99) / 100;
    let idx = rank.saturating_sub(1).min(values.len() - 1);
    values[idx]
}

fn print_report(architecture: &str, summary: Summary) {
    println!(
        "{architecture},count={},mean_ns={},p50_ns={},p95_ns={},p99_ns={},max_ns={}",
        summary.count,
        summary.mean_ns,
        summary.p50_ns,
        summary.p95_ns,
        summary.p99_ns,
        summary.max_ns
    );
}

fn tail_latency_compare(c: &mut Criterion) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .max(2),
        )
        .enable_time()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("tail_latency_compare");

    group.bench_function("threaded_spike_tail", |b| {
        b.iter(|| {
            let samples = run_threaded_spike_load();
            let summary = summarize(&samples);
            if !PRINTED_THREAD.swap(true, Ordering::SeqCst) {
                print_report("threaded", summary);
            }
            black_box(summary.p99_ns)
        })
    });

    group.bench_function("async_spike_tail", |b| {
        b.iter(|| {
            let samples = runtime.block_on(run_async_spike_load());
            let summary = summarize(&samples);
            if !PRINTED_ASYNC.swap(true, Ordering::SeqCst) {
                print_report("async", summary);
            }
            black_box(summary.p99_ns)
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(20));
    targets = tail_latency_compare
}
criterion_main!(benches);
