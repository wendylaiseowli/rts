use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

const UPDATE_BATCHES: u64 = 50_000;

fn worker_count() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(4)
}

fn run_mutex_workload() -> u64 {
    let shared = Arc::new(Mutex::new(0_u64));
    let mut handles = Vec::new();

    for _ in 0..worker_count() {
        let shared = Arc::clone(&shared);
        handles.push(thread::spawn(move || {
            for _ in 0..UPDATE_BATCHES {
                let mut guard = shared.lock().unwrap();
                *guard += 1;
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    *shared.lock().unwrap()
}

fn run_rwlock_workload() -> u64 {
    let shared = Arc::new(RwLock::new(0_u64));
    let mut handles = Vec::new();

    for _ in 0..worker_count() {
        let shared = Arc::clone(&shared);
        handles.push(thread::spawn(move || {
            for _ in 0..UPDATE_BATCHES {
                let mut guard = shared.write().unwrap();
                *guard += 1;
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    *shared.read().unwrap()
}

fn run_atomic_workload() -> u64 {
    let shared = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for _ in 0..worker_count() {
        let shared = Arc::clone(&shared);
        handles.push(thread::spawn(move || {
            for _ in 0..UPDATE_BATCHES {
                shared.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    shared.load(Ordering::Relaxed)
}

fn sync_contention_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention");

    group.bench_function("mutex", |b| {
        b.iter(|| {
            let total = run_mutex_workload();
            black_box(total)
        })
    });

    group.bench_function("rwlock", |b| {
        b.iter(|| {
            let total = run_rwlock_workload();
            black_box(total)
        })
    });

    group.bench_function("atomic", |b| {
        b.iter(|| {
            let total = run_atomic_workload();
            black_box(total)
        })
    });

    group.finish();
}

criterion_group!(benches, sync_contention_bench);
criterion_main!(benches);
