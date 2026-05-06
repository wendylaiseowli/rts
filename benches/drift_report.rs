use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::time::Duration;

#[derive(Clone, Copy)]
struct TaskTiming {
    expected_start: Duration,
    actual_start: Duration,
}

fn synthetic_samples(count: usize) -> Vec<TaskTiming> {
    (0..count)
        .map(|i| {
            let expected_start = Duration::from_micros((i as u64) * 10);
            let drift = Duration::from_micros((i % 7) as u64);
            let actual_start = expected_start + drift;

            TaskTiming {
                expected_start,
                actual_start,
            }
        })
        .collect()
}

fn build_drift_report(samples: &[TaskTiming]) -> String {
    if samples.is_empty() {
        return "TASK START REPORT count=0".to_string();
    }

    let mut total_expected = Duration::ZERO;
    let mut total_actual = Duration::ZERO;
    let mut total_drift = Duration::ZERO;
    let mut max_drift = Duration::ZERO;

    for sample in samples {
        let drift = sample.actual_start.saturating_sub(sample.expected_start);
        total_expected += sample.expected_start;
        total_actual += sample.actual_start;
        total_drift += drift;
        max_drift = max_drift.max(drift);
    }

    let count = samples.len() as f64;
    let avg_expected = Duration::from_secs_f64(total_expected.as_secs_f64() / count);
    let avg_actual = Duration::from_secs_f64(total_actual.as_secs_f64() / count);
    let avg_drift = Duration::from_secs_f64(total_drift.as_secs_f64() / count);

    format!(
        "TASK START REPORT count={} avg_expected_start={:?} avg_actual_start={:?} avg_scheduling_drift={:?} max_scheduling_drift={:?}",
        samples.len(),
        avg_expected,
        avg_actual,
        avg_drift,
        max_drift
    )
}

fn criterion_report(c: &mut Criterion) {
    let samples = synthetic_samples(1_000);

    c.bench_function("scheduling_drift_report", |b| {
        b.iter(|| {
            let report = build_drift_report(black_box(&samples));
            black_box(report)
        })
    });
}

criterion_group!(benches, criterion_report);
criterion_main!(benches);
