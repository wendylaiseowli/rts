# Tail Latency Comparison

Use the Criterion benchmark below to compare the async and threaded spike workloads:

```bash
cargo bench --bench tail_latency_compare
```

The benchmark reports:

- `count`
- `mean_ns`
- `p50_ns`
- `p95_ns`
- `p99_ns`
- `max_ns`

For the Distinction requirement, the key figure is `p99_ns` under the same spike pattern. In your write-up, show the workload design, then compare the p99 values directly and explain the likely reason for the difference in terms of scheduler overhead, wake-up behavior, and queue pressure.
