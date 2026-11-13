# Synthetic PostgreSQL benchmark

Executed: 2026-11-13T22:12:38.281006+00:00. Commit: `33e83cf2e01fd22ce86128bfc142491e93e5e33e`; dirty: True. Smoke: False.

CPU: Intel(R) Core(TM) Ultra 7 255H (16 logical CPUs). Python 3.12.3; PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2). N=1000; seed=20261113; repetitions=3; warm-up=100 separately per configuration. Telemetry: disabled. One dispatcher for job_outbox.

Storage and full durability/environment settings: [manifest.json](manifest.json). Durable baseline uses fsync=on, synchronous_commit=on and a dedicated Docker volume.

## Measured results

Rates are median jobs/events per second. Latencies are pooled backlog-drain completion latencies in milliseconds, measured from a DB clock reference before barrier release. Outbox latency ends at delivery acknowledgement. These are not HTTP E2E latencies.

| Profile | Workers | Reps | Jobs/s | Ack/s | Job p50/p95/p99 ms | Outbox p50/p95/p99 ms | Job efficiency | Ack efficiency |
|---|---:|---:|---:|---:|---|---|---:|---:|
| job_outbox | 2 | 3 | 82.83 | 41.63 | 5982.93/11481.87/11969.42 | 13604.34/23024.48/23833.93 | — | — |

Efficiency = median rate / (one-worker median rate × worker count). Nearest-rank percentiles use ceil(q×n), with no interpolation. p99 is suppressed below 1000 samples, including tiny smoke datasets. Count, min, max and mean are in summary.json. Even 1000 observations provide only about ten upper-tail observations; no population-tail guarantee.

## Correctness and failures

Complete: **True**. Invalid runs: 0. Missing measured cases: 0.

- job_outbox, 2 workers: succeeded=3000, failed=0, delivered=3000; all gates passed.

## Interpretation and limitations

Synthetic handlers are effectively no-op work; no HTTP ingress, network downstream effect or production workload equivalence is measured. The receiver is another transaction on the same PostgreSQL instance. DB work may dominate scaling; one dispatcher may constrain outbox throughput. Cache warmth, WAL, I/O and CPU contention affect results. Bottleneck attribution requires profiling; the rates alone cannot prove which DB resource dominates.

Startup/imports and data loading are excluded. Throughput uses controller perf_counter until all expected terminal counts are confirmed; polling adds up to one polling interval plus query latency. DB timestamps precede their COMMIT and are wall-clock sensitive. The DB-reference-to-release RPC gap is included in latency and is not estimated away. Idle processes retain the runtime's 1s poll interval. Post-drain row validation is outside timing, and gates all published numbers.

No cold-cache reset, independent machine replication, randomization of all factors, confidence interval or storage saturation diagnosis is claimed. Repetition order rotates worker counts. Existing development services remain untouched, so shared host background activity is a possible confounder. Console comparison exports to the null sink and includes formatting/export work, not terminal rendering; cumulative telemetry snapshots are never used to calculate rates.
