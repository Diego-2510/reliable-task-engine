# Synthetic PostgreSQL benchmark

Executed: 2026-11-13T22:04:36.738326+00:00. Commit: `33e83cf2e01fd22ce86128bfc142491e93e5e33e`; dirty: True. Smoke: False.

CPU: Intel(R) Core(TM) Ultra 7 255H (16 logical CPUs). Python 3.12.3; PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2). N=1000; seed=20261113; repetitions=3; warm-up=100 separately per configuration. Telemetry: disabled. One dispatcher for job_outbox.

Storage and full durability/environment settings: [manifest.json](manifest.json). Durable baseline uses fsync=on, synchronous_commit=on and a dedicated Docker volume.

## Measured results

Rates are median jobs/events per second. Latencies are pooled backlog-drain completion latencies in milliseconds, measured from a DB clock reference before barrier release. Outbox latency ends at delivery acknowledgement. These are not HTTP E2E latencies.

| Profile | Workers | Reps | Jobs/s | Ack/s | Job p50/p95/p99 ms | Outbox p50/p95/p99 ms | Job efficiency | Ack efficiency |
|---|---:|---:|---:|---:|---|---|---:|---:|
| job_only | 1 | 3 | 46.90 | — | 10308.56/20125.15/21198.98 | —/—/— | 1.00 | — |
| job_only | 2 | 3 | 93.47 | — | 5550.09/10202.07/10580.20 | —/—/— | 1.00 | — |
| job_only | 4 | 3 | 166.62 | — | 3006.85/5682.65/5916.77 | —/—/— | 0.89 | — |
| job_only | 8 | 3 | 316.81 | — | 1555.38/2991.65/3143.74 | —/—/— | 0.84 | — |
| job_outbox | 1 | 3 | 43.66 | 41.72 | 11263.14/21353.56/22664.80 | 12534.22/23138.84/24177.17 | 1.00 | 1.00 |
| job_outbox | 2 | 3 | 81.73 | 40.45 | 6128.19/11592.42/12079.92 | 13362.11/23882.03/25425.00 | 0.94 | 0.48 |
| job_outbox | 4 | 3 | 148.06 | 39.94 | 3366.80/6382.46/6653.57 | 13642.64/23520.66/24689.81 | 0.85 | 0.24 |
| job_outbox | 8 | 3 | 284.42 | 42.20 | 1808.72/3364.58/3553.95 | 12184.43/22559.65/23445.41 | 0.81 | 0.13 |

Efficiency = median rate / (one-worker median rate × worker count). Nearest-rank percentiles use ceil(q×n), with no interpolation. p99 is suppressed below 1000 samples, including tiny smoke datasets. Count, min, max and mean are in summary.json. Even 1000 observations provide only about ten upper-tail observations; no population-tail guarantee.

## Correctness and failures

Complete: **True**. Invalid runs: 0. Missing measured cases: 0.

- job_only, 1 workers: succeeded=3000, failed=0, delivered=0; all gates passed.
- job_only, 2 workers: succeeded=3000, failed=0, delivered=0; all gates passed.
- job_only, 4 workers: succeeded=3000, failed=0, delivered=0; all gates passed.
- job_only, 8 workers: succeeded=3000, failed=0, delivered=0; all gates passed.
- job_outbox, 1 workers: succeeded=3000, failed=0, delivered=3000; all gates passed.
- job_outbox, 2 workers: succeeded=3000, failed=0, delivered=3000; all gates passed.
- job_outbox, 4 workers: succeeded=3000, failed=0, delivered=3000; all gates passed.
- job_outbox, 8 workers: succeeded=3000, failed=0, delivered=3000; all gates passed.

## Interpretation and limitations

Synthetic handlers are effectively no-op work; no HTTP ingress, network downstream effect or production workload equivalence is measured. The receiver is another transaction on the same PostgreSQL instance. DB work may dominate scaling; one dispatcher may constrain outbox throughput. Cache warmth, WAL, I/O and CPU contention affect results. Bottleneck attribution requires profiling; the rates alone cannot prove which DB resource dominates.

Startup/imports and data loading are excluded. Throughput uses controller perf_counter until all expected terminal counts are confirmed; polling adds up to one polling interval plus query latency. DB timestamps precede their COMMIT and are wall-clock sensitive. The DB-reference-to-release RPC gap is included in latency and is not estimated away. Idle processes retain the runtime's 1s poll interval. Post-drain row validation is outside timing, and gates all published numbers.

No cold-cache reset, independent machine replication, randomization of all factors, confidence interval or storage saturation diagnosis is claimed. Repetition order rotates worker counts. Existing development services remain untouched, so shared host background activity is a possible confounder. Console comparison exports to the null sink and includes formatting/export work, not terminal rendering; cumulative telemetry snapshots are never used to calculate rates.
