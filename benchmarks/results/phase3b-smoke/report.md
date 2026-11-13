# Synthetic PostgreSQL benchmark

Executed: 2026-11-13T21:59:31.686512+00:00. Commit: `33e83cf2e01fd22ce86128bfc142491e93e5e33e`; dirty: True. Smoke: True.

CPU: Intel(R) Core(TM) Ultra 7 255H (16 logical CPUs). Python 3.12.3; PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2). N=20; seed=20261113; repetitions=1; warm-up=5 separately per configuration. Telemetry: disabled. One dispatcher for job_outbox.

Storage and full durability/environment settings: [manifest.json](manifest.json). Durable baseline uses fsync=on, synchronous_commit=on and a dedicated Docker volume.

## Measured results

Rates are median jobs/events per second. Latencies are pooled backlog-drain completion latencies in milliseconds, measured from a DB clock reference before barrier release. Outbox latency ends at delivery acknowledgement. These are not HTTP E2E latencies.

| Profile | Workers | Reps | Jobs/s | Ack/s | Job p50/p95/p99 ms | Outbox p50/p95/p99 ms | Job efficiency | Ack efficiency |
|---|---:|---:|---:|---:|---|---|---:|---:|
| job_only | 1 | 1 | 58.40 | — | 194.23/319.99/— | —/—/— | 1.00 | — |
| job_only | 2 | 1 | 83.43 | — | 107.31/214.81/— | —/—/— | 0.71 | — |
| job_only | 4 | 1 | 152.95 | — | 76.87/118.43/— | —/—/— | 0.65 | — |
| job_only | 8 | 1 | 177.79 | — | 68.81/97.11/— | —/—/— | 0.38 | — |
| job_outbox | 1 | 1 | 38.19 | 11.68 | 263.74/484.88/— | 1485.98/1670.87/— | 1.00 | 1.00 |
| job_outbox | 2 | 1 | 65.97 | 12.81 | 147.90/292.90/— | 1307.35/1526.41/— | 0.86 | 0.55 |
| job_outbox | 4 | 1 | 101.23 | 13.32 | 105.05/165.35/— | 1276.19/1468.88/— | 0.66 | 0.29 |
| job_outbox | 8 | 1 | 75.37 | 12.16 | 211.46/240.58/— | 1295.84/1599.51/— | 0.25 | 0.13 |

Efficiency = median rate / (one-worker median rate × worker count). Nearest-rank percentiles use ceil(q×n), with no interpolation. p99 is suppressed below 1000 samples, including tiny smoke datasets. Count, min, max and mean are in summary.json. Even 1000 observations provide only about ten upper-tail observations; no population-tail guarantee.

## Correctness and failures

Complete: **True**. Invalid runs: 0. Missing measured cases: 0.

- job_only, 1 workers: succeeded=20, failed=0, delivered=0; all gates passed.
- job_only, 2 workers: succeeded=20, failed=0, delivered=0; all gates passed.
- job_only, 4 workers: succeeded=20, failed=0, delivered=0; all gates passed.
- job_only, 8 workers: succeeded=20, failed=0, delivered=0; all gates passed.
- job_outbox, 1 workers: succeeded=20, failed=0, delivered=20; all gates passed.
- job_outbox, 2 workers: succeeded=20, failed=0, delivered=20; all gates passed.
- job_outbox, 4 workers: succeeded=20, failed=0, delivered=20; all gates passed.
- job_outbox, 8 workers: succeeded=20, failed=0, delivered=20; all gates passed.

## Interpretation and limitations

Synthetic handlers are effectively no-op work; no HTTP ingress, network downstream effect or production workload equivalence is measured. The receiver is another transaction on the same PostgreSQL instance. DB work may dominate scaling; one dispatcher may constrain outbox throughput. Cache warmth, WAL, I/O and CPU contention affect results. Bottleneck attribution requires profiling; the rates alone cannot prove which DB resource dominates.

Startup/imports and data loading are excluded. Throughput uses controller perf_counter until all expected terminal counts are confirmed; polling adds up to one polling interval plus query latency. DB timestamps precede their COMMIT and are wall-clock sensitive. The DB-reference-to-release RPC gap is included in latency and is not estimated away. Idle processes retain the runtime's 1s poll interval. Post-drain row validation is outside timing, and gates all published numbers.

No cold-cache reset, independent machine replication, randomization of all factors, confidence interval or storage saturation diagnosis is claimed. Repetition order rotates worker counts. Existing development services remain untouched, so shared host background activity is a possible confounder. Console comparison exports to the null sink and includes formatting/export work, not terminal rendering; cumulative telemetry snapshots are never used to calculate rates.
