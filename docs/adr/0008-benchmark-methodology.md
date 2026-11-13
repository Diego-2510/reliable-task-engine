# ADR 0008: Reproducible synthetic backlog-drain benchmarks

Status: Accepted for phase 3B

## Decision

Keep a standalone repository-local `benchmarks/` package, invoked using
`uv run python -m benchmarks`. Use Python standard library, SQLAlchemy/psycopg,
Alembic and pytest already in the lockfile; add no dependency or benchmark service.
Production workers, dispatcher, schemas and transitions are unchanged.

Benchmark adapters spawn fresh OS processes, set explicit database and telemetry
configuration before importing runtime modules, call `initialize` once, establish
a DB connection and report readiness. A common event releases every worker and,
for job_outbox, exactly one dispatcher. Each process repeatedly calls the unchanged
runtime `run_once()`, retaining the one-second idle polling interval. Imports,
connections and bulk synthetic SQL submission are outside timed intervals. The
bulk insert bypasses HTTP validation and submission idempotency deliberately:
this measures prepopulated backlog draining, not ingress or HTTP E2E latency.

Profiles are `job_only` and `job_outbox`. Both succeed on their first attempt;
the latter requests one fixed synthetic event per job. The optional
`permanent_failure_5pct` deterministically chooses floor(N/20) permanent failures
using existing `fail=true` functionality; it is not a transient-retry benchmark.
A fixed seed produces deterministic UUIDs and fixed-width synthetic payload data.
Per-repetition cleanup plus primary-key inserts prevent duplicate submissions.

The standard matrix uses workers 1, 2, 4, 8, N=1000 and three measured repetitions
per profile/configuration. A separate 100-job warm-up runs once per configuration
using separate processes/datasets; it warms shared PostgreSQL/OS state, not each
measured interpreter's SQL compilation caches. Every measured process still
preconnects before readiness. Worker order rotates across repetitions. Hardware,
storage, payloads, seed, lease duration and PG settings remain constant. Smoke
uses 20 jobs, five warm-up jobs and one repetition, solely to check functionality.

## Safety and isolation

Require `BENCHMARK_DATABASE=1`, explicit `BENCHMARK_DATABASE_URL`,
`postgresql+psycopg`, an explicit host, no query-string connection overrides and
an alphanumeric/underscore database name ending in `_test`. Never fall back to
`DATABASE_URL`. Verify actual `current_database()` before adoption, ownership
checks or destructive cleanup. The authorization variables explicitly designate
a disposable benchmark database; the suffix alone does not establish disposability.

`prepare` accepts only an empty database before creating a benchmark ownership
marker and running initial migrations. Nonempty unmarked databases are refused.
An owned database already at revision 0007 may be reused; upgrades of an existing
schema are refused. Interrupted initialization requires inspection/recreation of
this dedicated database, not automatic adoption of unknown application state.
The marker lives outside production schema migrations. No migrations are changed.

A session advisory lock excludes competing suite controllers. Refuse other client
connections before a run; this also catches an accidentally shared test database.
Controller statements/locks have five-second timeouts, connections a five-second
connect timeout. Destructive cleanup truncates only the three synthetic runtime
tables with no CASCADE, after identity and ownership checks. Spawned processes
are tracked, cooperatively stopped and joined, then terminated/killed if needed.
Exceptions during startup clean previously started processes. Unexpected child
exit, nonzero shutdown, workload timeout or invariant failure invalidates a run.
Keyboard interruption cleans processes/data and exits 130. Incomplete case lists
remain explicit in saved summaries. Cleanup failures exit nonzero.

Official measurements require a labeled dedicated persistent Docker volume using
the local driver without mount options. Reject tmpfs/ramfs and unverified mounts.
Inspect Docker mount/volume metadata and the in-container filesystem type; compare
PostgreSQL's actual cluster identifier with the inspected container's cluster.
Require fsync, synchronous_commit and full_page_writes on. Record version, image
identity, durability, storage and relevant settings. This verifies configuration,
not hardware guarantees about cache flushes or power-loss survival. Never remove
unknown volumes. A separate database holds destructive regression tests.

## Timing, statistics and correctness

Immediately before release, fetch PostgreSQL `clock_timestamp()` as the common
latency origin, sample controller `perf_counter()` and set the start event. Job
`updated_at` in terminal state is the existing DB-time terminalization timestamp;
outbox `delivered_at` marks acknowledgement. Subtract the common DB start to
obtain **backlog-drain completion latency** and **outbox completion latency**.
These include queue waiting. Outbox latency ends at ack, not receiver insertion;
it is not ack-minus-job duration. Timestamps precede COMMIT and may be affected
by wall-clock changes. Negative or causally inconsistent samples invalidate runs.
No cross-clock subtraction or interpolation corrects the DB-reference/RPC gap.

Controller monotonic intervals end when aggregate SQL queries confirm exactly N
terminal jobs and, separately, exactly N delivered events. Rates are N divided
by their respective elapsed intervals, including polling/query/commit visibility
cost. Poll every 20ms; terminal confirmation has polling and query overhead.
Post-drain row verification occurs after processes stop and outside timing.
Timing does not include setup, startup, teardown or report generation.

Validate exact expected job UUID/payload sets and counts; expected success/failure,
one attempt, original budgets, no error/retry metadata except expected permanent
failures, no active leases or submission keys. job_only must have no events/effects.
Outbox requires exactly N distinct committed event IDs, one per job, delivered
state, one delivery attempt, exact fixed payload/type, no dead letters/replays or
active delivery leases. Receiver rows must match the exact event IDs/job IDs/types
and timestamp causality. PK uniqueness and explicit set checks guard duplicates.
A terminal counter alone never establishes validity. Invalid runs publish fixed
failure codes and no throughput/latencies. Fail fast rather than continuing a
potentially contaminated matrix.

Nearest-rank percentiles select sorted element ceil(q*N), without interpolation.
Report count, p50, p95, p99, min, max and arithmetic mean in milliseconds. Suppress
p99 below 1000 samples; tiny smoke runs cannot establish tail behavior. Even
1000 observations provide only about ten observations above p99. Summary latency
statistics pool equal-size measured repetitions per configuration. Throughput is
the median of per-run rates, never a sum of rates. Scaling efficiency is median
rate divided by (one-worker median rate × worker count), separately for jobs/ack.
Raw per-run samples are rounded to microsecond resolution in compact CSV;
summaries are regenerated from those exact CSV values, with no missing-result
imputation or performance threshold. Three repetitions do not establish a
confidence interval, stationary tail behavior or cross-machine reproducibility.

## Telemetry and evidence

Baseline is `TASK_ENGINE_TELEMETRY=disabled`. Disable application console logging.
An optional controlled comparison enables console exporters with stdout redirected
to the null sink, preserving instrumentation/export formatting cost while avoiding
terminal/rendering throughput dependence. Orderly provider shutdown remains outside
timing. This does not represent terminal I/O overhead. Never derive performance
or correctness from cumulative telemetry snapshots.

Each new result directory contains an allowlisted environment manifest, per-run
CSV (including warm-ups), measured sample CSV, deterministic JSON summary and
Markdown report. Reject existing output directories. Record UTC execution time,
commit/dirty state and a source/config fingerprint for dirty-tree provenance;
OS/kernel, CPU, logical count, physical/available memory, Python/uv, PG/Docker/
Compose versions, storage/durability, process counts, N, seed, leases, telemetry,
warm-up method, repetitions and timeouts. Never save credentials, DB URLs,
private hostnames, ambient environment, arbitrary exception text or raw payloads.

## Limits and follow-up

No HTTP/network downstream effect is measured. Synthetic handlers are effectively
no-op work. The receiver is another transaction on the same PG instance. The DB
may dominate scaling; one dispatcher may constrain outbox throughput. Cache,
WAL, I/O, CPU contention, scheduler/power/thermal changes and unrelated host
activity affect results. No production workload equivalence is claimed.
Bottleneck attribution needs profiling, not rates alone. No cold-cache reset,
CPU affinity, full factorial randomization or independent-machine replication is
claimed. Initial/final host observations and shared-service limitations accompany
local measurements. See [benchmark guide](../benchmarks.md) and
[validation evidence](../phase-3b-validation.md).
