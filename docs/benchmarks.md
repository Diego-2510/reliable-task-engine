# Reproducing the phase 3B benchmarks

This suite measures synthetic PostgreSQL backlog draining, separately from API
submission. Runtime code and state transitions are unchanged. No new dependencies
are required. Run commands from the repository root with Docker and Python 3.12.

## Dedicated storage and authorization

```sh
uv sync --locked
docker compose -f benchmarks/compose.yaml config -q
docker compose -f benchmarks/compose.yaml up -d --wait
export BENCHMARK_DATABASE=1
export BENCHMARK_DATABASE_URL=postgresql+psycopg://benchmark:benchmark@127.0.0.1:55452/rte_benchmark_test
uv run python -m benchmarks prepare
```

The measured local image had RepoDigest
`postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94`
(recorded from local Docker inspection; image ID is in every official manifest).
The convenient `postgres:16` tag can move. To reproduce the same PostgreSQL image
bits, use this override for the startup command above:

```sh
cat > /tmp/rte-benchmark-image.yaml <<'YAML'
services:
  db:
    image: postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94
YAML
docker compose -f benchmarks/compose.yaml -f /tmp/rte-benchmark-image.yaml up -d --wait
```

Use a compatible host architecture; the image identifier in each manifest permits
checking which image actually ran. The optional override does not change runtime
code or database settings.

The example credentials are public local synthetic-only credentials. The port is
bound to loopback. `BENCHMARK_PORT` can select another unused port; update the URL
explicitly. This Compose project is separate from the development stack. It creates
`rte-benchmark_benchmark-data`, a labeled persistent Docker volume mounted at
`/var/lib/postgresql/data`, with fsync and synchronous_commit on. No tmpfs is used.

`prepare` will migrate only an empty explicitly authorized `_test` database and
record a benchmark ownership marker. Nonempty unmarked databases are refused.
Subsequent runs require the marker, actual `current_database()` equality and an
exclusive controller lock. Other clients in that database are rejected. Never use
a development/production database, even if it happens to end in `_test`. There is
no fallback to `DATABASE_URL`. Do not set benchmark variables to the regression
test database. A marker does not protect against an unrelated administrator who
has credentials; keep the database dedicated and stop unrelated clients.

## Smoke and official matrix

```sh
# Functionality only: 20 jobs, five warm-up jobs, one repetition per configuration.
uv run python -m benchmarks run --smoke --docker-container rte-benchmark-db-1

# Official baseline: 24 measured runs, plus eight separate warm-ups.
TASK_ENGINE_TELEMETRY=disabled uv run python -m benchmarks run \
  --profiles job_only job_outbox --workers 1,2,4,8 \
  --size 1000 --warmup 100 --repetitions 3 --seed 20261113 \
  --lease-seconds 60 --timeout 120 --telemetry disabled \
  --docker-container rte-benchmark-db-1
```

These are the exact matrix settings used for the checked-in baseline; its invocation
also selected `--output benchmarks/results/phase3b-baseline`. Default output is a
new UTC-named directory under `benchmarks/results/`; explicit existing directories
are refused. Run sequentially on the same host/PG configuration, without another
benchmark, test suite or load generator. Keep N and payloads constant across worker
counts. The suite verifies the inspected container and connected server have the
same PG cluster identity; official runs require the verified persistent volume
and durability settings. Custom workers/subsets produce explicit configuration
manifests and must not be described as the full matrix.

The optional existing-handler permanent-failure profile can be selected with
`--profiles permanent_failure_5pct`. It fails floor(N/20) jobs deterministically
and reports success/failure separately. It does not emit outbox events, exercise
transient retries or add handler functionality. Baseline evidence covers A and B.

A workload timeout, premature process exit or invariant violation exits nonzero,
cleans owned processes/data and records a fixed failure code. Invalid cases publish
no rates or latency samples. Saved summaries identify all missing measured cases.
Preflight safety failures abort before any workload and may have no result set.
Do not treat a smoke result, an incomplete matrix or a missing p99 as a replacement
for missing official measurements. Preserve failed evidence rather than rerunning
into an existing output directory.

## Controlled console telemetry comparison

After the matrix, run the following **sequentially**, with identical host settings:

```sh
uv run python -m benchmarks run --profiles job_outbox --workers 2 \
  --size 1000 --warmup 100 --repetitions 3 --seed 20261113 \
  --telemetry disabled --docker-container rte-benchmark-db-1
uv run python -m benchmarks run --profiles job_outbox --workers 2 \
  --size 1000 --warmup 100 --repetitions 3 --seed 20261113 \
  --telemetry console --docker-container rte-benchmark-db-1
```

Application logging is disabled in both modes. Console exporters still format
spans/cumulative metrics but write to `os.devnull`; this isolates instrumentation
and exporter cost from terminal rendering. Startup and shutdown are outside timing.
The comparison is small and ordered, so cache/thermal/background activity can
confound it. The difference is an observation, not a universal overhead estimate.
No cumulative counter snapshots contribute to measured rates or correctness.

## Metrics and artifacts

Each directory includes:

- `manifest.json`: allowlisted host/configuration evidence, source fingerprint,
  Git commit/dirty state, durability/storage identity and experimental settings.
- `runs.csv`: one row per actual warm-up or measured run; monotonic intervals,
  job/ack rates, correctness counts, validity and sanitized failures.
- `latencies.csv`: only valid measured samples, one value per job and (for B)
  outbox event. Values sorted per run/metric and rounded to 0.001ms; no UUIDs or
  payloads. No enormous telemetry exports or database files are saved.
- `summary.json`: missing/invalid cases, median per-run rates and pooled latency
  distributions by profile/worker count; counts, p50/p95/p99/min/max/mean.
- `report.md`: human-readable setup, scaling, correctness and limitations.

Regenerate a report from its actual CSV evidence without connecting to a database:

```sh
uv run python -m benchmarks summarize benchmarks/results/phase3b-baseline
```

Percentiles use nearest rank, ceil(q×sample count), without interpolation. p99 is
null below 1000 samples; small smoke samples do not support credible tail claims.
The baseline pools 3000 measured jobs per configuration. Throughput is the median
of three per-run rates; scaling efficiency is that rate divided by the one-worker
rate and worker count. Inspect individual runs for variability; three repetitions
and pooled percentiles do not provide confidence intervals or population p99.

The common latency origin is PostgreSQL `clock_timestamp()` fetched immediately
before releasing the ready-process barrier. Terminal job `updated_at` and outbox
`delivered_at` are DB timestamps before commit. They yield **backlog-drain
completion latency** and **outbox completion latency**, including queue wait.
Outbox latency ends at acknowledgement, not at receiver effect commit and not
relative to job completion. This is not HTTP-to-completion E2E latency.

Rates use controller `perf_counter()` until all expected terminal counts are visible,
separately for jobs and delivery. Polling adds up to 20ms plus query overhead.
The DB-start RPC gap is included in latency and not corrected. Process startup,
submission, post-drain row checks, cleanup, exports at shutdown and report generation
are excluded. Each run is published only after exact UUID/payload/status/attempt/
lease/error checks and exact event/effect identity checks pass. Read
[ADR 0008](adr/0008-benchmark-methodology.md) for the complete method.

## Tests and cleanup

Independent unit tests require no database or production application import:

```sh
uv run pytest -q benchmarks/tests -k 'not smoke'
# With explicit benchmark authorization above, includes the real PG smoke matrix:
uv run pytest -q benchmarks/tests
```

The normal full suite also collects benchmark tests. The existing destructive test
suite requires a **different** disposable `_test` DB with `TEST_DATABASE=1` and
explicit `TEST_DATABASE_URL`; migrate only that newly created empty test DB. See
[phase 3B validation](phase-3b-validation.md) for the exact local invocation. CI
creates a separate empty benchmark database in its ephemeral PG service and runs
the smoke test, with no absolute performance assertions.

After all measurements, stop only this benchmark service:

```sh
docker compose -f benchmarks/compose.yaml stop
```

The dedicated persistent volume remains for inspection/reproduction. Do not run
`down -v`, Docker prune or remove an unknown volume. Regression tables are cleaned
by fixtures; benchmark runtime tables are emptied after each result set. Do not
run regression tests and performance measurement concurrently.

## Results and limits

Actual baseline, telemetry comparison and hardware findings are linked from the
[phase 3B validation record](phase-3b-validation.md) and README. No production
performance/SLA equivalence is claimed. Synthetic handlers are effectively no-op
work; no HTTP ingress or network downstream effect is measured. Receiver effects
are DB rows on the same server. DB work may dominate scaling; one dispatcher may
constrain outbox throughput. Cache, WAL, I/O and CPU contention affect measurements.
Bottleneck attribution needs additional profiling. Existing idle development
services were left untouched; shared-host background activity remains a confounder.
