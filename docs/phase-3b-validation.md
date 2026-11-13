# Phase 3B validation: reproducible performance evidence

## Scope and provenance

Started on clean `main` at `33e83cf2e01fd22ce86128bfc142491e93e5e33e`.
The supplied baseline GitHub CI status was SUCCESS. No new remote CI run, commit,
push, pull request, remote repository write or Git identity change was performed.
All data is synthetic, from this repository's existing handlers/receiver only.
No private project code or information was used. Production runtime, migrations,
schemas and state-transition semantics are unchanged.

Read the project AGENTS.md, README, ADRs 0001–0007, relevant runtime/database/model/
telemetry code, tests/conftest safety, tests, pyproject and CI/Compose configuration.
Loaded the three required installed efficiency/validation skills and Serena's
instructions. Serena symbol inspection had no active language server; used focused
repository reads/searches. Independent reads/checks were batched, unchanged guidance
was not reread, and no subagents were delegated. No required checks were skipped.

## Implementation

`benchmarks/` provides safety/ownership, deterministic profiles, process lifecycle,
monotonic timing, DB invariants, nearest-rank statistics, allowlisted environment
manifest, compact CSV and reproducible JSON/Markdown reporting. Default matrix:
A/B × 1/2/4/8 workers × three measured repetitions, N=1000; 100-job separate warm-up
once per configuration. Job_outbox holds one dispatcher. Optional deterministic
5% permanent failures use existing `fail=true`, never a new handler or transient
retry claim. The CLI is `uv run python -m benchmarks`.

`benchmarks/tests/` is independent of production test fixtures and requires no DB
for unit tests. Its real PostgreSQL smoke test is explicitly authorized and uses
a different disposable database from the existing destructive tests. Pytest's
configuration collects both suites and puts the repository package on its import
path. CI provisions the separate smoke DB in its ephemeral PostgreSQL service;
there are no throughput threshold assertions. No dependency/lockfile change.

The first unit attempt exposed the missing pytest repository import path; fixed
using pytest's `pythonpath`. A cleanup test initially attempted to pickle a mock
pipe; replaced it with real multiprocessing pipes and a synthetic ready child.
Initial storage discovery used host findmnt, which could not access Docker Snap's
volume path. It aborted before workload creation; switched to in-container
filesystem discovery, Docker mount/label verification and actual PG cluster-ID
comparison. No unsafe target was adopted and no safety gate was bypassed.
The first independent test collection failed with one import error (0.11s).
After that fix, the mock-pipe cleanup test yielded 1 failed / 39 passed /
1 deselected (10.46s); the corrected unit run yielded 40 passed / 1 deselected
(15.84s). Lint findings during implementation were corrected before the final
successful checks. These failures did not produce official performance numbers.
Subsequent correctness improvements invalidate saved evidence on final cleanup
failure and reject corrupt outbox states. These are implementation checks, not
performance observations.

## Local environment and database isolation

Created a new Compose project `rte-benchmark` and container `rte-benchmark-db-1`,
with newly created labeled volume `rte-benchmark_benchmark-data`; no existing
volume was removed/reused unknowingly. PostgreSQL 16.15, fsync=on,
synchronous_commit=on, full_page_writes=on, local Docker volume on ext4. Image and
container IDs, WAL settings and full allowlisted environment are in each manifest.

Host: Ubuntu 24.04.3 LTS, Linux 7.0.0-38-generic, Intel Core Ultra 7 255H,
16 logical CPUs, 31,749,392 KiB physical memory (about 30.28 GiB).
Python 3.12.3; uv 0.12.17; Docker 29.8.0; Compose 2.37.1+ds1-0ubuntu2~24.04.1.
Available physical memory is sampled separately for every result set. CPU affinity,
power/thermal state and cache state were not fixed. Existing development containers
remained untouched. A pre-measurement Docker snapshot showed their API/worker
CPU usage around 0.28–0.32% each and DB 13.67% over that observation interval;
no isolation from all shared-host activity is claimed. No competing benchmark/load
generator or regression suite runs alongside official measurements.

New `rte_benchmark_test` is dedicated to benchmarks; actual identity, ownership,
absence of other clients and Docker/PG cluster association are checked. Separate
new `rte_regression_test` in the same disposable container holds existing tests;
its actual name and empty public schema were verified before initial Alembic
migration. Test/migration activity finishes before official measurement. Startup
30s, workload 120s, cooperative shutdown 5s, bounded terminate/kill fallback;
controller statement/lock timeouts 5s, controller connect timeout 5s. Leases 60s.

## Commands and automated verification

```sh
uv sync --locked
uv run ruff check .
uv run mypy src
docker compose config -q
docker compose -f benchmarks/compose.yaml config -q
docker compose -f benchmarks/compose.yaml up -d --wait
export BENCHMARK_DATABASE=1
export BENCHMARK_DATABASE_URL=postgresql+psycopg://benchmark:benchmark@127.0.0.1:55452/rte_benchmark_test
uv run python -m benchmarks prepare

# Created exactly once in this newly provisioned disposable container:
docker exec rte-benchmark-db-1 createdb -U benchmark rte_regression_test
# Actual DB name and empty schema checked read-only before the migration below.
DATABASE_URL=postgresql+psycopg://benchmark:benchmark@127.0.0.1:55452/rte_regression_test \
  uv run alembic upgrade head
TEST_DATABASE=1 \
TEST_DATABASE_URL=postgresql+psycopg://benchmark:benchmark@127.0.0.1:55452/rte_regression_test \
  uv run pytest -q

uv run python -m benchmarks run --smoke --docker-container rte-benchmark-db-1 \
  --output benchmarks/results/phase3b-smoke
git diff --check
```

The original checkpoint full suite passed **340 tests, 26 warnings in 95.41s**:
299 existing tests plus the then-current 41 benchmark tests, including real PG smoke.
Later outbox/storage/cleanup tests expand the final suite. The existing warnings
are FastAPI/Starlette's httpx TestClient deprecation and 25 Alembic path_separator
warnings. No production dependency was changed to suppress them.

Final full suite: **349 passed, 26 warnings in 94.72s (0:01:34)**,
comprising all **299 existing tests plus 50 benchmark tests**, including the
real PostgreSQL smoke matrix. Ruff passes; mypy reports no issues in nine source
files; both Compose configurations validate; locked uv sync succeeds. Additional
focused negative tests passed during implementation (eight outbox/storage cases,
then cleanup-evidence and filesystem checks). A final diff/secret review follows
measurement. No absolute throughput threshold was asserted.

## Measurement contract and limitations

DB `clock_timestamp()` precedes barrier release. All actual terminal job timestamps
and delivery ack timestamps are enumerated; no sample interpolation or missing-case
imputation. Rates use controller monotonic confirmation intervals. Queue waiting
is included in backlog-drain completion latency; no HTTP-to-completion E2E latency
or HTTP/network downstream effect is measured. Timestamps precede COMMIT, depend
on DB wall time and include the start-reference RPC gap. Polling/query cost affects
confirmation time. Startup/loading/teardown/validation are outside measurement.

Percentiles use nearest rank ceil(q*N), no interpolation; p99 suppressed below
1000 observations. Summary pools equal-size measured repetitions and takes median
per-run throughput. Every distribution includes count/min/max/mean. Small-sample
p99 and population-tail/SLA claims are excluded; three repetitions do not produce
a confidence interval. Warm-up uses different processes and data, warming shared
DB/OS caches rather than retaining Python interpreter caches.

The synthetic handlers are effectively no-op work; the receiver's entire effect
is another row on the same PostgreSQL server. The database may dominate scaling,
and one dispatcher may constrain ack throughput. Cache, WAL, I/O and CPU contention
and other host activity matter. Rates alone cannot identify the exact resource
bottleneck. No production workload equivalence is claimed. See
[ADR 0008](adr/0008-benchmark-methodology.md) and [reproduction guide](benchmarks.md).

## Official baseline: actual measured results

[Report](../benchmarks/results/phase3b-baseline/report.md) ·
[manifest](../benchmarks/results/phase3b-baseline/manifest.json) ·
[runs CSV](../benchmarks/results/phase3b-baseline/runs.csv) ·
[summary](../benchmarks/results/phase3b-baseline/summary.json) ·
[latencies CSV](../benchmarks/results/phase3b-baseline/latencies.csv).

Executed from 2026-11-13T22:04:36.738326Z, with the dirty working tree explicitly
recorded in the manifest. Source/config fingerprint:
`192ab442c553227bc34ccba923720ee7568fbc107cce4acd93f94b91e859b81d`.
The baseline completed 24 measured runs and eight separate warm-ups. Each measured
case has three repetitions of 1000 jobs. Zero invalid runs and zero missing cases.
Telemetry disabled throughout; one dispatcher for B, zero for A. No other suite
or benchmark ran concurrently.

```sh
BENCHMARK_DATABASE=1 \
BENCHMARK_DATABASE_URL=postgresql+psycopg://benchmark:benchmark@127.0.0.1:55452/rte_benchmark_test \
TASK_ENGINE_TELEMETRY=disabled uv run python -m benchmarks run \
  --profiles job_only job_outbox --workers 1,2,4,8 --size 1000 --warmup 100 \
  --repetitions 3 --seed 20261113 --lease-seconds 60 --timeout 120 \
  --telemetry disabled --docker-container rte-benchmark-db-1 \
  --output benchmarks/results/phase3b-baseline
```

Rates are median per-run jobs/events per second; efficiency is relative to that
profile's one-worker rate divided by worker count. Latency tuples are pooled
p50/p95/p99 **seconds**, for readability (CSV/JSON/report use milliseconds).
Every distribution below enumerates 3000 actual observations; full min/max/mean
and count are in summary.json.

| Profile | Workers | Jobs/s | Ack/s | Job p50/p95/p99 s | Outbox p50/p95/p99 s | Job efficiency | Ack efficiency |
|---|---:|---:|---:|---|---|---:|---:|
| job_only | 1 | 46.90 | — | 10.309 / 20.125 / 21.199 | — | 100.0% | — |
| job_only | 2 | 93.47 | — | 5.550 / 10.202 / 10.580 | — | 99.7% | — |
| job_only | 4 | 166.62 | — | 3.007 / 5.683 / 5.917 | — | 88.8% | — |
| job_only | 8 | 316.81 | — | 1.555 / 2.992 / 3.144 | — | 84.4% | — |
| job_outbox | 1 | 43.66 | 41.72 | 11.263 / 21.354 / 22.665 | 12.534 / 23.139 / 24.177 | 100.0% | 100.0% |
| job_outbox | 2 | 81.73 | 40.45 | 6.128 / 11.592 / 12.080 | 13.362 / 23.882 / 25.425 | 93.6% | 48.5% |
| job_outbox | 4 | 148.06 | 39.94 | 3.367 / 6.382 / 6.654 | 13.643 / 23.521 / 24.690 | 84.8% | 23.9% |
| job_outbox | 8 | 284.42 | 42.20 | 1.809 / 3.365 / 3.554 | 12.184 / 22.560 / 23.445 | 81.4% | 12.6% |

Job-only scales from 46.90 to 316.81 jobs/s: **6.76× speedup**, **84.4% efficiency**
at eight workers. Job_outbox terminalization scales from 43.66 to 284.42 jobs/s:
**6.51× speedup**, **81.4% efficiency**. Acknowledgements stay 39.94–42.20/s:
**1.012× speedup**, **12.6% efficiency** at eight workers. Its completion p99
remains about 23–25 seconds, despite job-completion p99 decreasing from 22.665s
to 3.554s. Adding workers alone does not improve delivery throughput materially
in this experiment. This is consistent with a delivery-side constraint when one
dispatcher performs claim, receiver commit and ack serially, sharing PostgreSQL.
It does not isolate SQL round trips, fsync/WAL, storage latency or CPU as the cause.

The existing worker performs recovery, claim and terminalization transactions;
the dispatcher performs claim/recovery, receiver and ack transactions. No-op
handlers leave these DB operations central to measured work. More workers show
some loss of linear efficiency. Query/lock/WAL/I/O profiling is needed before
attributing that loss or proposing transaction changes. No production behavior
was altered for this milestone.

## Correctness evidence

Across measured baseline runs: **24,000 succeeded, zero failed**, each attempts=1.
B created **12,000 distinct committed events**, all delivered at delivery_attempts=1,
with exactly **12,000 unique matching receiver effects**. A produced zero events
or effects. Per-run exact expected job UUIDs/payloads, budgets, statuses, error/
terminal metadata, event/effect identities/types/payloads and timestamp causality
passed. Zero dead letters, duplicate IDs, active/stale leases or abandoned jobs.
All eight warm-ups separately passed the same gates. Throughput and latencies
were published only after processes stopped and row validation passed. The final
runtime-table cleanup succeeded. CSV contains 36,000 measured latency samples:
12,000 A job samples, 12,000 B job samples and 12,000 B acknowledgement samples.

Independent tests additionally verify unsafe/missing authorization, actual identity
mismatch, rejection of nonempty unowned DBs before migration, exact statistics,
worker parsing, deterministic data, timeout failure, status/attempt/lease/payload
corruption, missing/duplicate/corrupt outbox/effect rows, process-start/readiness/
body failures with cleanup of all owned children, invalid final cleanup evidence,
reproducible CSV summarization, explicit missing cases, volatile-storage rejection
and the real PostgreSQL smoke matrix. Smoke values are not substituted for any
baseline case; its p99 values are null.

## Controlled telemetry comparison

After the matrix, sequential A/B-style measurements used job_outbox, two workers,
one dispatcher, N=1000, 100 warm-up jobs and three measured repetitions for each
mode. Hardware, PG/volume/durability, source fingerprint, seed, leases and timeouts
were the same. Both result sets completed with zero invalid/missing cases and all
6000 measured jobs/events/effects passed their correctness gates.

```sh
# BENCHMARK_DATABASE and explicit BENCHMARK_DATABASE_URL remain as above.
uv run python -m benchmarks run --profiles job_outbox --workers 2 \
  --size 1000 --warmup 100 --repetitions 3 --seed 20261113 \
  --telemetry disabled --docker-container rte-benchmark-db-1 \
  --output benchmarks/results/phase3b-telemetry-disabled
uv run python -m benchmarks run --profiles job_outbox --workers 2 \
  --size 1000 --warmup 100 --repetitions 3 --seed 20261113 \
  --telemetry console --docker-container rte-benchmark-db-1 \
  --output benchmarks/results/phase3b-telemetry-console
```

| Mode | Jobs/s median | Ack/s median | Job p50/p95/p99 s | Ack p50/p95/p99 s |
|---|---:|---:|---|---|
| disabled | 82.83 | 41.63 | 5.983 / 11.482 / 11.969 | 13.604 / 23.024 / 23.834 |
| console | 74.00 | 36.19 | 7.032 / 12.941 / 13.769 | 14.757 / 26.252 / 27.498 |

Observed console-mode median rate reductions: **10.66% jobs/s** and
**13.07% acknowledgements/s** relative to the controlled disabled mode.
This is a small ordered comparison, not a universal causal overhead estimate or
confidence interval. Exporters format/export to the null sink; terminal rendering
is excluded, as are startup/shutdown. App logging is disabled in both modes.
Cumulative counter snapshots were never aggregated or used as benchmark events.

[Disabled report](../benchmarks/results/phase3b-telemetry-disabled/report.md) ·
[console report](../benchmarks/results/phase3b-telemetry-console/report.md).

## Optional permanent-failure functionality

After A/B and the telemetry comparison, ran:

```sh
uv run python -m benchmarks run --smoke --profiles permanent_failure_5pct \
  --workers 1,2,4,8 --docker-container rte-benchmark-db-1 \
  --output benchmarks/results/phase3b-permanent-failure-smoke
```

All four worker configurations passed with 20 measured jobs each: 19 succeeded,
one permanently failed, all attempts=1, no events/effects/leases. Across measured
smoke cases: **76 successes, four expected permanent failures**. Five-job warm-ups
contain floor(5/20)=0 failures, as specified. No transient-retry or official C
performance result is claimed. p99 remains null. See
[functional C report](../benchmarks/results/phase3b-permanent-failure-smoke/report.md).

## Evidence review and v1.0 follow-up

All five sets can be parsed and independently summarized from their CSVs; generated
Markdown and summary JSON match regeneration. Baseline, both telemetry sets and
C smoke fingerprints match the final measured source/config exactly. The initial
saved A/B smoke predates final metadata/controller hardening and retains its own
original fingerprint. It is functional evidence, not a performance baseline.
The final full suite also ran the A/B smoke independently on the final implementation.
Result artifacts total about 1 MB; no database files or raw telemetry exports are
included. Tracked changes and every new source/doc/config file and generated
manifest/CSV schema were reviewed. Artifact review checks counts, parseability,
source provenance and no credentials/URLs/private hostnames/ambient environment.
Only public local synthetic example credentials appear in docs/Compose/CI.

Recommended v1.0 performance work, outside this milestone:

1. Profile query plans, SQL round trips, locks, WAL/fsync and storage/CPU utilization
   before changing batching, claims, recovery cadence or transaction boundaries.
   Preserve fencing, independent receiver commits and bounded-attempt correctness.
2. Add a separately designed dispatcher-scaling experiment and isolated HTTP
   ingress/real-transport benchmarks with representative payload/handler costs,
   downstream idempotency and ambiguous-outcome testing. Do not generalize these
   no-op/shared-PG results to production effects.
3. Repeat on an isolated host with pinned PG image, controlled cache/power state,
   longer runs and more repetitions; report per-run spread and confidence intervals.
   The measured image RepoDigest is documented in the reproduction guide; the
   convenient postgres:16 tag can otherwise move.

No changes implementing these recommendations were made. The suite remains an
evidence tool; it does not add production business functionality.

## Final cleanup and repository state

Final artifact/schema/source checks passed for all five result sets. Standalone
`python -m benchmarks summarize` regenerated each summary/report byte-for-byte
without DATABASE_URL or benchmark authorization variables. Source/config hashes
of all official sets matched current measured files. Reviewed all **43** changed/
untracked files (18 source/config/docs plus 25 result artifacts); result directory
is **1.1 MiB** on disk. Secret-pattern and credential/hostname checks found only
public local synthetic example credentials. No secrets or connection URLs occur
in saved results; production src/migrations/uv.lock/main compose are unchanged.

Final checks: `uv run ruff check .` passed; `uv run mypy src` passed (nine files);
both Compose config checks passed; `git diff --check` passed. No code changes were
made after the official measurements. Documentation was completed from the saved
actual results. New remote CI was not executed.

Read-only cleanup verification returned:

```text
rte_benchmark_test|jobs=0|outbox=0|receiver=0|other_client_connections=0
rte_regression_test|jobs=0|outbox=0|receiver=0
```

`docker compose -f benchmarks/compose.yaml stop` completed; the dedicated container
is `exited`. Its persistent volume remains. No volume deletion, prune or changes
to development services were performed. Final branch is `main`, HEAD remains
`33e83cf2e01fd22ce86128bfc142491e93e5e33e`; intentional changes remain uncommitted.
No commits, pushes, PRs, remote writes or Git identity changes.
