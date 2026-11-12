# Phase 3A validation: observability

## Baseline and scope

Started from clean `main` at `5c73b3a51fc68bf79ed463db8938ffb748bcc19c`.
The supplied baseline GitHub Actions result was SUCCESS; no remote write or CI run
was requested or performed. No commits, pushes, PRs or Git identity changes.
Existing ignored local configuration and running development services were left
alone. All data used here is synthetic.

Read AGENTS.md, the three required installed efficiency/validation skills, README,
ADRs 0001–0006, API/worker/dispatcher/operator/database/models and existing tests,
including destructive-test safeguards and previous validation evidence. Serena's
initial instructions were loaded; symbol inspection was unavailable because no
language server was active, so repository reads/searches were used. Independent
reads were batched; unchanged instructions were not reread. No required checks
were omitted for efficiency and no agents were delegated.

## Files and dependencies

- `src/task_engine/telemetry.py`: private API/SDK providers, explicit once-per-process
  initialization, disabled/console modes, bounded manual spans, process-local
  counters/histograms, safe console exporters and JSON formatter.
- `src/task_engine/api.py`, `worker.py`, `dispatcher.py`, `operator.py`: explicit
  startup integration and instrumentation at existing commit boundaries. The
  worker fatal CLI boundary now uses fixed diagnostics without traceback text.
  The operator retains its default stdout/stderr and exit-code contract.
- `src/task_engine/diagnostics.py`: one read-only aggregate SELECT, no mutation.
- `compose.yaml`: one dispatcher using the same application build and database,
  successful-migration dependency, independent delivery lease configuration and
  no additional port; explicit telemetry/log opt-in variables.
- `pyproject.toml`, `uv.lock`: official direct `opentelemetry-api>=1.39,<2` and
  `opentelemetry-sdk>=1.39,<2`, locked to **1.45.2**. The API was already a
  transitive locked dependency; it is now direct. The SDK adds official
  `opentelemetry-semantic-conventions==0.66b2`. No unrelated versions changed.
- `tests/test_telemetry.py`: 65 added parametrized cases.
- `README.md`, `docs/adr/0007-observability.md`, this record: configuration, schema,
  interpretation, privacy, reproduction and limitations.

No migration, transport adapter, public endpoint, dashboard or backend service
was added. State machines, fencing predicates, retry schedules, transactional
outbox atomicity and stable receiver deduplication IDs remain intact. A fixed
state column in delivery claim selection distinguishes expired reclaims, and
`RETURNING id` counts actual recovery updates. Neither changes persisted values.

## Disposable environment and commands

Environment: uv 0.12.17, Python 3.12.3, Docker 29.1.3, Compose 2.37.1,
PostgreSQL 16.15. Created a dedicated container independent of development
Compose. `docker inspect` confirmed `Mounts=[]`, with tmpfs storage at
`/var/lib/postgresql/data`. Only loopback port 55441 was bound.

```sh
uv sync --locked

docker run --detach --name rte-phase3a-test-db \
  --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_USER=task -e POSTGRES_PASSWORD=task \
  -e POSTGRES_DB=rte_phase3a_test \
  -p 127.0.0.1:55441:5432 postgres:16

DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:55441/rte_phase3a_test \
  uv run alembic upgrade head

TEST_DATABASE=1 \
TEST_DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:55441/rte_phase3a_test \
  uv run pytest -q --tb=short

uv run ruff check .
uv run mypy src
docker compose config -q
git diff --check
git status --short
git ls-files --others --exclude-standard
```

All database-touching pytest invocations used both explicit `TEST_DATABASE=1` and
this disposable `_test` URL. Existing safeguards were unchanged: reject unsafe or
implicit URLs before application import, and check the actual connected database
before deletion. Migration tests operated only in this isolated database.
Development and production databases were never migrated, cleaned or deleted.

## Actual results

| Check | Result |
| --- | --- |
| Baseline collection before adding tests | 234 cases |
| Final complete PostgreSQL suite | **299 passed**, 26 warnings, **38.05 s** |
| New observability cases in final suite | **65 passed** |
| `uv sync --locked` | Passed; 38 resolved, 36 checked |
| `uv run ruff check .` | All checks passed |
| `uv run mypy src` | Success: no issues found in 9 source files |
| `docker compose config -q` | Passed |
| `git diff --check` and new-file whitespace check | Passed |
| Tracked/untracked diff and secret review | Passed; only intended files, synthetic markers/local example credentials |

Warnings are the existing Starlette/httpx TestClient deprecation (1) and Alembic
`path_separator` configuration deprecation (25). They were not suppressed.

An initial focused run exposed stdout capture of an exporter default argument;
passing the current stdout explicitly fixed capture and embedding behavior. The
first complete run exposed a default operator INFO line on success stderr; using
the operator's existing WARNING threshold restored the prior CLI contract. Both
were corrected before the successful complete runs. The final run includes the
additional interruption tests; no application code changed after that run.

## Test coverage and evidence interpretation

The new tests use official `InMemorySpanExporter` and `InMemoryMetricReader`.
No external telemetry backend, new test framework or long sleep is used.

| Requirement | Evidence |
| --- | --- |
| A, K: disabled behavior and no export | Disabled integration completes a job and delivery while provider constructors are forbidden. A separate interpreter forbids network connection creation; even OTLP exporter environment settings cannot enable export. Import does not initialize providers. All previous regression tests also run in default disabled mode. |
| B–D: submission accounting | One created, one replayed, one conflicting submission; one created counter only. Exact submit and lookup span names/outcomes asserted. |
| E: worker outcomes | Success, permanent failure, retryable failure and unexpected failure; committed claims, handler observation vocabulary, retry/terminal transitions and monotonic durations asserted. |
| F: outbox outcomes | Acknowledgement, retry, permanent dead-letter and unexpected receiver failure; receiver observations separated from committed transitions. Exhausted expiry dead-letter/reclaim and operator replay counted. |
| G: stale/expired owners | Both job and outbox transitions tested for success/retry/failure with wrong and expired tokens; no transition count and bounded lease_lost span. Recovered owners are also fenced. |
| H: rollback/failed commit | Actual PostgreSQL division-by-zero abort during commit or real lost-response injection after DBAPI commit for submission, job claim/finish/recovery, delivery claim/finish/recovery, receiver and operator replay (18 cases). No false transition success; database_error spans. Separate post-statement PostgreSQL aborts test receiver and both owner completion paths. Receiver errors never cause acknowledgement. |
| I: privacy | Synthetic private marker in payload, key and exception; inspect span attributes/events/status/resources, metric points/exemplars and logs. Exact allowlisted attribute keys, no exception events/status descriptions, no exemplars. Hostile log message, arguments, stack, exception and extras are discarded by JSON formatting. Ambient resource attributes ignored. |
| J: concurrency | Eight simultaneous API submissions create one job; concurrent worker/dispatcher polls produce one committed claim/completion/delivery/effect with matching counters. Two same-token finish contenders produce one committed signal and one rejection for each owner protocol. Existing multiprocess and crash/race tests also pass. |
| L: diagnostics | Empty/due/expired/dead-letter counts, actual transaction_read_only=on, one SQL statement, exact persisted-row comparisons before/after, and subprocess CLI counts. |
| M: regressions | All 234 prior cases pass, including migrations, idempotency, independent receiver commits, concurrency, crashes, bounded retries, fencing, replay and fail-closed test guards. |
| Export/initialization robustness | Once-per-process setup, fixed resources, actual console span/metric output, failed console export and failed instruments leave committed job/delivery outcomes successful. |
| Process interruption | KeyboardInterrupt/SystemExit propagate from handler/receiver; observation is interrupted and no transition success is emitted. |

These tests exercise real PostgreSQL locks and transactions. Lost-response tests
use controlled faults after real commits, rather than claiming to reproduce a
physical network partition. Successful handler/receiver observations remain
allowed when a later sender completion fails; they describe different operations.

## Reproducible synthetic smoke

A second fresh disposable database in the same isolated container kept the smoke
independent of destructive tests. This exercises a real HTTP API and separate
worker/dispatcher processes. The one-shot calls use the same explicit startup
functions as the long-running CLIs; they avoid background polling and sleeps.

```sh
docker exec rte-phase3a-test-db createdb -U task rte_phase3a_smoke_test
export DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:55441/rte_phase3a_smoke_test
uv run alembic upgrade head
export TASK_ENGINE_TELEMETRY=console TASK_ENGINE_LOG_FORMAT=json

# Terminal 1: wait for the ordinary startup message, then use terminal 2.
uv run uvicorn task_engine.api:app --host 127.0.0.1 --port 18003

# Terminal 2: submit exactly once on this fresh database.
curl --fail --silent --show-error http://127.0.0.1:18003/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"emit_outbox_event":true,"example":"phase-3a-synthetic"}}'

uv run python - <<'PYTHON'
from task_engine.telemetry import configure_logging, initialize
from task_engine.worker import run_once
configure_logging()
initialize("worker")
assert run_once()
PYTHON

uv run python - <<'PYTHON'
from task_engine.telemetry import configure_logging, initialize
from task_engine.dispatcher import run_once
configure_logging()
initialize("dispatcher")
assert run_once()
PYTHON

# Use the returned UUID for a lookup; it is never a metric label.
curl --fail --silent --show-error http://127.0.0.1:18003/v1/jobs/JOB_UUID
uv run python -m task_engine.diagnostics

docker exec rte-phase3a-test-db psql -U task -d rte_phase3a_smoke_test \
  -Atc 'SELECT count(*) FROM synthetic_receiver_events'
# Stop only this smoke API with Ctrl-C when finished.
```

The actual invocation redirected exporter stdout and JSON stderr to temporary
`/tmp/rte-phase3a-{api,worker,dispatcher}.{stdout,logs}` files, captured the POST
response locally and performed lookup using that synthetic UUID. POST returned
**201**, GET returned **200 / succeeded**, and receiver count was **1**. Temporary
raw output was reviewed; selected fields are captured below. Formatting omits
SDK envelopes/trace IDs/timestamps except where a real log timestamp is shown.
Cumulative snapshots can repeat the same counter value; repeated exports must
not be summed as new operations.

```text
POST status=201
GET status=200 job_status=succeeded
synthetic_receiver_events count=1
```

```json
{"name":"api.submit","attributes":{"component":"api","operation":"submit","outcome":"created"},"events":[],"service.name":"task-engine-api"}
{"name":"worker.finish","attributes":{"component":"worker","operation":"finish","outcome":"succeeded"},"events":[],"service.name":"task-engine-worker"}
{"name":"dispatcher.finish","attributes":{"component":"dispatcher","operation":"finish","outcome":"acknowledged"},"events":[],"service.name":"task-engine-dispatcher"}
{"name":"task_engine.job.submissions","attributes":{"component":"api","operation":"submit","outcome":"created"},"value":1}
{"name":"task_engine.job.claims","attributes":{"component":"worker","operation":"claim","outcome":"claimed"},"value":1}
{"name":"task_engine.job.transitions","attributes":{"component":"worker","operation":"finish","outcome":"succeeded"},"value":1}
{"name":"task_engine.delivery.attempts","attributes":{"component":"dispatcher","operation":"claim","outcome":"claimed"},"value":1}
{"name":"task_engine.delivery.transitions","attributes":{"component":"dispatcher","operation":"finish","outcome":"acknowledged"},"value":1}
{"name":"task_engine.handler.duration","attributes":{"component":"worker","operation":"handle","outcome":"succeeded"},"count":1,"sum":0.00028882408514618874}
{"name":"task_engine.receiver.duration","attributes":{"component":"dispatcher","operation":"receive","outcome":"succeeded"},"count":1,"sum":0.004884301917627454}
```

```json
{"timestamp":"2026-11-12T21:10:02.836319+00:00","level":"INFO","component":"worker","event":"finish","outcome":"succeeded"}
{"timestamp":"2026-11-12T21:10:03.638317+00:00","level":"INFO","component":"dispatcher","event":"finish","outcome":"acknowledged"}
```

Authoritative snapshot at smoke query time:

```json
{"dead_letter_events":0,"due_pending_jobs":0,"due_pending_outbox_events":0,"expired_delivery_leases":0,"expired_job_leases":0,"jobs_by_status":{"succeeded":1},"outbox_by_delivery_state":{"delivered":1}}
```

The smoke API was stopped. After checks, `docker rm -f rte-phase3a-test-db`
removed only the dedicated test container and its disposable tmpfs databases.
No Compose volume or development container was removed.

## Privacy review and limitations

Reviewed full tracked changes and every new file, including dependency URLs,
examples and generated smoke output. No private source material, external business
data, new credentials or telemetry endpoint was introduced. Existing/public local
`task:task` example credentials and synthetic test privacy markers are intentional.
No payload/key/token/bind/exception text appears in the new exported signals.
SQLAlchemy parameter hiding remains enabled; verbose SQL logging stays disabled.

Instrument names/attributes and all operations are listed in README and ADR 0007.
Confirmed-transition metrics are best-effort observations after commit, not durable
facts. Process crashes, disabled intervals, uncertain commits and failed exports
can make them differ from durable state. Processing observations cannot establish
job completion, unique receiver effect or outbox acknowledgement. The diagnostic
SELECT is authoritative only for its database snapshot/time and may be expensive
on large tables.

There is no cross-process parent/child trace continuity, persisted trace context,
network exporter, durable telemetry buffer or production backend requirement.
Console output has local I/O cost; exporter failures are isolated but abrupt
termination can lose final export. The synthetic receiver still shares PostgreSQL
in a separate transaction, with no real HTTP delivery or external business effect.
Dead letters remain potentially ambiguous. OTLP/HTTP, production aggregation,
retention, persisted correlation and real-transport instrumentation are deferred.
