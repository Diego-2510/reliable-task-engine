# Reliable Task Engine — phase 2D-2

A small PostgreSQL-backed job queue with a FastAPI service and independent polling workers. Python 3.12 and the checked-in uv lockfile define the application environment. Handlers are synthetic and perform no external business effects.

## Quickstart

Run `docker compose up --build --scale worker=2` for a new installation. The migration runs before the API and workers. Submit a job with `curl -X POST localhost:8000/v1/jobs -H 'Content-Type: application/json' -d '{"payload":{"example":1},"max_attempts":3}'`, then retrieve its UUID with `curl localhost:8000/v1/jobs/<id>`. Payloads are JSON objects. Omitting `max_attempts` retains the default of 3. `{"fail":true}` permanently fails on its first attempt. Validation errors and missing jobs return `{"error":{"code":...,"message":...}}`.

For local development: `uv sync --locked`; start PostgreSQL, set `DATABASE_URL` if needed, run `uv run alembic upgrade head`, then `uv run uvicorn task_engine.api:app`, `uv run python -m task_engine.worker` and, in a separate process, `uv run python -m task_engine.dispatcher`. Default connection is `postgresql+psycopg://task:task@localhost:5432/task`. The Compose credentials are for local development only.

## Upgrade and configuration

**Stop all existing workers, API instances and dispatchers before schema migration**, including phase-2A workers; they do not honor retry eligibility or attempt limits. Older workers also cannot enqueue outbox intents. Stop locally started dispatchers separately. For an existing Compose installation, use `docker compose stop worker api`, `docker compose build`, `docker compose run --rm migrate`, then `docker compose up -d --scale worker=2`. Resume only upgraded binaries. Earlier migrations take a table lock and update existing rows; allow downtime and plan for table/index work on large queues. Do not use the fresh-install command to perform a rolling upgrade with old workers still running.

Migration `0003` adds `max_attempts` (positive integer, database default 3), `available_at` (TIMESTAMPTZ, database wall-time default), and nullable fixed-vocabulary `last_error_code` and `terminal_reason`. Backfill sets `available_at = created_at` and `max_attempts = greatest(3, attempts)`. Attempts, payloads, IDs, creation/update timestamps and lease metadata are preserved. Historical attempts above 3, including above 10, are supported without granting additional claims. Already exhausted queued jobs become `failed` with `attempts_exhausted`; running jobs retain their existing lease and can complete successfully while valid, or be recovered according to the new limit after expiry. Existing terminal jobs retain their status; old failed jobs receive `legacy_failure`, without inventing an error code. Migration `0002` still initializes expired leases for phase-1 running rows.

POST accepts strict JSON integer `max_attempts` in **1..10**, including the initial attempt; booleans, strings and fractions are rejected. The database enforces positivity, not the API upper bound, to preserve legacy data. There is no API to reset attempts, raise a stored limit, or manually retry a terminal job. `LEASE_SECONDS` is a positive integer per worker, default 60. Backoff and the 100-row recovery batch are fixed implementation constants in this phase; there are no backoff environment settings. Downgrades require stopping workers/API, discard retry metadata and cannot undo terminal decisions already made.

## Submission idempotency

`POST /v1/jobs` accepts an optional `Idempotency-Key` header. Without it, every valid submission creates a new job with **201 Created**. A new key also returns **201**. Reusing a key with the same JSONB payload and `max_attempts` returns **200 OK**, the same job ID, and its current state, including running or terminal state. Omitting `max_attempts` equals explicitly sending its default of 3. Nested JSON object key order is ignored; array order and JSON types remain significant. Equality follows PostgreSQL JSONB semantics (for example, `1` and `1.0` compare equal).

```sh
# First call: 201. Repeat this call: 200, same ID and current state.
curl -i localhost:8000/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-123' \
  -d '{"payload":{"example":1},"max_attempts":3}'
# Same key, different parameters: 409.
curl -i localhost:8000/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-123' \
  -d '{"payload":{"example":2},"max_attempts":3}'
```

A mismatch in payload or `max_attempts` returns **409 Conflict** with `{"error":{"code":"idempotency_conflict","message":"Job parameters differ from the original submission"}}`. Keys must contain 1–128 printable ASCII characters excluding spaces (`!` through `~`); empty, repeated, non-ASCII, whitespace or control-containing headers return **422** with the existing `invalid_input` error. Keys are case-sensitive opaque values; they are not trimmed. Keys are excluded from responses and application logs/errors. Database failures return **500** with a fixed `database_error` response, never a successful replay or a conflict. Infrastructure outside this application must also avoid logging request headers and database diagnostics containing keys.

Migration `0004` adds nullable `jobs.idempotency_key` and a unique partial index for non-null keys. All existing rows receive NULL; no existing job fields are updated. Stop API/workers for migration as described above and allow time for the table lock and index creation. Downgrading to `0003` discards key associations; replay guarantees cannot survive that downgrade.

One short **READ COMMITTED** transaction executes `INSERT ... ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING ...`. PostgreSQL serializes competing inserts through the unique index. A loser reads the existing job and compares JSONB/attempt limit in a **separate SELECT**, whose fresh snapshot sees the committed winner. Only the targeted key conflict is ignored; other database failures propagate to the fixed 500 handler. Commit completes before returning success. Replays do not update timestamps, status, leases or attempts. There are no application locks or caches. Eight concurrent identical submissions create one job (one 201, seven 200); differing submissions yield one 201 and conflicts for parameters that differ from the winner. Different keys may proceed independently. Associations persist across API restarts.

Keys are **global in this MVP**, because there is no user/tenant isolation. There is **no automatic key expiration**; retention is a future design problem. Deleting jobs deletes their key associations. Submission idempotency prevents duplicate logical jobs, not duplicate external side effects. There is **no exactly-once execution guarantee**. Without authentication this API is **not suitable for public production deployment**. See [ADR 0003](docs/adr/0003-submission-idempotency.md).

## Transactional outbox

A synthetic payload with `"emit_outbox_event": true` opts into a durable `job.succeeded.v1` intent after handler success. Missing, false or other values retain the previous behavior. The handler performs no external effect. The outbox payload is constructed solely as `{"job_id":"<UUID>","event_version":1}`; it never copies submission keys, destination URLs, credentials or other job payload contents.

Completion first locks the job row, then requires the current token, running status and a lease strictly later than PostgreSQL `clock_timestamp()`. The successful status update, clearing the lease and optional outbox INSERT share one transaction and commit together. An insert failure or transaction abort rolls back both writes; database errors propagate. Connection loss during COMMIT can leave the outcome unknown to the worker, while PostgreSQL still persists both or neither. Handler failures and rejected/stale completions create no event. Validity is checked at the locked UPDATE, not at commit; fencing covers these PostgreSQL writes and does not fence external effects. Retry, recovery and submission-idempotency semantics remain unchanged.

Migration `0005` only adds `outbox_events`: UUID `id`, FK `job_id`, fixed `event_type`, JSONB `payload`, and TIMESTAMPTZ `created_at`. A unique constraint on `(job_id, event_type)` permits at most one success event per job. The committed event UUID is the stable future downstream deduplication key; retries of delivery must retain it. No historical jobs are backfilled or changed. Stop old workers/API before upgrading. Downgrade to `0004` drops only this table and permanently discards its events/identifiers. The FK prevents deleting referenced jobs; a future retention policy must coordinate jobs and events.

Phase 2D-2 adds a separate dispatcher and synthetic receiver to this durable intent. There is no public outbox API or exactly-once-delivery guarantee. See [ADR 0004](docs/adr/0004-transactional-outbox.md) for enqueueing and [ADR 0005](docs/adr/0005-outbox-dispatcher.md) for delivery.

## Outbox delivery and synthetic receiver

Start the dispatcher after `uv run alembic upgrade head`:

```sh
uv run python -m task_engine.dispatcher
# Optional independent lease configuration (positive integer seconds; default 60):
DELIVERY_LEASE_SECONDS=120 uv run python -m task_engine.dispatcher
```

Multiple dispatcher processes can poll concurrently. The dispatcher uses three short, separately committed transactions:

1. **Claim:** `FOR UPDATE SKIP LOCKED` selects a due `pending` event or an expired `delivering` lease. An atomic update assigns a fresh token and lease, increments `delivery_attempts`, and commits before receiver invocation.
2. **Receive:** a new transaction inserts the allowlisted event ID, job ID and fixed event type into `synthetic_receiver_events`. The event-ID primary key and `ON CONFLICT DO NOTHING` durably deduplicate repeated delivery. The original `processed_at` remains unchanged; duplicates are accepted as success. No JSON payload, submission key, destination or secret is copied.
3. **Ack:** another transaction locks the outbox row, checks delivering state, current token and a strictly unexpired lease using PostgreSQL `clock_timestamp()`, then sets `delivered` and DB-time `delivered_at` and clears both lease fields. Delivered events are never reclaimed.

The persisted outbox UUID is the **stable Event ID**, unchanged across claims, crashes and redelivery. Ownership tokens change with every claim. Expired senders cannot ack or schedule retry, including after a newer claim. Fencing checks happen after lock acquisition and at the locked update, not at commit. No transaction spans receiver execution or backoff waiting.

A temporary `RetryableDeliveryError` schedules a fenced transition to pending with `next_attempt_at` based on PostgreSQL wall time and capped exponential delays of 2, 4, 8, 16, 32, 60, 60, … seconds. Only claims increment attempts. There is **no configured maximum delivery-attempt limit and no dead-letter policy**; permanently failing events can consume resources indefinitely. Tests inject synthetic temporary errors; job payloads cannot request delivery failures. Unexpected errors propagate; database errors stop the CLI with exit code 1 and a fixed diagnostic without database/exception details. Uncertain COMMIT outcomes are errors, never confirmed successes. An uncertain receiver commit does not trigger ack.

A crash **after receiver commit but before outbox ack** leaves delivering state and a stored receiver effect. After lease expiry, a new owner redelivers the same event ID, the receiver accepts the duplicate without another stored effect, and the new owner acknowledges. A crash before receiver commit instead allows a later first effect. An ack failure cannot roll back the separately committed receiver row. Reconnect and inspect durable state to resolve uncertain commits; continued polling handles eligible leases.

The synthetic receiver uses **the same PostgreSQL instance in separate transactions**. It simulates an external reception boundary; it is **not evidence of real HTTP/network delivery**, separate-service availability, or a distributed transaction. Its row is the entire synthetic effect, and it has no foreign keys to sender tables. No real network delivery or external business effect is implemented.

Migration `0006` initializes all `0005` events as due pending, attempts zero, with no lease/delivered metadata. Existing IDs, payloads, `created_at` and jobs are preserved. State/lease/timestamp checks and partial due/expiry indexes protect the protocol. Stop API/workers/dispatchers before migration; allow table locks, check validation and index work. Downgrade to `0005` preserves original outbox/jobs data but destroys delivery state and receiver deduplication. Re-upgrade therefore re-enables delivery of every retained event and can repeat effects.

**Non-guarantees:** no exactly-once delivery, eventual success, guaranteed receiver invocation for every claim, ordering, strict fairness, bounded latency, heartbeat, jitter, dead-letter handling, retention or production readiness. Continued polling and database/receiver availability are prerequisites for progress. Paused senders can overlap after expiry; DB wall-clock jumps affect leases. Receiver-ID deletion loses deduplication, so future retention must account for replay horizons. The worker's finite job-attempt budget remains separate from delivery retries.

## Retry policy and status

Attempts count only successfully committed claims, including claims after recovery. Scheduling and recovery do not increment them. Claims start only due `queued` jobs with `attempts < max_attempts`, ordered by `available_at`, creation time and UUID. A partial index on `(available_at, created_at, id)` covers queued eligibility; the running-expiry index supports recovery. The old queued index is retained by this additive migration.

| Transition | Condition | Effect |
| --- | --- | --- |
| `queued -> running` | Due and attempts below limit | Fresh token, expiry, attempts + 1 |
| `running -> succeeded` | Successful handler and valid owner | Terminal success |
| `running -> failed` | `PermanentJobError` and valid owner | `permanent_error`, even on attempt 1 |
| `running -> queued` | Retryable/unexpected handler failure, attempts below limit, valid owner | Delayed eligibility |
| `running -> failed` | Retryable/unexpected failure at limit, valid owner | `attempts_exhausted` |
| Expired `running -> queued` | Attempts below limit | Recovery schedules delayed eligibility |
| Expired `running -> failed` | Attempts at/above limit | `attempts_exhausted`, even if no further claim is possible |

`RetryableJobError` explicitly requests retry. `PermanentJobError` also subclasses `ValueError`, preserving the synthetic handler contract. Other ordinary handler exceptions are classified as `unexpected_error` and retried within the limit. `KeyboardInterrupt` and `SystemExit` propagate, leaving the running lease for recovery. Database failures propagate and stop the worker. Logs contain job IDs and fixed classification codes, without handler exception messages, stack traces or payloads.

Backoff after failed attempt **n** is `min(60, 2 * 2**(n-1))` seconds: 2, 4, 8, 16, 32, 60, 60, … . It starts at the database-time failure/recovery update, rather than at the old lease expiry; delayed recovery therefore starts a fresh delay. No random jitter is applied, so synchronized failures may produce retry bursts. No transaction sleeps or waits through backoff. Other ready jobs can proceed.

POST/GET return `max_attempts`, `attempts`, `available_at`, `last_error_code` and `terminal_reason` alongside the previous response fields. `queued` with a future `available_at` means waiting for retry eligibility; there is no new status spelling. `last_error_code` is one of `retryable_error`, `permanent_error`, `unexpected_error`, `lease_expired` (or null); it retains the latest failure even after later success. `terminal_reason` is `permanent_error`, `attempts_exhausted`, or `legacy_failure` for failed jobs and null otherwise. `available_at` is meaningful for queued eligibility, not a completion timestamp or a promise of immediate execution.

## Ownership and crash recovery

Claiming uses `FOR UPDATE SKIP LOCKED` in a short transaction and commits before handler execution. Every worker completion/retry locks the row before testing PostgreSQL `clock_timestamp()`, then updates conditionally on ID, token, running status and strictly unexpired lease. Rejected updates raise `LeaseLostError` and change nothing. Every transition out of running clears both lease fields. A database constraint enforces the running/lease relationship. Expiry is evaluated after lock acquisition, not at transaction start; commit latency still consumes the lease and validity at commit is not promised.

Each poll first recovers up to 100 expired running rows with `FOR UPDATE SKIP LOCKED`, rechecking expiry after locking and atomically scheduling retry or terminal failure. Recovery commits independently before looking for a new claim, so exhaustion cannot strand an expired job. Concurrent recovery and completion serialize on the row lock. Old tokens cannot write success, permanent failure or retry after recovery. Recovery does not re-execute handlers directly. Workers sleep one second outside transactions when no job is claimable. Continued polling drains larger recovery backlogs without a separate service; locked rows can be skipped until a later poll.

There is **no heartbeat**. A paused or slow handler can outlive its lease and overlap a later execution. Lease timestamps and eligibility use database wall time; clock jumps affect timing. Creation timestamps still use transaction time and do not establish a global event sequence.

## At-least-once boundary and limitations

The engine follows an **at-least-once execution design with a finite attempt budget**, under continued polling and database availability. It does not promise exactly-once execution, eventual success, or even handler invocation for every committed claim: a worker can crash immediately after claiming and consume an attempt. Terminal failures are not automatically reopened.

A crash after an external effect but before completion, or expiry during execution, can duplicate effects. UUID tokens fence PostgreSQL job-state updates only; external systems do not enforce these tokens or attempt generations. Before adding real external effects, define downstream idempotency and a transactional delivery protocol. Bounded retries do not make effects exactly-once.

There is no jitter, heartbeat, cancellation, failure history, authentication, rate limiting, payload-size policy, retention or production hardening. Strict fairness and prompt completion are not guaranteed. Do not expose this service to untrusted networks. See [ADR 0001](docs/adr/0001-leases-and-fencing.md) and [ADR 0002](docs/adr/0002-bounded-retries.md).

## Validation

Phase-2C integration coverage additionally includes eight fresh API processes submitting identical/conflicting requests, independent keys, process restart persistence, terminal/running replays, header validation, real database failures and exact preservation of all existing columns during `0003 → 0004`.

Tests **delete every row in `synthetic_receiver_events`, `outbox_events` and `jobs`**, in that order; migration tests also downgrade and upgrade the test schema. Use a dedicated disposable PostgreSQL database. Never point these commands at development or production data. Tests require both `TEST_DATABASE=1` and an explicit `TEST_DATABASE_URL`, with driver `postgresql+psycopg`, a host, no URL query overrides, and a database name ending in `_test`. Missing or unsafe configuration aborts pytest before application import, without falling back to `DATABASE_URL`. The fixture also verifies the actual database name before deletion. The name check cannot prove that a database is disposable; setting these variables explicitly authorizes destruction of that database. Do not run workers or another test suite against it concurrently.

For example, start a fresh container without persistent storage (choose a free port):

```sh
docker run --detach --name rte-test-db \
  --env POSTGRES_USER=task --env POSTGRES_PASSWORD=task --env POSTGRES_DB=rte_test \
  --publish 127.0.0.1:55432:5432 --tmpfs /var/lib/postgresql/data postgres:16
# Wait for: docker exec rte-test-db pg_isready -U task -d rte_test
export TEST_DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:55432/rte_test
DATABASE_URL="$TEST_DATABASE_URL" uv run alembic upgrade head
TEST_DATABASE=1 uv run pytest
uv run ruff check .
uv run mypy src
docker compose config -q
docker rm --force rte-test-db
```

Integration coverage includes eight-process initial and scheduled recovery claims, bounded classified failures, success after retries, permanent failure, retry eligibility without sleeps, recovery exhaustion, stale success/failure/retry rejection, completion/recovery races, migration from 0001/0002 with legacy attempts above 3/10, and destructive-configuration rejection before deletion. See the [phase-2B validation record](docs/phase-2b-validation.md). GitHub Actions is configured; no successful remote CI run is claimed.

Phase-2D-1 adds PostgreSQL tests for atomic visibility/rollback, opt-in and payload filtering, stale/concurrent completions, recovery after completion failure, database constraints and data preservation through `0004 → 0005 → 0004`. See the [phase-2D-1 validation record](docs/phase-2d1-validation.md).

Phase-2D-2 adds real PostgreSQL coverage for eight concurrent dispatchers, stable-ID duplicate acceptance, process crash/recovery between receiver commit and ack, stale-token ack/retry rejection, real database failures and ambiguous commit responses, privacy filtering, and exact `0005 → 0006 → 0005` data preservation. Run just these tests with `TEST_DATABASE=1 uv run pytest -q tests/test_dispatcher.py tests/test_migrations.py` after setting the explicit disposable `TEST_DATABASE_URL` above. See the [phase-2D-2 validation record](docs/phase-2d2-validation.md).

Next phase: design a real transport adapter and durable recipient idempotency contract, including timeouts/uncertain responses and an explicit dead-letter/operator policy. Operational metrics, jitter, heartbeat and cancellation require follow-up semantics.
