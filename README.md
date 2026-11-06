# Reliable Task Engine — phase 2A

A small PostgreSQL-backed job queue with a FastAPI service and independent polling workers. Python 3.12 and the checked-in uv lockfile define the application environment. Handlers are synthetic and perform no external business effects.

## Quickstart

Run `docker compose up --build --scale worker=2`. The migration runs before the API and workers. Submit a job with `curl -X POST localhost:8000/v1/jobs -H 'Content-Type: application/json' -d '{"payload":{"example":1}}'`, then retrieve its UUID with `curl localhost:8000/v1/jobs/<id>`. Payloads are JSON objects; `{"fail":true}` causes the synthetic handler to fail deterministically. Validation errors and missing jobs return `{"error":{"code":...,"message":...}}`.

For local development: `uv sync --locked`; start PostgreSQL, set `DATABASE_URL` if needed, run `uv run alembic upgrade head`, then `uv run uvicorn task_engine.api:app` and `uv run python -m task_engine.worker`. Default connection is `postgresql+psycopg://task:task@localhost:5432/task`. The Compose credentials are for local development only.

**Upgrading phase 1:** stop all old workers before applying migration `0002` and start only upgraded workers afterwards. Old workers perform unfenced updates and must never overlap the lease protocol. The migration gives existing `running` rows fresh tokens and already expired leases, preserving status, payload and attempts. It leaves other rows without leases. It takes a table lock; plan downtime for a large existing queue. Downgrading removes ownership protection and also requires stopping workers.

## Guarantees

Jobs have a UUID, JSONB object payload, constrained status (`queued`, `running`, `succeeded`, `failed`), nonnegative attempt count, timezone-aware timestamps, and nullable `lease_token` (UUID) and `lease_expires_at` (PostgreSQL TIMESTAMPTZ). `queued` is the existing API spelling for a pending job. A database constraint requires both lease fields for running jobs and neither for other states. Partial indexes cover the queued order and running lease expiry. Attempts count successful claims, including recovery claims, rather than submissions or completed handler executions.

Claiming uses `SELECT ... FOR UPDATE SKIP LOCKED` inside a short PostgreSQL transaction. It considers queued jobs and running jobs with expired leases, ordered by creation time and UUID. Each successful claim atomically sets running status, a new random token, a database-time expiry and an incremented attempt count. The transaction commits before the handler runs. Concurrent workers cannot acquire the same currently valid lease.

Success and failure use a separate transaction. Completion first locks the job row, then performs a conditional `UPDATE ... RETURNING` requiring the job ID, current token, running status and an expiry strictly later than PostgreSQL `clock_timestamp()`. Locking first ensures time is checked after any row-lock wait. A successful terminal transition clears both lease fields. No matching row raises `LeaseLostError`; the worker logs the discarded completion and keeps polling. Database errors propagate and terminate the worker.

## Lease lifecycle and recovery

| Transition | Condition | Effect |
| --- | --- | --- |
| `queued -> running` | First claim | New token, expiry and attempt + 1 |
| `running -> running` | Lease expiry <= database wall time | Recovery claim with new token, expiry and attempt + 1 |
| `running -> succeeded/failed` | Matching token and unexpired lease | Terminal status, cleared lease |
| Completion rejected | Old/wrong token, expired lease, terminal/missing job | No state change by that completion |

Workers poll empty queues once per second. The normal claim path recovers expired running jobs, including after a crash; there is no separate recovery service. Set `LEASE_SECONDS` to a positive integer (default `60`) in each worker environment; Compose forwards the same setting. Lease comparisons and expiry assignment use database wall time rather than worker clocks or transaction-start time. The lease starts at the claim update, so commit latency also consumes its duration.

There is **no heartbeat**. A slow or paused handler can outlive its lease and overlap a new handler after recovery. Expiry prevents even the original owner from completing before any reclaim has happened. Completion validity is checked when the locked conditional update executes; it does not promise that commit finishes before the deadline. Creation timestamps still use PostgreSQL transaction time and do not establish a globally ordered event sequence.

## Non-guarantees and at-least-once boundary

This is an at-least-once execution design under continued polling and database availability, **not exactly-once execution**. A crash after an external effect but before completion, or lease expiry during a handler, can cause duplicate effects. The random UUID fences job-state changes in PostgreSQL only. It is not a monotonically increasing fencing number understood by an external system. The attempt count is the per-job generation; this phase does not pass it to external systems or enforce it there.

There is no bounded retry policy, max-attempt limit, retry backoff or idempotency-key support yet. Handler exceptions produce terminal `failed` jobs when the lease is valid; failed jobs are not automatically retried. Repeated crashes or lease expiry can cause unbounded recovery claims. Recovery depends on workers and PostgreSQL being available, and strict fairness or prompt completion is not guaranteed. Database clock jumps can affect lease duration. No authentication, rate limiting, payload size policy, retention or production deployment hardening is provided. Do not expose this service to untrusted networks.

See [ADR 0001](docs/adr/0001-leases-and-fencing.md) for the ownership and transaction decisions.

## Validation

Tests **delete every row in `jobs`**; the migration test also downgrades and upgrades the test schema. Use a dedicated disposable PostgreSQL database. Never point these commands at development or production data. Tests require both `TEST_DATABASE=1` and an explicit `TEST_DATABASE_URL`, with driver `postgresql+psycopg`, a host, no URL query overrides, and a database name ending in `_test`. Missing or unsafe configuration aborts pytest before application import, without falling back to `DATABASE_URL`. The fixture also verifies the actual database name before deletion. The name check cannot prove that a database is disposable; setting these variables explicitly authorizes destruction of that database. Do not run workers or another test suite against it concurrently.

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

Integration coverage includes eight-process initial and recovery claiming, committed ownership before handler execution, deterministic expiry by database updates, stale and expired success/failure rejection, valid completion, terminal exclusion, lost-lease worker behavior, and migration of existing phase-1 rows. Configuration tests exercise the destructive-test guard. See the [local validation record](docs/phase-2a-validation.md) for executed checks and remaining limits. GitHub Actions is configured; no successful CI run is claimed here.

Phase 2B should define bounded retries, backoff and failure classification, followed by idempotency keys and a concrete strategy for external side effects. Heartbeats and cancellation require a separate explicit protocol if long handlers are needed.
