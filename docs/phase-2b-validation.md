# Phase 2B implementation and local validation

Date: 2026-11-06. Work started on clean `main`. No commit, push, PR, publication or Git identity change was made. Existing local Compose API/workers/development database were left untouched.

## Changed files

- `src/task_engine/models.py`: stored attempt budget, eligibility and constrained metadata.
- `src/task_engine/api.py`: optional strict 1..10 budget and retry fields in POST/GET responses.
- `src/task_engine/worker.py`: explicit errors, bounded backoff, fenced retry transitions and polling recovery.
- `migrations/versions/0003_bounded_retries.py`: additive schema, backfill, constraints and due-queue index.
- `tests/test_engine.py`: retain phase-2A ownership/claim tests with delayed recovery semantics.
- `tests/test_migrations.py`: both legacy upgrade paths, all statuses, attempts 0/1/3/12 and data preservation.
- `tests/test_retries.py`: classifications, eligibility, bounds, interruption, races and stale retries.
- `tests/test_safety.py`: subprocess checks prove unsafe configuration aborts before deletion.
- `README.md`, `docs/adr/0002-bounded-retries.md`, this record: retry contract, rollout and limits.

## Semantics and backfill

Only committed queued-to-running claims increment attempts. Only due queued jobs below their stored limit can be claimed. Permanent errors fail immediately; retryable and unexpected ordinary handler errors schedule a delayed retry or fail on exhaustion. Lease recovery uses the same delay and limit without incrementing attempts. All transitions leaving running clear ownership; all worker completions/retries lock before checking PostgreSQL expiry and require the current token.

Default budget is 3. Delay after attempt n is min(60, 2 * 2**(n-1)) seconds, measured from failure/recovery time. Fixed metadata codes contain no handler messages or payloads. Success retains the previous failure code for context; terminal reason is null on success/queued/running.

Backfill preserves IDs, payloads, attempts, original timestamps and leases. Stored legacy budget is greatest(3, attempts), including counts above the API limit 10. Exhausted queued rows become failed/attempts_exhausted; running rows keep their lease until completion/expiry. Existing failed rows receive legacy_failure. No historical claims are erased or extra claims granted. Stop all old workers/API before upgrading; downtime is required for table/index work. A downgrade discards retry fields and does not undo terminal decisions.

## Executed validation

Dedicated ephemeral PostgreSQL 16 container `rte-phase2b-test-db`, tmpfs storage, database `rte_phase2b_test`, bound only to `127.0.0.1:55433`. Destructive tests explicitly used TEST_DATABASE=1 and TEST_DATABASE_URL. The development database on port 5432 was not used. Application DATABASE_URL was overridden explicitly for migrations and smoke execution. The temporary container and its tmpfs data were removed after validation.

- `uv sync --locked`: passed, Python 3.12.3 environment.
- Baseline suite before implementation: 34 passed.
- `uv run alembic upgrade head` on the disposable database: passed through 0003. Integration tests also exercised 0001 -> head and 0002 -> head with existing data, constraints and index checks.
- Complete `uv run pytest -q --tb=short`: 59 passed. No real lease-expiry sleeps; eligibility/expiry are moved relative to PostgreSQL wall time by explicit test updates. Eight-process contention covers initial claims, recovery scheduling and subsequent exclusive claims. A separate completion/recovery race preserves the recovered state. Unsafe pytest subprocesses exit 4 with a sentinel row intact.
- `uv run ruff check .`: passed.
- `uv run mypy src`: passed (5 source files).
- `docker compose config -q`: passed.
- `git diff --check`: passed.
- Smoke: launched two independent `python -m task_engine.worker` subprocesses against the disposable database. Both remained alive while 40 jobs reached terminal status: 30 succeeded, 10 permanently failed; each consumed exactly one attempt and cleared its lease. Processes were stopped afterwards. Readiness polling was bounded and outside database transactions.

The suite emits existing FastAPI/Starlette TestClient and Alembic configuration deprecation warnings. No dependency changes were needed. GitHub Actions was read but no remote run was triggered or verified.

## Limits and unverified cases

No jitter, heartbeat, external effects or exactly-once guarantee. Execution remains at-least-once in design with a finite claim budget; a crash before handler invocation can consume an attempt, and successful delivery is not promised. External effects would still be vulnerable to duplication. Continued polling and database availability are prerequisites for recovery. Recovery batches at most 100 rows per poll; strict fairness is not guaranteed.

Not verified: large-table migration duration, load/soak behavior, real network partitions, database wall-clock jumps, actual machine/process crashes during external effects, or production rollout. Crash semantics were tested by controlled lease expiry. The smoke used the existing synthetic success/permanent-failure handler; temporary failures were injected only in integration tests. No claim of a successful GitHub Actions run is made.

## Concrete phase-2C candidates

1. Add submission idempotency keys with a unique database constraint, a defined replay response and concurrent duplicate-POST tests.
2. Before introducing external effects, specify downstream idempotency keys and a transactional outbox/delivery protocol; test crash-after-effect/before-completion duplicates explicitly.
3. Add operational counts for queued/delayed/running/exhausted jobs and structured failure-code metrics, without payloads or exception text. Consider jitter under load with a separately documented policy and deterministic tests.

Required skills were read once; independent reads/checks were batched and existing context reused. All requested validation was retained; no token-savings percentage is claimed.
