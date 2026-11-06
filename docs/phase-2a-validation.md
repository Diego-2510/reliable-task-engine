# Phase 2A local implementation and validation record

Date: 2026-11-05. Repository: `/home/diego/Projekte/reliable-task-engine`.

## Inspected baseline

The current branch is `main`, with no commits. Phase-1 source, tests, migrations, Compose and CI files were already untracked at the start. `AGENTS.md` is ignored by the repository. No user files were staged or committed. Git author and committer resolved to Diego Ringleb and the configured `diego.ringleb2510@gmail.com`; identity settings were not changed. GitHub email verification was not queried; no remote operation was necessary.

Read the repository instructions, the three mandatory global efficiency/validation skills, all five application modules, the existing migration and Alembic configuration, three tests, README, Compose, Dockerfile, project dependencies and CI. Phase 1 creates `queued` jobs, claims them using a locked transaction, then executes a synthetic handler after commit. Completion used a separate ownerless read/write transaction. There was no lease or recovery. The old destructive-test fixture accepted any nonempty `TEST_DATABASE` and otherwise used the application's default development database.

The existing development Compose stack (API, two workers, PostgreSQL) was left running without migration or rebuild. All database writes for this work targeted a newly created disposable PostgreSQL 16 container with tmpfs storage, a fresh `rte_phase2a_test` database and dynamic loopback port `32786`.

## Files changed or added

- `src/task_engine/models.py`: lease fields and state/metadata check constraint.
- `src/task_engine/worker.py`: configurable leases, expired-job claiming, fenced completion, explicit lease-loss handling.
- `migrations/versions/0002_job_leases.py`: additive lease schema, legacy-running backfill, constraint and partial expiry index; reversible schema downgrade.
- `tests/conftest.py`: fail-closed database configuration, actual-database verification and guarded cleanup.
- `tests/test_engine.py`: claim, fencing, recovery, concurrency, transaction-time and handler-boundary coverage.
- `tests/test_safety.py`: unsafe configuration rejection and lease-duration validation.
- `tests/test_migrations.py`: 0001-to-0002 upgrade with all four existing states and recovery afterwards; downgrade exercised in setup.
- `compose.yaml`: configurable worker lease duration.
- `.github/workflows/ci.yml`: explicit disposable `_test` database and test URL.
- `README.md`: lifecycle, guarantees, recovery, limitations, upgrade and safe test instructions.
- `docs/adr/0001-leases-and-fencing.md`: ownership and transaction decisions.
- This validation record.

## Executed commands and results

Initial inspection included `git status --short`, `git branch --show-current`, `git ls-files`, `git log -3 --oneline`, `git config --show-origin --get-regexp '^(user\.|author\.|committer\.)'`, `git var GIT_AUTHOR_IDENT`, `git var GIT_COMMITTER_IDENT`, source/configuration reads and `docker ps`. `git log` confirmed that no initial commit exists. Toolchain: `uv 0.12.17`, Python 3.12, Docker Compose 2.37.1.

```sh
uv sync --locked
docker run --detach --name reliable-task-engine-phase2a-test \
  --env POSTGRES_USER=task --env POSTGRES_PASSWORD=task \
  --env POSTGRES_DB=rte_phase2a_test --publish 127.0.0.1::5432 \
  --tmpfs /var/lib/postgresql/data postgres:16
docker port reliable-task-engine-phase2a-test 5432
docker exec reliable-task-engine-phase2a-test pg_isready -U task -d rte_phase2a_test
```

Dependency synchronization passed. The container became ready on `127.0.0.1:32786`; an initial readiness probe during startup returned not-ready, followed by ready. No persistent volume was used.

```sh
DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:32786/rte_phase2a_test \
  uv run alembic upgrade head
TEST_DATABASE=1 \
  TEST_DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:32786/rte_phase2a_test \
  uv run pytest -q
uv run ruff check .
uv run mypy src
docker compose config -q
```

The baseline suite passed **3 tests** after introducing the test-database guard. Applying `0002` succeeded. Final validation passed **34 tests in 0.61 s**, Ruff passed, mypy reported no issues in all 5 source files, and Compose configuration validation passed. Integration tests include eight parallel processes for both an initial claim and an expired-job reclaim. Every expiry test changes the database timestamp directly; none waits for lease expiry. The migration integration test downgrades to `0001`, inserts legacy jobs, upgrades to `0002`, checks preserved state and recovery, and restores head in a finally block.

Four warnings remain: one existing Starlette/httpx TestClient deprecation and three Alembic `prepend_sys_path`/missing `path_separator` deprecations from the new migration exercise. An intermediate mypy literal-type error, import ordering issue and a JSON literal interpreted as a SQL bind parameter in the new migration test were corrected before final validation.

```sh
env -u TEST_DATABASE -u TEST_DATABASE_URL uv run pytest -q
```

This intentionally failed with exit code **4** and a configuration error, before collecting application tests. Unit tests also check missing URLs, development/production names, wrong flags, wrong drivers, missing hosts, malformed URLs and connection-query overrides. Only an explicit disposable database is accepted; the guard cannot independently prove that a database with an accepted name contains no valuable data.

A local `DATABASE_URL=... uv run python -` smoke harness started Uvicorn on an inherited ephemeral loopback socket and two independent `python -m task_engine.worker` subprocesses. Distinct PostgreSQL `application_name` entries confirmed both workers connected. Via HTTP it submitted 20 jobs, then checked 19 successes and 1 synthetic failure at attempt 1. A directly seeded expired running job recovered from attempt 4 to 5 and succeeded. All 21 terminal rows had null lease metadata. Startup/completion polling was bounded; lease expiry was controlled in SQL. All three subprocesses were stopped afterwards.

## Review, limitations and next work

The full current implementation and untracked-file diffs were reviewed. `git diff` cannot show these files because this repository has no tracked source baseline; review therefore also uses `git diff --no-index /dev/null <file>` without staging. `git diff --check` and corresponding whitespace checks for changed untracked files passed. The disposable test container was removed after checks; no development or production database was modified. No commits, pushes, PRs or GitHub changes were made. GitHub Actions was not run.

Remaining limits: no heartbeat, cancellation, bounded retries, backoff or idempotency keys; slow handlers can overlap after expiry, and external effects can repeat. UUID fencing protects database job state only. Validity is evaluated at the locked conditional update, not at commit. No external side-effect integration, fault injection across network/database outages, clock-jump testing, performance benchmark or production rolling upgrade was performed. Old workers must be stopped before migration. Recovery requires active polling and database availability.

Phase 2B should define max-attempt accounting (including recovery), retryable/permanent failures, scheduling and backoff, and exhausted-attempt behavior; add deterministic crash/retry tests. Plan idempotency keys and external-side-effect coordination explicitly before real business handlers. Long-handler support needs an explicit heartbeat/cancellation decision.

Efficiency measures used: independent inspection/checks were batched, unchanged instructions were read once, and validation focused on transaction ownership and destructive-test safety while retaining every requested check. No subagents or remote services were used.
