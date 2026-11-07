# Phase 2C validation record

Validated locally on 2026-11-07, starting from commit `0e6e33e492ce05bf1e144c583d26cd899efcfd20` on `main` with a clean working tree. No commits, remote mutations or Git identity changes were made.

## Environment and results

A new disposable PostgreSQL 16 Docker container used tmpfs storage and database `rte_phase2c_test` on loopback port 55439. Tests ran with explicit `TEST_DATABASE=1` and `TEST_DATABASE_URL`; the existing fail-closed configuration and actual database identity checks were retained. No application/development database was used. The temporary container was removed after validation.

Executed successfully:

- `uv sync --locked`
- `DATABASE_URL=<isolated test URL> uv run alembic upgrade head` (0001 through 0004)
- `TEST_DATABASE=1 TEST_DATABASE_URL=<isolated test URL> uv run pytest -q`: **86 passed**, 10 deprecation warnings, 5.79 seconds in the final run
- `uv run ruff check .`
- `uv run mypy src`: five source files, no issues
- `docker compose config -q`
- `git diff --check`

The warnings concern the installed Starlette/httpx TestClient integration and Alembic's existing path-separator configuration. Early test runs exposed errors in new test fixtures (cached UUID default replacement, SQL bind parsing and PostgreSQL parameter typing); those fixtures were corrected before the successful complete run.

## Evidence and review

Eight spawned processes each import the API afresh, establish independent connections and issue an ASGI HTTP request through TestClient after a barrier. Matching submissions yield exactly one 201 and seven 200 responses with one ID; distinct payloads yield one 201 and seven structured 409 responses with exactly one row. Distinct keys produce eight 201 responses and eight rows. Fresh processes also replay an earlier committed submission. No artificial sleeps were added.

Other tests cover nested object order, JSON type and array-order distinctions, omitted/default attempt limits, payload/limit conflicts, header validation, running/terminal state preservation, unrelated primary-key violations, and actual PostgreSQL INSERT/SELECT errors. The migration test compares every pre-existing column across 0003 to 0004 for all four statuses, including lease, attempt, error and timestamp data. All earlier retry, recovery, fencing and destructive-test safety tests pass.

Reviewed all changed/new source, tests, migration and documentation for API regressions and new secrets. No new secrets were found. Worker source, dependency lockfile, CI and test-database safety guard are unchanged. Reading instructions once and batching checks avoided redundant inspection without removing required verification.

## Limits and follow-up

Concurrency tests exercise independent API processes via ASGI TestClient, not a deployed TCP server or proxy. Remote CI, infrastructure header/database logging, production load, PostgreSQL failover and large-table migration duration were not verified. Local fresh-process tests establish persistence across API initialization, not database disaster recovery.

Keys remain global without authentication or tenant isolation, do not expire automatically, and disappear when their job is deleted or the migration is downgraded. Retention requires a future design. Submission idempotency does not deduplicate external effects and provides no exactly-once execution guarantee. Public production deployment requires authentication. The next useful task is to design downstream idempotency and transactional delivery before adding real external effects.
