# Phase 2D-1 validation

Date: 2026-11-08. Baseline: local `main` at `b2060c3`, initially clean.
Changes are uncommitted. No commits, pushes, PRs, GitHub operations or Git identity changes were performed. No remote CI result is claimed.

## Environment and database isolation

- `uv 0.12.17`, Python `3.12.3`, PostgreSQL `16.15`.
- Created a new `postgres:16` container named `rte-phase2d1-test-db`, with database `rte_phase2d1_test`, a loopback-only dynamically allocated port (`32787`) and tmpfs at `/var/lib/postgresql/data`. Docker inspection confirmed no volume mounts. No existing database/container was migrated or cleared.
- All pytest runs explicitly used `TEST_DATABASE=1` and `TEST_DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:32787/rte_phase2d1_test`. These are disposable local test credentials. Alembic used that same explicit URL as `DATABASE_URL`.
- The fixture retains configuration and actual `current_database()` checks before destructive statements. It now deletes outbox rows before jobs to respect the FK. Existing unsafe-configuration/sentinel tests passed.
- After the full suite, PostgreSQL reported database `rte_phase2d1_test`, revision `0005`, zero jobs and zero outbox events.
- Removed the newly created container after validation; its tmpfs test data was discarded. Development data was untouched.

## Executed checks

| Check | Actual result |
| --- | --- |
| `uv sync --locked` | Passed; lockfile unchanged, no added dependencies |
| `DATABASE_URL=<isolated test URL> uv run alembic upgrade head` | Exit 0; fresh schema upgraded to `0005` |
| `TEST_DATABASE=1 TEST_DATABASE_URL=<isolated test URL> uv run pytest -q` | **112 passed**, 14 warnings, 7.27 seconds |
| `uv run ruff check .` | Passed: All checks passed |
| `uv run mypy src` | Passed: no issues in 5 source files |
| `docker compose config -q` | Passed, exit 0 |
| `git diff --check` | Passed, exit 0; new files also checked with `git diff --no-index --check` |

The first focused run passed 28 tests and exposed a new migration-test setup error: SQLAlchemy parsed a colon in a literal JSON string as a bind parameter. The fixture SQL now passes JSON through an explicit bound parameter and CAST. The final full suite includes the corrected test. Ruff also requested multiline import formatting; that was corrected before the successful checks.

The 14 warnings are dependency/configuration deprecations: one Starlette/httpx TestClient warning and 13 Alembic `path_separator` warnings. No dependency/configuration scope expansion was made to silence them.

## Requirement coverage

`tests/test_outbox.py` adds 25 PostgreSQL cases; `tests/test_migrations.py` adds one migration case. The previous 86 cases remain in the full suite.

| Requirement | Evidence |
| --- | --- |
| A/B: opt-in success and legacy success | Real API submission and worker execution; boolean true creates exactly one event, absent/false/string/integer opt-in creates none; lease fields clear |
| C: permanent failure | Synthetic `fail=true` and classified permanent errors create no event |
| D/E: retry then success | Retryable and unexpected errors enqueue no event while delayed; successful second execution creates exactly one; exhausted failures create none |
| F: lease rejection | Wrong token, expiry without reclaim, recovery and missing job reject completion without state/event changes |
| G: concurrent workers | Two connections start at a barrier with old/new tokens; old loses, valid owner commits one event; simultaneous duplicate current-token completions also produce one winner |
| H: INSERT/transaction failure | Real PostgreSQL division-by-zero before and after the actual outbox INSERT aborts the transaction; an independent connection sees the original committed running claim, and all its columns match after rollback |
| I: recovery after rollback | Controlled expiry, recovery and due-time adjustment allow another successful execution; exactly one event remains and the old token still fails |
| J: regressions | All prior API, classified retry, recovery, process-contention, submission-idempotency and destructive-configuration tests pass |
| K: existing-schema migration | `0004 → 0005 → 0004` preserves every jobs column for 12 rows across all statuses, attempts 0/2/12, max_attempts 14, stored keys/nulls, timestamps and running leases; jobs indexes are unchanged; no event backfill; downgrade drops a populated outbox table only |
| L: privacy | Exact allowlisted event payload, fixed type, synthetic sensitive markers excluded from events/logs, no exception traceback logging, idempotent submission replay retains the existing event ID |

Additional tests demonstrate no event before handler return, stable event IDs across sessions, unique/FK/type constraints, rejected failed completion with intent, and both writes remaining invisible to independent readers until the shared commit. For the expiry-after-lock test, `pg_blocking_pids` proves the completing connection is waiting before the holder sets expiry to PostgreSQL wall time and releases the row. Thread barriers/events control overlap; database-relative timestamp updates control expiry and retry eligibility. No lease-duration or backoff sleeps are used.

## Review and limits

Reviewed the complete changed-file diff, including new migration, tests and documentation, for transaction boundaries, scope, sensitive data and accidental unrelated changes. The new event is always built from job ID and constant version; arbitrary submission contents are not forwarded. No private project information, real credentials, business handlers, new frameworks or external delivery services were added.

The handler remains outside transactions. The completion transaction locks first, checks current ownership/DB time, updates success, inserts the optional event and commits once. Confirmed transaction aborts roll back both writes. Connection loss around COMMIT can leave the worker uncertain about the outcome, but cannot produce a partial PostgreSQL commit. Validity is checked at the locked UPDATE rather than at commit.

Only durable enqueueing is implemented. There is no dispatcher, delivery status/attempt tracking or exactly-once delivery. Event-referencing jobs cannot be deleted until retention addresses the FK. Migration requires stopped old workers/API; downgrade irreversibly loses outbox events and IDs. Completion rollback preserves consumed attempts, so recovery at the limit can still fail terminally without an event.

Next work is a separately designed at-least-once dispatcher with persistent event IDs and recipient deduplication. No remote deployment or production-readiness claim is made.
