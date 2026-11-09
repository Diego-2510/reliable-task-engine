# Phase 2D-2 validation

Date: 2026-11-09. Baseline: local `main` at `b5455b258557894292aed250710bf6636e688381`, initially clean. All changes remain uncommitted. No commits, pushes, PRs, remote modifications or Git identity changes were performed. No remote CI result is claimed.

## Implementation and files

| File | Change |
| --- | --- |
| `migrations/versions/0006_outbox_delivery.py` | Six additive outbox columns, state/attempt/lease/timestamp checks, two partial indexes, independent receiver table and reversible schema migration |
| `src/task_engine/models.py` | Matching delivery fields, indexes/checks and four-column `SyntheticReceiverEvent` |
| `src/task_engine/dispatcher.py` | Separate CLI; claim, receiver, fenced ack/retry, capped backoff and sanitized fatal DB-error handling |
| `tests/conftest.py` | Receiver cleanup before outbox/jobs; all existing database safety checks retained |
| `tests/test_dispatcher.py` | 55 cases, including real PostgreSQL concurrency, crashes, errors and privacy |
| `tests/test_migrations.py` | One additional preservation/upgrade/downgrade case; previous tests unchanged |
| `README.md` | Startup/test instructions, protocol, recovery, stable IDs, non-guarantees and migration risks |
| `docs/adr/0005-outbox-dispatcher.md` | Delivery decision and transaction/error semantics |
| `docs/phase-2d2-validation.md` | This execution record |

The worker, API, existing outbox tests, unsafe-configuration tests, dependency manifests/lockfile, Compose services and Git configuration were not changed. No frameworks, brokers, real destinations or credentials were added.

## State and transaction boundaries

`pending → delivering → delivered`; temporary receiver rejection returns a valid delivering claim to delayed pending. Expired delivering leases can be reclaimed directly. Only committed claims increment attempts and assign a fresh token; the persisted event ID is unchanged. Every ack/retry locks first, then checks token, delivering state and strictly unexpired PostgreSQL `clock_timestamp()` at its conditional UPDATE. Both lease fields clear together on exit from delivering. Delivered is terminal for the dispatcher.

Claim commits before receiver invocation. Receiver insertion/deduplication commits in a second short transaction before ack opens a third transaction. The synthetic effect is the receiver row itself, protected by its event-ID primary key and `ON CONFLICT DO NOTHING`. The receiver has no sender FKs and stores no arbitrary JSON or ownership token. Delayed retries use database wall time and 2, 4, 8, 16, 32, 60, 60, … second backoff without sleeping in transactions. Empty polling sleeps outside transactions.

The synthetic receiver uses **the same PostgreSQL instance in separate transactions**. It simulates an external receiving boundary and proves no HTTP/network transport or independently available receiver service.

## Environment and isolation

- `uv 0.12.17`, Python `3.12.3`, PostgreSQL `16.15 (Debian 16.15-1.pgdg13+2)`.
- Created a fresh `postgres:16` container named `rte-phase2d2-test-db`, with database `rte_phase2d2_test`, bound only to `127.0.0.1:55439`. Docker inspection reported `Mounts=[]` and tmpfs `/var/lib/postgresql/data`. Existing development containers remained running and were not migrated or cleared.
- All pytest runs explicitly used `TEST_DATABASE=1 TEST_DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:55439/rte_phase2d2_test`. Migration and CLI smoke used that same URL as `DATABASE_URL`. These are disposable local test credentials.
- The original environment/URL safety checks, `_test` suffix, prohibited query overrides and actual `current_database()` check before deletion remain intact. Existing destructive-configuration/sentinel tests passed.
- Immediately after the complete suite: database revision `0006`, zero jobs, zero outbox events, zero receiver rows. A subsequent CLI smoke inserted one synthetic job/event in this same disposable database and verified one delivered event, one attempt and one receiver effect, then terminated the CLI process.
- Removed only the newly created test container after checks, discarding its tmpfs data. No development data was deleted.

## Executed checks and actual results

| Check | Result |
| --- | --- |
| Initial `git status --short`, branch, HEAD | Clean; `main`; exact requested baseline |
| `uv sync --locked` | Exit 0; dependencies already satisfied; lockfile unchanged |
| Initial `DATABASE_URL=<isolated URL> uv run alembic upgrade head` | Exit 0; initial schema at `0005` |
| Unmodified baseline `TEST_DATABASE=1 TEST_DATABASE_URL=<isolated URL> uv run pytest -q` | **112 passed**, 14 warnings, 14.79 s |
| `DATABASE_URL=<isolated URL> uv run alembic upgrade head` after implementation | Exit 0; `0005 → 0006` on isolated test DB |
| Focused `pytest -q tests/test_dispatcher.py` | **55 passed**, 5.39 s |
| Focused `pytest -q tests/test_migrations.py` | **5 passed**, 18 warnings, 2.03 s |
| Complete `TEST_DATABASE=1 TEST_DATABASE_URL=<isolated URL> uv run pytest -q` | **168 passed**, 19 warnings, 19.78 s |
| `uv run ruff check .` | Passed: All checks passed |
| `uv run mypy src` | Passed: no issues in 6 source files |
| `docker compose config -q` | Exit 0 |
| `git diff --check` and whitespace checks for every new file | Passed |
| CLI subprocess `uv run python -m task_engine.dispatcher` | Delivered seeded event; attempts 1; receiver effects 1; sensitive synthetic marker absent from output |
| Full changed-file diff and secret review | Completed, including new files; no real secrets or unrelated changes found |

The 19 warnings are existing dependency/configuration deprecations: one Starlette/httpx TestClient warning and 18 Alembic `path_separator` warnings, increased by the new migration test's commands. Ruff initially identified one unused import in the new tests; it was removed before the successful checks. No dependency/configuration changes were made to silence warnings.

## Required coverage and evidence

| Requirement | Evidence |
| --- | --- |
| A: pending delivered and acknowledged | `test_pending_delivered_and_never_reclaimed`: DB-time receiver/delivery timestamps, cleared lease, one attempt; original event columns unchanged |
| B: delivered never reclaimed | Same test forces `next_attempt_at` into the past and still observes no claim/invocation |
| C: eight contenders, one active owner | Eight forked processes with separate connections and start barrier claim one pending event once; repeated for one expired delivering lease, with a fresh token |
| D: durable receiver deduplication | Eight concurrent receiver transactions and later repeat calls retain one row and original processed time; a different event ID, even for the same job, creates an independent row |
| E: crash after receiver commit | A child claims and commits the receiver, then abruptly calls `os._exit(23)` before ack. Parent observes delivering, attempt 1, effect 1. After controlled DB-relative expiry, redelivery commits delivered at attempt 2, with identical receiver row and event ID |
| F: stale token cannot ack/retry | Both transitions reject reclaimed, expired, wrong-token, missing and delivered rows with no state changes; lock-wait tests prove expiry is checked after lock acquisition |
| G: temporary error/backoff | Injected `RetryableDeliveryError` yields delayed pending with cleared lease, no effect and unchanged attempt count; controlled due time permits successful redelivery. Attempts above 1000 remain retryable with 60-second cap |
| H: receiver DB error never acks | Real PostgreSQL division-by-zero before and after INSERT aborts receiver transaction; error propagates, outbox stays delivering, no receiver row. Recovery later succeeds |
| I: independent events | Eight processes claim eight distinct events with distinct valid tokens; parallel receipt/ack completes once per ID. Separately, eight processes run the entire dispatcher operation on eight events, each delivered at attempt 1 |
| J: migration preservation | 12 jobs across all statuses, attempts 0/2/12, stored/null keys, payloads, timestamps and leases; 3 existing events with arbitrary legacy JSON and historical timestamps. Every previous column and index survives upgrade and downgrade exactly. Upgrade initializes only new fields; populated delivered/delivering/pending states can downgrade, and re-upgrade resets delivery state/deduplication as documented |
| K: previous 112 tests retained | Baseline 112 pass before implementation; final suite contains these unchanged cases plus 56 new cases. Original test bodies/intentions remain intact |
| L: sensitive values excluded | Synthetic privacy markers injected into job and outbox JSON and error messages remain absent from delivery envelopes, four-column receiver rows and application logs; fatal CLI DB diagnostics are fixed and contain no traceback |

Additional checks prove claim visibility and zero checked-out connections before receiver invocation, receiver visibility before independent ack, skip-locked progress around a held outbox lock, one winner for simultaneous same-token transitions, and database constraint enforcement. A real ack transaction abort preserves the receiver commit and recovers through duplicate acceptance.

Ambiguous commits are tested by fault injection immediately **after actual DBAPI commits** at claim, receiver and ack. Each error propagates rather than returning confirmed success. A committed but unreported claim remains recoverable; an unreported receiver commit leaves delivering with one effect and no ack; an unreported ack may already be delivered and remains unclaimable. This models lost commit confirmation; it does not physically sever a network connection.

Concurrency uses process/thread barriers and row-lock observation via `pg_blocking_pids`. Lease expiry and retry due times are updated relative to PostgreSQL wall time. No lease-duration or backoff sleeps are used. CLI smoke has a bounded startup deadline, independent of lease validity.

## Review, limits and next phase

The complete tracked and untracked diff was reviewed for ownership conditions, transaction ordering, data preservation, scope and secrets. Supplementary pattern scanning checked changed files for common private-key/token formats; fixtures contain only clearly synthetic markers and documented disposable credentials. The CLI suppresses raw database diagnostics while preserving a fixed critical log and nonzero exit. Library callers still receive database exceptions and must avoid logging their potentially sensitive diagnostics themselves.

No exactly-once delivery or eventual-success guarantee is claimed. Continued polling/availability is required. Permanently failing events have unlimited policy retries, with no dead-letter handling; the integer counter is finite and overflow would propagate as a DB error. No heartbeat, jitter, ordering/fairness promise, retention, independent receiver service, real HTTP transport, metrics or production hardening was added. Removing deduplication rows or downgrading/re-upgrading can allow repeated effects. Fencing is checked at the locked UPDATE, not at commit, and covers outbox writes only.

Not verified: actual HTTP/network delivery, independent database/network failures, physical connection-loss during COMMIT, host/power loss, wall-clock jumps, production-volume performance/lock duration, remote CI or deployment. The local tests do verify real PostgreSQL aborts, actual process death, committed persistence, concurrency and injected lost commit responses.

Next phase: specify a real transport/recipient idempotency contract with timeouts and ambiguous responses, then implement it with integration tests; define dead-letter/operator recovery policy before production integration.

Work used targeted reads and batched independent checks, with no delegation, repeated empty polling or added dependencies. All mandatory validation was retained.
