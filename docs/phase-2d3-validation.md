# Phase 2D-3 validation

Date: 2026-11-11. Initial local branch `main`, clean, HEAD
`b991178121d405162477e872b2b4e580cc9b63f9`, matching the supplied baseline.
All work remains uncommitted. No commits, pushes, PRs, remote writes or Git
identity changes were performed. No remote CI result or deployment is claimed.

## Changed files

| File | Change |
| --- | --- |
| `migrations/versions/0007_delivery_dead_letter.py` | Six fields, dead-letter vocabulary/invariants, historical limit backfill, exhausted-pending conversion, monotonic-attempt trigger, guarded downgrade |
| `src/task_engine/models.py` | Matching six fields and database checks |
| `src/task_engine/dispatcher.py` | Budgeted claims, bounded exhausted-lease recovery, fenced permanent/exhausted failure, sanitized fatal unexpected errors |
| `src/task_engine/operator.py` | Local atomic replay CLI, bounded additional budget, overflow checks, fixed diagnostics/exit codes |
| `tests/test_dispatcher.py` | Extend stale/expired/competing-owner checks to permanent dead-letter transitions; preserve high-attempt backoff test with an explicit historical budget |
| `tests/test_dead_letter.py` | 57 cases covering retry limits, permanent failure, final-attempt crash/replay, recovery, replay concurrency, overflow, privacy, SQL resets and failed claims |
| `tests/test_migrations.py` | Two new 0007 preservation/downgrade cases; historical 0006 test seeds its old schema directly rather than calling the current dispatcher against old columns |
| `README.md` | Delivery policy, operator usage, stable IDs, ambiguous outcomes, migrations/downgrades and limits |
| `docs/adr/0006-delivery-dead-letter.md` | Protocol, invariants, transaction/fencing rationale, replay and operational caveats |
| `docs/phase-2d3-validation.md` | This execution record |

Worker, API, database configuration, fixture safety guards, dependency manifests,
lockfile, Compose services and earlier migration implementations remain unchanged.
No dependencies or frameworks were added.

## Updated protocol

`pending → delivering → delivered`. A valid retryable rejection returns to delayed
pending while budget remains; at the limit it becomes dead-letter. A valid
permanent rejection becomes dead-letter immediately. Exhausted expired delivering
leases become dead-letter through recovery without another claim. Only explicit
operator replay permits `dead_letter → pending`; delivered remains terminal.

Default limit is 5, including the first attempt. Only committed claims increment
historical attempts. Retry backoff remains 2, 4, 8, 16, 32, 60, 60, … seconds.
Recovery locks at most 100 expired exhausted rows per poll with SKIP LOCKED, rechecks
wall-time expiry after locking, and shares the short claim transaction. It commits
even when no new claim is available. Receiver invocation occurs after this commit;
receiver commit and owner acknowledgement/failure each have separate transactions.

Owner transitions lock first, then require delivering state, current token and a
strictly unexpired PostgreSQL `clock_timestamp()` at UPDATE. Final owners can
acknowledge success. Stale owners cannot acknowledge, retry, dead-letter or
replace an operator replay. UUID tokens fence sender database writes only.

Replay locks a dead letter and adds 1–1000 attempts to the stored maximum. It does
not reset attempts or event IDs. It preserves payload/envelope and receiver rows,
increments replay count, records DB-time replay/due timestamps and clears active
dead-letter/lease metadata atomically. Two simultaneous replays of the same dead
letter cannot both grant budget. Overflow checks cover both INTEGER budget and
replay count. At the absolute integer maximum, additional replay needs a future
schema/policy change.

Fixed error vocabulary: `retryable_error`, `permanent_error`, `lease_expired` or
NULL; fixed reasons: `permanent_error`, `attempts_exhausted`. Unknown receiver
exceptions and DB failures propagate from the library without acknowledgement.
CLI output is fixed and nonzero on errors; no exception text, arbitrary input,
payloads or credentials are emitted. Exit codes are documented in ADR 0006.

## Isolated environment and actual commands

Used uv 0.12.17, Python 3.12.3 and PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2).
Created a new local `postgres:16` container, independent of existing development
services, with explicitly disposable database `rte_phase2d3_test`. Port binding
was loopback-only, 55440; Docker inspection showed `Mounts=[]` and tmpfs at
`/var/lib/postgresql/data`.

```sh
uv sync --locked

docker run --detach --name rte-phase2d3-test-db \
  --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_USER=task -e POSTGRES_PASSWORD=task \
  -e POSTGRES_DB=rte_phase2d3_test \
  -p 127.0.0.1:55440:5432 postgres:16

DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:55440/rte_phase2d3_test \
  uv run alembic upgrade head

TEST_DATABASE=1 \
TEST_DATABASE_URL=postgresql+psycopg://task:task@127.0.0.1:55440/rte_phase2d3_test \
  uv run pytest -q --tb=short

uv run ruff check .
uv run mypy src
docker compose config -q
git diff --check
git status --short
git ls-files --others --exclude-standard
```

All database-touching pytest commands used the same explicit `TEST_DATABASE=1` and
`TEST_DATABASE_URL` above. Existing fail-closed guards reject missing authorization,
wrong database suffix/driver, query overrides and connection-target mismatches
before deleting data. Those safety and sentinel regression tests passed unchanged.
Development services were not stopped, migrated, cleared or used for tests.

| Check | Actual result |
| --- | --- |
| Initial Git state | Clean `main`, exact requested baseline |
| `uv sync --locked` | Exit 0; 36 packages resolved, 34 checked; lockfile unchanged |
| Isolated Alembic upgrade | Exit 0; fresh database upgraded through `0007` |
| Focused dispatcher suite | 62 passed in 5.44 s |
| Focused dead-letter and then-existing migration tests | 59 passed, 18 warnings in 11.51 s |
| Full suite after all behavioral changes | **234 passed, 26 warnings in 30.78 s** |
| `uv run ruff check .` | All checks passed |
| `uv run mypy src` | Success, no issues in 7 source files |
| `docker compose config -q` | Exit 0 |
| `git diff --check` | Exit 0 |
| Tracked and untracked file review | Complete, including new source, migration, tests and documentation |
| Changed-file secret scan and whitespace checks for untracked files | Passed; no real secrets found |

The warnings comprise one existing Starlette/httpx TestClient deprecation and
25 Alembic `path_separator` configuration warnings. No dependency changes were
made to suppress them. Initial lint/type findings were fixed (including argparse's
non-returning error override). The new raw-SQL migration fixture needed bound JSON
and an explicit varchar cast for a reused state parameter; these fixture failures
were resolved before the successful full run. No unresolved test failures remain.
Only subsequent source/test presentation edits moved a constant and wrapped/renamed
existing test declarations; final static checks also passed.

After the complete suite, an explicit query in the new container reported:
`rte_phase2d3_test|0007|0|0|0` (database, revision, jobs, outbox, receiver counts).
The disposable test container was then removed, discarding only its tmpfs data.

## Required coverage and preservation evidence

| Requirement | Evidence |
| --- | --- |
| A: retryable failure schedules backoff | Existing dispatcher DB-time delay test plus retry-to-budget test; due time is controlled without sleeping |
| B: final retryable failure dead-letters | Five attempts stop at default limit; final INTEGER-limit retry also dead-letters |
| C: permanent error immediately dead-letters | Injected `PermanentDeliveryError` at attempt 1 with no receiver effect; private error marker absent from logs |
| D: delivered is terminal | Existing claim/reclaim regression plus final-attempt acknowledgement and rejected replay |
| E: eight competing dispatchers | Existing barrier-synchronized independent processes test pending and expired claims with one valid token; distinct-event regressions also pass |
| F: exhaustion recovery without claim | Expired final claim becomes dead-letter on an empty poll; attempts unchanged; bounded/locked-row recovery also tested |
| G: stale ownership | Ack, retry and permanent dead-letter all reject wrong/missing/expired/completed/superseded ownership, including after recovery and replay; real lock waits prove expiry is checked after acquisition |
| H: final-attempt receiver-commit crash | Child commits receiver effect then abruptly exits 23 before ack; recovery dead-letters; explicit replay retains one original receiver row and succeeds with the same event ID |
| I: stable replay identity/history | Exact event envelope/payload/created-time checks and receiver rows including original `processed_at` remain unchanged |
| J: bounded additional budget | Attempts 1 / limit 1 replayed +3 grants only attempts 2–4; subsequent replay increments audit without reset. Early permanent failure at 1 / 5 replayed +3 becomes 1 / 8 |
| K: simultaneous replay | Two independent DB sessions released by a barrier yield one committed replay and one state rejection; exactly one increment/budget grant |
| L: invalid replay state | Pending, delivering and delivered reject atomically; repeated CLI replay exits 4; missing UUID exits 3 |
| M: constraints/configuration | Lease/state/dead-letter metadata checks, fixed vocabularies, positive limit, nonnegative counters, monotonic-attempt SQL trigger, invalid replay budgets/types and existing delivery-lease configuration cases |
| N: 0006 → 0007 preservation | 20 historical events and jobs, across pending/delivered/valid-delivering/expired-delivering and attempts 0/1/5/12/2147483647; see details below |
| O: regressions | Full 234-test suite includes worker, API, idempotency, outbox, earlier migrations, concurrency and database protection tests |
| P: privacy | Receiver envelope/rows remain allowlisted; retry/permanent/unknown/DB diagnostics and CLI invalid inputs contain no private markers; no raw exception/traceback output |
| Q: integer limits | Claim from 2147483646 reaches exactly 2147483647; final ack/retry/recovery work without another claim; exact-boundary replay succeeds and overflowing budget/counter changes are rejected atomically |

The new preservation test compares every pre-existing outbox column, every jobs
column, every receiver column and all pre-existing indexes. The only historical
state change is the three exhausted pending rows becoming dead-letter. Old IDs,
JSON values, creation/eligibility/delivery timestamps, counters, tokens and lease
deadlines otherwise match exactly. New limits equal `GREATEST(5, old_attempts)`;
new replay fields default to zero/NULL and historical error codes remain NULL.
All five valid historical delivering claims, including the INTEGER maximum, can
still acknowledge. The three expired exhausted historical claims recover to
dead-letter without increment. Receiver records remain identical
as SQL rows throughout these sender changes.

Downgrade with dead letters raises an error and transactionally preserves revision
0007 and all rows. A separate test populates delivered, pending-after-replay and
active-delivering rows, then compares every 0006 column and all receiver rows
through successful downgrade. Re-upgrade demonstrates the documented loss of
replay audit and unused added budget; it does not reset historical attempts.

Existing tests inject lost commit responses after actual DBAPI claim, receiver and
ack commits, never returning confirmed success for those errors. New replay tests
likewise inject response loss after actual commit: the CLI reports database error,
the committed pending/budget state is visible, and repeating replay rejects it.
Real SQL errors before/after claim UPDATE prove aborted claims do not consume
attempts; a replay UPDATE error rolls back all replay fields. No lease-duration or
backoff sleeps were added. Tests use DB-relative time updates, process/thread
barriers and row-lock observation.

## Crash semantics, risks and next milestone

A final-attempt crash after receiver commit is **AMBIGUOUS**: the effect can exist
while the sender later records dead-letter. Dead-letter means automatic attempts
stopped, not that the receiver did nothing. Replay keeps the original event UUID
so the receiver can deduplicate the committed effect; changing IDs would bypass
that protection. Unknown COMMIT outcomes require reconnecting and inspecting
state, including for operator replay. No exactly-once, eventual-success, ordering,
fairness, latency or per-claim receiver-invocation guarantee is made.

The synthetic receiver shares the sender's PostgreSQL instance but commits in a
separate transaction. Its entire effect is its deduplicated row; it does not prove
real transport, independently available recipients or general external-effect
atomicity. This run did not verify physical network cuts during COMMIT,
independent database failures, host/power loss, wall-clock jumps or production-scale
query/lock behavior. Lost commit responses use controlled fault injection after
real commits, and actual abrupt process death is tested separately.

Operational risks remain: downtime/table locks and full-table work for migration;
unused budget/audit loss on permitted downgrade; finite INTEGER headroom;
deduplication retention across replay horizons; clock jumps and overlapping expired
senders; no heartbeat, jitter, retention, append-only audit, metrics or production
hardening. Library callers receive raw errors and must sanitize their own logs.
No significant architecture expansion or unresolved fencing/atomicity issue was
identified within the defined scope.

Next recommended milestone: define a real transport/recipient idempotency contract
with timeouts, ambiguous responses, durable deduplication atomic with effects,
retention across replay horizons and operator visibility before adding real effects.

Work reused the required skill guidance and targeted reads, batched independent
checks and retained all mandatory validation. No subagents were spawned, no new
dependencies were added, and no empty long-running polling loop was needed.
