# ADR 0002: Bounded classified retries and delayed crash recovery

- Status: Accepted for phase 2B
- Date: 2026-11-06
- Supersedes ADR 0001's unbounded immediate recovery and terminal-on-all-exceptions policy.

## Context

Leases fence database completions but cannot prevent repeated crashes from consuming unlimited claims. Immediate recovery also lets failing jobs contend with healthy jobs. We need bounded execution, safe error metadata, and retry eligibility without another service or long transactions.

## Decision

Persist a positive per-job `max_attempts`, default 3. POST accepts strict integers 1..10; the database permits higher legacy values. Attempts remain successful committed claims, never schedules. `available_at` is non-null PostgreSQL TIMESTAMPTZ. Only queued rows due according to `clock_timestamp()` and below their stored limit may be claimed. Index queued rows by `(available_at, created_at, id)`; retain the old queue index for this additive migration.

Explicit `PermanentJobError` fails immediately. It subclasses ValueError to retain the existing synthetic `fail=true` behavior. Explicit `RetryableJobError` and unexpected ordinary handler exceptions schedule retry while budget remains, otherwise fail terminally. Catch Exception only around the handler; BaseException interrupts and database errors propagate. Persist and log only fixed classification codes, never handler messages, exception class names, payloads or tracebacks. Metadata stores the latest failure code, including across eventual success, and a terminal reason. No failure history is introduced.

Retry delay after failed attempt n is `min(60, 2 * 2**(n-1))` seconds, with no jitter. Clamp the exponent before computing so large legacy attempts cannot make backoff expensive. The delay starts at the database-time transition update. Eligibility is a lower bound, not an execution deadline. There are no sleeps within transactions. Synchronized retries remain a known limitation.

All success, permanent failure and retry transitions first lock the row by ID and then use an UPDATE requiring ID, token, running status and strictly future database-time expiry. The row lock must precede expiry evaluation, including when the transaction has waited for a lock. Leaving running clears token and expiry atomically. A rejected worker transition changes nothing and raises LeaseLostError.

Before each claim, the existing poll loop runs a separate short recovery transaction, selecting at most 100 expired running rows with SKIP LOCKED. After acquiring locks, recovery rechecks running status, token and expired database wall time. It schedules queued + backoff when attempts remain, or failed + attempts_exhausted when exhausted. It records lease_expired and invalidates ownership. It never increments attempts. A separate committed recovery step ensures exhausted jobs can become terminal even when no new claim is possible. Completion/recovery cannot overwrite one another under the row lock; a skipped row is revisited on later polls. Using the recovery time rather than the historical expiry avoids immediately replaying a long-abandoned job.

Migration 0003 is additive. Stop existing workers and API before upgrading and restart upgraded binaries only. Set legacy max_attempts to greatest(3, attempts), preserving historical claims with no additional grant. Set available_at to created_at. Preserve IDs, payloads, attempts, original timestamps and leases; preserve terminal statuses and mark old failed rows legacy_failure. Normalize exhausted queued rows to failed/attempts_exhausted so no unclaimable queued rows remain. Running rows can finish within their existing valid lease, including at their final attempt; expired rows follow recovery. The new positive constraint deliberately has no upper bound, allowing existing attempts above 10. Fixed-vocabulary nullable constraints protect metadata fields. Table/index work requires planned downtime for large queues. Downgrade drops retry metadata and cannot reverse terminal decisions; stop all workers/API before downgrading.

## Consequences

At most max_attempts claims are granted under the new protocol. Some claims can crash before invoking the handler, so neither successful execution nor a minimum number of handler invocations is guaranteed. Historical claims cannot be undone. There is no manual retry/reset endpoint.

This remains an at-least-once design with a finite attempt budget, not exactly-once execution. The UUID capability fences PostgreSQL writes only. Effects outside the database can duplicate across crash/lease windows; real effects require an explicit idempotency and delivery protocol. No external handlers, heartbeat, broker, cancellation or new recovery service are added. Continued polling and database availability are prerequisites; database clock jumps, lock contention, polling cadence and backlogs affect recovery and eligibility latency.

## Validation

Use only explicitly authorized disposable PostgreSQL _test databases. Exercise both migration paths with all statuses and attempt counts above 3/10. Test classification, delay formula and cap, controlled database eligibility updates, successful retries, exhaustion, crash recovery, stale tokens before/after recovery, eight-process contention and recovery/completion races. Test unsafe pytest configuration in subprocesses with a sentinel row to prove rejection precedes deletion. Use no long sleeps or real lease-expiry waits. See the phase-2B validation record for checks actually executed.
