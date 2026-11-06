# ADR 0001: PostgreSQL lease ownership and fenced completion

- Status: Accepted for phase 2A
- Date: 2026-11-05

## Context

Phase 1 uses FastAPI, SQLAlchemy, PostgreSQL, Alembic and independent polling workers. Its queued-only `FOR UPDATE SKIP LOCKED` claim commits running status and an increased attempt count before executing the synthetic handler. Completion is a separate unconditional ownerless write. Crashed workers strand running jobs indefinitely. The API uses `queued`, not `pending`; retaining that spelling preserves its contract.

## Decision

Add only nullable UUID `lease_token` and TIMESTAMPTZ `lease_expires_at`. Keep `attempts` as the per-job generation. A check constraint requires complete lease metadata for running jobs and null metadata for queued/terminal jobs.

In one short transaction, select a queued or expired running job with `FOR UPDATE SKIP LOCKED`, then assign a fresh UUID, `clock_timestamp() + LEASE_SECONDS`, running status and attempts + 1. Commit before exposing the claim to the handler. PostgreSQL wall time defines expiry, including the equality boundary; transaction-start time could be stale after a lock wait. Do not use the worker clock.

Complete in a second transaction: first lock the row by ID, then issue an update conditional on ID, token, running status and unexpired lease. `RETURNING` detects loss of ownership; raise `LeaseLostError` on no result. A separate lock acquisition is deliberate: an update predicate can be evaluated before waiting for an unrelated row lock, so the time check must happen afterwards. Holding the lock prevents claim/completion races until commit. The successful update clears ownership metadata; a rejected update changes nothing. Both success and exception-driven failure use this path. The polling loop logs lease loss and continues; database failures remain fatal.

A partial index on running expiry supplements the existing queued index. No heartbeat, broker, recovery service or new framework is needed. Configure a positive integer `LEASE_SECONDS` per worker, defaulting to 60. Ordinary polling recovers expired jobs without a retry limit. A failed handler is terminal when its owner is still valid.

Migration `0002` adds the fields, backfills only legacy running rows with new tokens and expired database-time leases, then enforces the metadata constraint and adds the index. Attempts and existing job state are preserved until recovery. Stop old workers before migration: old binaries do not honor fencing. Schema downgrade also requires stopping workers and discards lease metadata.

## Consequences

Only the valid database owner can change a running job to succeeded or failed, even when expiry has occurred without a reclaim. Only one current lease exists for a job; multiple handler executions may nevertheless overlap. A handler can lose its lease during processing or commit latency. Validity is checked at the locked update, not at transaction commit. Database wall-clock adjustments affect timing.

A random UUID is a database ownership capability, not a monotonic external fencing token. Attempts provide a generation count but external systems do not enforce it in this phase. Crash-after-effect and expiry-before-completion windows permit repeated effects. There is no exactly-once guarantee or bounded retry guarantee. Continued worker polling and database availability are prerequisites for recovery. Max attempts, backoff, idempotency keys and external-effect protocols remain future work.

Destructive integration tests require explicit disposable database configuration, reject default/unsafe targets before importing the application, and verify the connected database before deletion. Expiry tests update timestamps directly instead of sleeping for leases to expire.
