# ADR 0004: Atomic job success and synthetic outbox intent

Status: Accepted for phase 2D-1

## Context

The existing PostgreSQL lease fences job transitions and bounded recovery. Submission idempotency deduplicates logical jobs. Neither protocol couples a successful job to a durable downstream message intent. Writing that intent in a separate transaction would allow success without an event or an event without success after a crash.

## Decision

Migration `0005` adds only `outbox_events` with non-null UUID primary key `id`, UUID FK `job_id → jobs.id`, `event_type`, JSONB `payload`, and database-wall-time TIMESTAMPTZ `created_at`. A check restricts the type to `job.succeeded.v1`; a unique constraint on `(job_id, event_type)` protects at most one such event per job. There is no conflict suppression: an insert violation aborts the completion transaction. The FK uses the default restrictive deletion behavior, so retention must eventually coordinate event and job deletion.

The event UUID is generated at insert and becomes a stable downstream idempotency identifier when committed. Future delivery retries must reuse this persisted ID. A rolled-back candidate ID is not an event and need not be reused by recovery. The uniqueness key describes one event per logical job, not one per attempt. `created_at` is not a global ordering guarantee.

The synthetic handler returns the fixed `SyntheticEventIntent.JOB_SUCCEEDED` only after successful processing when `emit_outbox_event` is the JSON boolean true. Missing/false/non-boolean values produce no intent. Existing handlers/mocks returning None remain compatible. `fail=true` still raises `PermanentJobError` before an intent can be returned. No handler performs external effects. The intent carries no arbitrary payload or destination; completion constructs exactly `{"job_id":"<UUID>","event_version":1}`. Submission keys, arbitrary URLs, credentials, personal fields and exception messages are never copied to the event or application logs.

## Transaction boundaries and fencing

1. Recovery retains its own short committed transaction. Claim then locks a due queued job with SKIP LOCKED, increments attempts and commits running status with a fresh lease before handler execution.
2. The handler runs outside a database transaction. Returning an intent performs no database insert.
3. Completion enters one `Session.begin()` transaction and first selects the job by ID with `FOR UPDATE`. It rejects missing jobs, non-running status and mismatched tokens.
4. After the lock is acquired, an UPDATE again requires ID, current token, running status and `lease_expires_at > clock_timestamp()`. Success sets succeeded, clears lease metadata and updates the timestamp. A zero-row result raises `LeaseLostError`.
5. Only after that successful UPDATE, an optional INSERT persists the fixed outbox event in the same session/transaction. Flush and commit errors propagate. Exiting the transaction commits both writes together; an insert error or transaction abort rolls back both. There is no intermediate commit or separate event session.

The job lock is held through insertion and commit. Other readers cannot observe either uncommitted change. Concurrent completion and recovery serialize on the row lock (recovery may skip a locked row). A worker waiting on that lock must pass expiry checks using PostgreSQL wall time after acquiring it. A stale worker cannot enqueue an event, including after a new owner has claimed the job. Repeating completion after success fails because the job is terminal.

Lease validity is checked at the locked UPDATE, not at commit. An insert or commit can finish after the checked lease deadline while the lock still prevents another transition. UUID lease tokens fence these PostgreSQL writes only; they are not external monotonic fencing tokens. Database clock changes still affect validity. There is no heartbeat or exactly-once handler-execution guarantee.

| Transition/result | Outbox effect |
| --- | --- |
| Valid running → succeeded, handler returned intent | One event, same commit |
| Valid running → succeeded, no intent | No event |
| Permanent failure → failed | No event |
| Retryable/unexpected failure → queued or exhausted failed | No event |
| Expired lease → recovery queued or exhausted failed | No event |
| Missing/stale/expired completion | No writes; LeaseLostError |
| Outbox insert error/confirmed transaction abort | Roll back success and event; prior running claim remains |
| Recovery and later successful opt-in execution | One event for that job |

Database completion failures retain the existing fatal-worker behavior. After a confirmed rollback, the committed running claim, attempt and lease remain intact. On expiry, recovery applies the existing delay and attempt budget. Connection loss during COMMIT can leave the outcome unknown to the worker: PostgreSQL still commits both writes or neither. Inspect persisted state to resolve that uncertainty; a committed succeeded job is terminal and cannot be reclaimed or completed again. A failed completion has consumed its claim; eventual success or enqueueing is not guaranteed, especially at the final attempt. Handler failures are classified exactly as before and never enqueue intents. Submission replay neither runs the handler nor creates events.

## Upgrade and downgrade

Stop all old workers and API instances before migrating `0004 → 0005`, then restart upgraded binaries only. Older workers could otherwise succeed opt-in jobs without enqueueing. The migration creates a table, constraints and their indexes; PostgreSQL may lock the referenced jobs table while establishing the FK. Plan downtime and lock acquisition accordingly. It does not update existing jobs, attempts, leases, timestamps or idempotency keys and does not backfill historical successes.

With API/workers stopped, `alembic downgrade 0004` drops only `outbox_events` and its constraints/indexes. It preserves every jobs row and column, but irreversibly discards event intents and identifiers. Re-upgrade creates an empty table and cannot reconstruct lost events. Integration migration checks run exclusively against an explicitly authorized disposable `_test` database.

## Delivery boundary and follow-up

This phase implements durable enqueueing only. There is no dispatcher, external service, delivery-attempt history, delivered marker, retention job or public outbox endpoint. No exactly-once delivery or production-ready integration is claimed.

A future dispatcher must support at-least-once delivery, retries and crash recovery while preserving each event ID. A recipient must deduplicate by that ID atomically with its own effect, or through its service's equivalent idempotency mechanism. Unique enqueueing does not make delivery exactly once. Dispatcher ownership, retry/acknowledgement semantics, ordering, retention and operations require a separate design before real external effects are introduced.

## Validation

PostgreSQL tests cover opt-in compatibility, failures/retries, no pre-handler event, stale and concurrent owners, simultaneous duplicate completions, actual row-lock waiting followed by controlled expiry, pre-commit visibility, real database failures both before and after INSERT, recovery after rollback, uniqueness/FK/type constraints, payload allowlisting, submission replay and exact jobs/index preservation across upgrade/downgrade. Lease/eligibility timestamps are adjusted relative to database time; no lease-duration sleeps are used. See [the validation record](../phase-2d1-validation.md) for actual commands and results.
