# ADR 0003: PostgreSQL-backed submission idempotency

Status: Accepted

## Context

Clients may lose a POST response and retry. UUID job IDs alone do not stop these retries from creating multiple logical jobs. Submission deduplication must survive process restarts and concurrent requests without affecting phase-2B retry, recovery or lease fencing.

## Decision

Accept an optional case-sensitive `Idempotency-Key` with 1–128 characters in ASCII `!` through `~`. Reject invalid or repeated headers with 422 using the existing error envelope, without echoing their value. Preserve `JobInput` and `JobOutput`; never expose the stored key. No key means a new job on every request (201).

Add nullable `jobs.idempotency_key` and a partial unique index over non-null keys in migration 0004. Existing rows receive NULL, with every other field preserved. No separate table or expiration machinery is needed.

Each submission uses one short transaction explicitly at READ COMMITTED. An INSERT targets only the key index with ON CONFLICT DO NOTHING RETURNING. A returned row is a new job (201). A conflicting insertion waits for the competing transaction; if it rolls back, PostgreSQL can insert our row. If it commits, a separate SELECT gets a fresh statement snapshot, returning the existing job and comparing its payload and max_attempts in that same snapshot. A single INSERT/SELECT CTE would not provide that visibility after waiting for an uncommitted winner.

Payload equality uses PostgreSQL JSONB equality: nested object key order is irrelevant; array order and JSON types are significant, and numerically equal JSON numbers compare equal. Omitted max_attempts normalizes to 3 through JobInput. A match returns 200 and current job state; a mismatch returns 409 with code idempotency_conflict. Replays never issue UPDATE, restart work or create attempts. Responses are returned only after the transaction exits and commits. Worker transitions are unchanged, and a worker may advance state after the response snapshot.

The unique index is the sole arbiter. Eight concurrent matching requests produce one 201, seven 200 and one job ID. With eight distinct payloads, one wins and seven receive 409. Different keys can insert independently. No application locks, caches or process-local ownership exist. Key associations survive process restarts. There is no concurrent job deletion API; any future deletion/retention design must specify how it interacts with replay and the insert/select window.

Unrelated integrity errors are not interpreted as key conflicts. SQLAlchemy errors, including commit or SELECT failures, yield a fixed 500 database_error envelope. Do not return/log driver details, which may contain keys. Bound parameter diagnostics are hidden on the engine. Request-header logging and external database logging need the same privacy policy.

## Consequences and limits

Keys are global in the MVP because user/tenant isolation does not exist. This API without authentication is not suitable for public production deployment. Introducing tenancy requires changing key scope and the unique index together.

There is no automatic key expiration; retention is a later design problem. Deleting a job or downgrading the schema discards its association. Migration requires a table lock and index build; stop API/workers and plan downtime for large queues.

Submission idempotency prevents duplicate logical jobs, not duplicate external side effects. There is no exactly-once execution guarantee. Claims, bounded retries, crash recovery and fencing retain their phase-2B semantics. A downstream idempotency/delivery protocol must be designed before real external effects.

## Validation

PostgreSQL integration tests exercise no-key submissions, defaults, nested object order, array/type differences, payload/limit conflicts, header boundaries, eight separate spawned processes (identical/conflicting/independent keys), fresh-process replay, running/terminal state preservation, unrelated unique violations, real INSERT/SELECT failures and full-column preservation from 0003. Existing phase-2B tests remain part of the full suite. Destructive tests retain the explicit authorization, `_test` database naming and actual database identity checks.
