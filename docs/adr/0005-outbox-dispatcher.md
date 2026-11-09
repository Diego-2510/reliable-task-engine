# ADR 0005: At-least-once outbox dispatcher and synthetic receiver

Status: Accepted for phase 2D-2

## Context

ADR 0004 commits job success and one stable outbox event together. Delivery must survive crashes without inventing a new event ID. A sender cannot atomically commit a remote receiver effect and its local acknowledgement; duplicate attempts in that gap require durable recipient deduplication.

## Decision

A separate polling process (`uv run python -m task_engine.dispatcher`) claims one eligible event at a time. It uses existing SQLAlchemy/psycopg/PostgreSQL dependencies, without a broker, HTTP transport or public outbox API. `DELIVERY_LEASE_SECONDS` is a positive integer, default 60, independent of worker `LEASE_SECONDS`.

Migration `0006` adds `delivery_state`, `delivery_attempts`, `next_attempt_at`, `delivery_token`, `delivery_lease_expires_at` and `delivered_at`. Existing events become due `pending` events with zero attempts and null lease/delivery timestamps. Their IDs, payloads and creation timestamps, and every jobs column, remain unchanged. Checks enforce the state vocabulary, nonnegative attempts, exactly two non-null lease fields only while delivering, and non-null `delivered_at` exactly while delivered. Partial indexes cover pending due-time ordering and delivering lease expiry. The eligibility OR predicate permits PostgreSQL to use these indexes; index usage and production-scale query performance are not claimed.

| Transition | Required condition | Effect |
| --- | --- | --- |
| `pending → delivering` | `next_attempt_at <= clock_timestamp()` | Fresh token, lease deadline, attempts + 1 |
| `delivering → delivering` | Expired lease | Fresh token and deadline, attempts + 1, same event ID |
| `delivering → delivered` | Receiver returned after commit, current token, unexpired lease | DB-time `delivered_at`, clear both lease fields |
| `delivering → pending` | Temporary receiver failure, current token, unexpired lease | DB-time backoff eligibility, clear both lease fields |
| `delivered` | Terminal for dispatcher | Never claim again |
| Missing/stale/expired owner | Ack or retry rejected | No writes; `DeliveryLeaseLostError` |

Attempts increase exclusively at a committed claim, including lease recovery. Crashes before receiver invocation can consume claims. There is no configured maximum attempt budget or dead-letter state. The database integer representation is finite; overflow is a database failure, not an automatic reset. Retry delay after attempt n is `min(60, 2 * 2**(n-1))` seconds, with exponentiation clamped before calculation. Thus delays are 2, 4, 8, 16, 32, 60, 60, … . No jitter or backoff sleep occurs inside a transaction. Expired claims are directly reclaimable, without an additional scheduled delay. Empty polls sleep one second outside transactions.

## Transaction boundaries and ownership

1. **Claim transaction.** Select due pending or expired delivering envelope columns using `FOR UPDATE SKIP LOCKED`. Recheck eligibility using PostgreSQL wall time in the locked UPDATE. Persist the fresh UUID token, lease deadline and incremented attempts together. Exit/commit before exposing the claim. No JSON payload is selected or passed to the receiver.
2. **Receiver transaction.** Use a new, short `Session.begin()` to `INSERT` only event ID, job ID and the fixed event type into `synthetic_receiver_events`, with database-wall-time `processed_at`. `ON CONFLICT (event_id) DO NOTHING` accepts duplicates without changing their metadata or original processed time. Return success only after this transaction commits. The row itself is the synthetic effect; primary-key insertion and that effect are atomic.
3. **Acknowledgement transaction.** Open another session and lock the outbox row. Check delivering state and token, then issue an UPDATE conditional on the same identity, state, token and `delivery_lease_expires_at > clock_timestamp()`. Set delivered and its DB timestamp, clearing lease metadata atomically. Commit before returning success.
4. **Retry transaction.** Only `RetryableDeliveryError` requests scheduling. The same locked fencing checks apply; set pending and `next_attempt_at = clock_timestamp() + backoff`, clearing the lease. Attempts do not change. A stale sender cannot postpone or overwrite a replacement owner's work.

Expiry checks happen after row-lock acquisition, including after a wait, rather than against transaction-start time or a Python clock. Validity is checked at the locked UPDATE, not at commit; the lock is retained through commit. Commit latency still consumes the lease. There is no heartbeat. Long or paused senders may overlap replacement senders. UUID tokens fence outbox writes; the receiver deliberately accepts duplicate attempts regardless of token validity and deduplicates only the stable event ID. Tokens are not monotonic external fencing generations.

The synthetic receiver uses **the same PostgreSQL instance in separate committed transactions**. It simulates an external reception boundary and is **not evidence of actual HTTP/network delivery** or independent receiver availability. It has no foreign keys to sender tables: its deduplication lifetime is independent of outbox/jobs retention. Test fixtures explicitly clear receiver rows before outbox and jobs. There are no external business effects or arbitrary destination connections.

## Crashes, database errors and uncertain outcomes

- Crash after claim, before receiver commit: delivering remains until lease expiry; a later claim retries the same event ID. A confirmed receiver transaction abort leaves no effect.
- Crash after receiver commit, before ack: outbox remains delivering, although the effect exists. Recovery redelivers the same event ID; receiver accepts the duplicate without a second row, then the new owner can ack.
- Ack database failure: a confirmed rollback leaves delivering and preserves the independently committed receiver effect. Recovery deduplicates the repeated attempt.
- Connection loss during claim, receiver or ack COMMIT can leave its outcome unknown. Database errors propagate from `run_once()` and are never interpreted as successful delivery or as a temporary synthetic receiver rejection. In particular, an uncertain receiver commit never triggers ack. A claim commit may have persisted without an exposed claim; an ack commit may already have persisted delivered. Reconnect and inspect durable state; continued polling recovers only eligible rows.
- The CLI stops with exit code 1 and a fixed `database_error` log for SQLAlchemy failures. It excludes SQL/driver diagnostics and exception tracebacks; the library propagates errors to its caller. Other ordinary unexpected exceptions and process termination also propagate, leaving the lease for recovery. Temporary failures are injected in tests; production payloads cannot request them.

No application log includes arbitrary payloads, submission keys, exception messages or credentials. The delivery envelope contains only event ID, job ID, fixed event type and token; the token is not persisted by the receiver. The receiver has only its four declared columns. Changing the approved event vocabulary requires an explicit schema/protocol change.

## Guarantees, limits and operations

This is an **at-least-once delivery-attempt design**, assuming continued polling, database availability and eventual receiver acceptance. It does not promise exactly-once delivery, eventual success, bounded latency, ordering, fairness, or a receiver call for every claim. Durable deduplication provides one stored synthetic effect per retained event ID; it does not prove general exactly-once effects for external services.

Permanently failing events retry indefinitely, consuming resources. There is no dead-letter policy, operator retry API, failure history, heartbeat, jitter, retention/expiry policy, metrics or production hardening. Fast failures can synchronize retries. Slow delivery and clock jumps can expire leases; wall-clock changes affect eligibility. Eight local processes do not establish distributed/production-scale performance or resilience across independent databases/networks. Deleting receiver records loses the deduplication guarantee for those IDs; future retention must coordinate replay horizons. Sender outbox IDs must remain stable.

Stop API, workers and dispatchers before migration. The additive ALTERs/check validation/index creation take locks and may rewrite existing data; plan downtime and capacity for large tables. No historical success events are invented. Downgrading to `0005` preserves existing job/outbox columns but discards delivery metadata and receiver deduplication permanently. Re-upgrading makes every retained outbox event pending again, allowing repeated effects. Do not use downgrade as a delivery reset.

## Validation and follow-up

Real PostgreSQL tests cover concurrent claims and independent events, duplicated receiver calls, abrupt process death after receiver commit, old-token fencing for both transitions, expiry after lock waits, bounded delayed retry, real receiver/ack transaction aborts, and commit-response loss after actual claim/receiver/ack commits. Migration tests compare every previous job/outbox column and existing indexes. Synthetic privacy markers stay outside receiver rows and logs. See [the phase validation record](../phase-2d2-validation.md) for executed results and limits.

Next phase: design an actual transport adapter and receiver contract (durable ID deduplication atomic with its effect, timeouts and ambiguous responses), plus an explicit dead-letter/operator policy. Real HTTP delivery requires its own tests and design decision.
