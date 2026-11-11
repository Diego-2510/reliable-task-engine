# ADR 0006: Bounded delivery, dead letters and explicit replay

Status: Accepted for phase 2D-3

## Context and decision

ADR 0005's unlimited delivery attempts can consume resources indefinitely. Delivery
now has its own positive `max_delivery_attempts`, default 5, including the initial
claim. It is independent of the job execution budget. No new transport, framework,
public management endpoint or real external effect is introduced.

Migration `0007` adds that limit, nullable `delivery_last_error_code`, nullable
`dead_letter_reason` and `dead_lettered_at`, nonnegative `replay_count` default 0,
and nullable `last_replayed_at`. The new state is `dead_letter`. PostgreSQL checks
retain the existing lease/delivered timestamp invariants and require a reason and
timestamp exactly in dead-letter state. Checks restrict error codes to
`retryable_error`, `permanent_error`, `lease_expired`, and reasons to
`attempts_exhausted`, `permanent_error`. NULL means no recorded classification.
A database trigger rejects decreasing `delivery_attempts`; CHECK alone cannot
compare the old and new versions of a row. No exception text is stored.

## State machine and transaction boundaries

| Transition | Condition and result |
| --- | --- |
| Due `pending → delivering` | Attempts below stored limit; fresh token/lease; attempts + 1 |
| Expired `delivering → delivering` | Budget remains; fresh token/lease; attempts + 1 |
| Valid `delivering → delivered` | Receiver confirmed success; allowed on final attempt |
| Valid `delivering → pending` | Retryable rejection with budget remaining; delayed eligibility |
| Valid `delivering → dead_letter` | Permanent rejection immediately, or retryable rejection at limit |
| Expired exhausted `delivering → dead_letter` | Bounded recovery; `lease_expired` / `attempts_exhausted`; no claim or increment |
| `dead_letter → pending` | Only explicit operator replay; add budget, preserve historical attempts |
| `delivered` | Terminal; never claim or replay |

Every poll first selects up to 100 exhausted expired claims ordered by expiry/ID
with `FOR UPDATE SKIP LOCKED`, then rechecks state, budget and PostgreSQL wall-time
expiry in a separate locked UPDATE. It clears leases and records dead-letter
metadata. The bounded recovery and optional one-event claim share a short
transaction. Recovery commits even when no claimable row exists; an error aborts
the transaction visibly and a later poll can retry it. Locked rows are skipped;
continued polling drains larger backlogs. Non-exhausted expired leases remain
directly reclaimable. Eligibility is rechecked after locking, and only a committed
claim increments attempts. `attempts < max_delivery_attempts` makes the increment
safe even at PostgreSQL INTEGER's maximum of 2147483647.

Claim commits before receiver invocation. The synthetic receiver commits in its
own transaction, and acknowledgement/failure uses a third transaction. No
transaction spans the receiver call. Every owner transition obtains the row lock,
checks delivering state and token, then performs an UPDATE requiring the same
identity/state/token and `delivery_lease_expires_at > clock_timestamp()`. Expiry
is checked after lock acquisition, including after lock waits. Stale/expired owners
cannot acknowledge, retry, dead-letter or overwrite replay. Validity is checked
at the locked UPDATE, not at commit; commit latency may consume the lease.

`RetryableDeliveryError` requests delayed retry only when budget remains, otherwise
it dead-letters. Delays remain 2, 4, 8, 16, 32, 60, 60, … seconds, calculated from
PostgreSQL wall time without sleeping in a transaction. `PermanentDeliveryError`
dead-letters immediately, including on attempt 1. Unknown receiver exceptions and
database errors propagate from the library without acknowledgement or invented
classifications; the abandoned lease is resolved by recovery. Both CLI boundaries
exit nonzero with fixed diagnostics, without arbitrary exception text or tracebacks.
Process termination is not converted into a delivery result. Last error code
survives successful delivery and replay; active dead-letter metadata is cleared
by replay. There is no append-only failure history in this phase.

## Ambiguous outcomes and stable IDs

**AMBIGUOUS delivery outcome:** a crash after receiver commit but before
acknowledgement on the final attempt can leave a receiver effect while exhausted
lease recovery records `dead_letter`. This state is not proof that the receiver
did not process the event. The same uncertainty exists for lost COMMIT responses.
A receiver database error never authorizes acknowledgement, even if its commit
might have succeeded. A lost acknowledgement response may mean already delivered.
Inspect durable state after reconnecting; no uncertain outcome is reported as a
confirmed success.

Every claim and replay preserves `id`, `job_id`, `event_type`, payload and receiver
deduplication rows. **Manual replay must not reset event IDs:** a recipient sees a
new UUID as a new logical event and could repeat an effect already committed
before a crash. Keeping the ID lets a conforming receiver durably accept the
duplicate without another effect. Deleting recipient deduplication records loses
that protection. Bounded retries do not provide exactly-once delivery, eventual
success, or even a receiver invocation for every committed claim.

The synthetic receiver's entire effect is its unique event-ID row. It runs in a
separate transaction on **the same PostgreSQL instance** and has no sender foreign
keys. This tests deduplication and commit boundaries, not HTTP/network transport,
independent service availability, physical connection loss, or a distributed
transaction. Real recipients must commit ID deduplication atomically with their
business effect and define retention relative to operator replay horizons.

## Local operator replay

```sh
uv run python -m task_engine.operator replay-event EVENT_UUID --extra-attempts 3
```

Use the configured `DATABASE_URL`; this is a local CLI, with no HTTP management
endpoint or automatic dead-letter reactivation. Access to its database credentials
is the existing operational trust boundary; there is no added user authentication.

One transaction locks the requested row, requires an existing dead letter and adds
the requested **1–1000** attempts to the *stored maximum*, not to attempts. Thus an
early permanent failure at attempts 1 / maximum 5 replayed with +3 becomes attempts
1 / maximum 8. Existing unused budget is retained. INTEGER overflow of either the
maximum or replay counter is rejected before updating anything. At the absolute
integer maximum, further replay requires a future schema/policy change.

The same atomic UPDATE increments replay count, records DB-wall-time replay and
immediately due eligibility, sets pending and clears active dead-letter and lease
metadata. Historical attempts and all receiver records are untouched. Concurrent
replays serialize: once the winner changes the state to pending, a contender
rejects that state without granting another budget. A later dead-letter episode
may be replayed through a new explicit decision. A lost replay COMMIT response
returns a database error; inspect the row before repeating the command, since it
may already be pending with the granted budget.

Exit codes: 0 confirmed replay scheduled; 1 database/unexpected CLI error; 2 invalid
arguments/budget; 3 missing event; 4 event is not dead-letter; 5 budget/counter
would overflow. Output contains only fixed status codes and never payloads,
credentials, arbitrary input or exception text. Invalid argparse diagnostics are
sanitized too.

## Migration, preservation and downgrade

Stop API, workers and dispatchers before upgrading; only resume upgraded binaries.
Old dispatchers cannot honor limits and do not recognize dead letters. Plan for
ALTER/check validation locks, the full-table backfill and capacity on large tables.

Backfill sets `max_delivery_attempts = GREATEST(5, delivery_attempts)` without
resetting counters. IDs, payloads, timestamps, jobs, receiver rows and existing
indexes are preserved. Only pending rows already at the backfilled limit become
dead-letter (`attempts_exhausted`, new DB-time dead-letter timestamp). No historical
error is invented. Delivered rows remain delivered. Delivering tokens/deadlines
remain intact, including final-budget claims: a valid owner can acknowledge;
expired exhausted claims become dead-letter through subsequent bounded recovery.

Downgrade to `0006` takes a table lock and **refuses while any dead letters exist**;
it never silently reactivates them. Resolve them explicitly before downgrade.
When permitted, it preserves all `0006` columns and receiver data, but discards
limits, error classifications, replay audit fields and monotonic-attempt protection.
Old binaries then retry without a policy limit. Re-upgrade reconstructs limits from
attempts, losing any unused operator-granted budget and prior replay history.
Downgrading further to `0005` also destroys receiver deduplication as documented in
ADR 0005. Never use downgrade as an operational retry/reset mechanism.

## Limits and follow-up

No heartbeat, jitter, ordering/fairness or latency guarantee, retention policy,
append-only audit, metrics, real transport or production hardening is added. Clock
jumps affect eligibility and leases; paused senders can overlap newer senders.
Tokens fence sender PostgreSQL writes only. Production-scale lock/query performance
and failures across independent databases/networks remain unverified.

Next milestone: specify and test a real transport/recipient idempotency contract,
including timeouts, ambiguous responses, deduplication retention and operator
visibility of ambiguous dead letters before introducing real effects.
