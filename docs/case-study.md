# Reliable Task Engine: making failure boundaries explicit

Reliable Task Engine is a bounded engineering MVP for durable background work.
Python 3.12, FastAPI, SQLAlchemy and PostgreSQL 16 implement submission, execution,
recovery and synthetic delivery. The interesting result is a protocol whose
transaction boundaries can be inspected and tested. This case study describes
those boundaries, their evidence and their costs.

## The engineering problem

Background processing becomes difficult when a process disappears between two
observable actions. A client can lose a submission response after the database
commits. A worker can execute a handler and die before recording success. A
recipient can persist an effect while the sender never learns that it succeeded.
Treating every error as rollback duplicates work; treating every timeout as
success loses work. Concurrent recovery adds another problem: an old process can
resume after another process has legitimately taken ownership.

This project makes these situations explicit without promising exactly-once
execution or delivery. Attempts have finite budgets, failures become visible,
and correctness depends on durable identities and conditional state transitions.

## Why PostgreSQL is the authority

For this bounded MVP, PostgreSQL holds jobs, submission keys, leases, retry
budgets and outbox intent. A separate broker would introduce another authority
and a database-to-broker publication boundary without removing the need for
idempotency or recipient-side deduplication. Keeping the queue beside application
state allows atomic job success and event creation using an ordinary transaction.

The trade-off is deliberate: polling, row locks, indexes and WAL share database
capacity with state storage. This is not evidence that PostgreSQL queues are
universally preferable to brokers. It is a smaller system in which the required
invariants are directly enforceable. [ADR 0001](adr/0001-leases-and-fencing.md)
and [ADR 0004](adr/0004-transactional-outbox.md) explain the ownership and outbox
choices; [ADR 0002](adr/0002-bounded-retries.md) supersedes the original unbounded
recovery policy.

## The transactional path

The path is **API → jobs → worker → atomic success/outbox → dispatcher → synthetic
receiver → acknowledgement**. Each arrow does not imply a shared transaction.

The API validates input and commits a job before returning success. A worker
claims eligible work in a short transaction, assigning a fresh ownership token
and database-time lease and incrementing attempts. The synthetic handler runs
outside that transaction. Completion locks the job, verifies ownership and
expiry, and commits success together with an optional `job.succeeded.v1` outbox
row. An outbox insertion failure rolls back success as well.

Delivery then crosses three independent commits: claim, receiver effect and
acknowledgement. The receiver effect is a row in the same PostgreSQL instance,
written in a separate transaction. This models the acknowledgement gap without
claiming to implement real HTTP delivery or an independently available recipient.

## Correctness mechanisms

`FOR UPDATE SKIP LOCKED` lets competing workers skip rows already locked by other
claimers. It supports concurrent progress without a process-local lock, but does
not guarantee fairness or global ordering. Claims commit before execution, so a
crash cannot hide that an attempt was consumed.

Leases establish temporary ownership. Completion first takes the row lock, then
checks status, token and strict expiry against PostgreSQL `clock_timestamp()`.
Checking after lock acquisition matters: a deadline evaluated before a lock wait
could be stale when writing becomes possible. An expired or replaced token cannot
finish, retry or create an outbox intent. Validity is checked at the locked update,
not at COMMIT. Tokens fence these database writes; external effects are not fenced.

Retry budgets count committed claims, including claims whose processes die before
invocation. Classified failures use capped exponential backoff. Ordinary polling
recovers expired jobs in bounded batches, rescheduling remaining budgets or
terminalizing exhaustion. Recovery does not grant extra attempts.

Submission idempotency uses an optional global key and a unique partial index.
At READ COMMITTED, a competing insertion waits for the winner; a separate SELECT
then sees its committed job and compares JSONB payload and attempt limit. Matching
replays return the same job; conflicting parameters return 409. Keys survive
process restarts but have no automatic expiry. [ADR 0003](adr/0003-submission-idempotency.md)
details the snapshot choice.

The transactional outbox removes success-without-intent within PostgreSQL. Its
persisted event UUID survives delivery retries. The receiver's primary key on
that UUID makes repeated acceptance leave one persisted effect and its original
timestamp. Delivery failure can become a dead letter; explicit operator replay
adds a finite budget while preserving attempts, event identity and receiver
history. [ADR 0005](adr/0005-outbox-dispatcher.md) and
[ADR 0006](adr/0006-delivery-dead-letter.md) specify those transitions.

## Failure boundaries that shape the protocol

A worker SIGKILL leaves its committed claim intact. After expiry, another process
uses normal recovery and backoff. An old worker that resumes becomes a stale
writer and cannot overwrite the new owner. Expiry can nevertheless permit
overlapping handler executions; there is no heartbeat.

A dispatcher crash after receiver COMMIT but before ack leaves a real synthetic
effect and an unacknowledged event. With budget remaining, redelivery uses the
same ID and deduplicates. On the final attempt, expiry becomes a dead letter even
if the effect exists. That is an **ambiguous outcome**, not proof of non-delivery.
Replay must retain the original event ID.

An unknown COMMIT result is similarly distinct from an aborted transaction.
Connection loss can occur after PostgreSQL committed but before confirmation.
The application reports an error without claiming confirmed success. Operators
must inspect durable state; submission clients can retry the same key and body.
Telemetry can undercount confirmed database facts and cannot resolve uncertainty.

## Verification and evidence

The [Phase 4A reliability audit](reliability-audit.md) records **350 passing tests**
against disposable PostgreSQL, with zero skipped. Its coverage includes eight-process
claim and submission contention, lock-wait expiry, stale-owner rejection, retry
exhaustion, migration preservation, real SQL aborts around outbox insertion and
lost responses after actual DBAPI commits at nine boundaries.

[Worker recovery tests](../tests/test_process_recovery.py) send actual SIGKILL
and recover in a fresh process. Dispatcher tests use abrupt `os._exit(23)` after
receiver commit; final-attempt and replay tests preserve the receiver timestamp.
These are different injection mechanisms, represented as such. The audit also
records a clean-image workflow and PostgreSQL kill/restart, including manual
restart and shutdown limitations. [Phase 4B validation](phase-4b-validation.md)
records the current candidate's independent checks without rewriting that history.

## What the measurements establish

The [saved baseline report](../benchmarks/results/phase3b-baseline/report.md),
[manifest](../benchmarks/results/phase3b-baseline/manifest.json),
[runs](../benchmarks/results/phase3b-baseline/runs.csv) and
[samples](../benchmarks/results/phase3b-baseline/latencies.csv) cover two profiles,
1/2/4/8 workers, three measured repetitions of 1,000 jobs per configuration and
separate warm-ups. Job-only median throughput rises from **46.90 jobs/s** with
one worker to **316.81 jobs/s** with eight: **6.76× speedup** and **84.4% scaling
efficiency**. With one dispatcher fixed, acknowledgements remain **39.94–42.20/s**.
Adding workers alone therefore did not materially improve delivery throughput.

Handlers are essentially no-op, downstream effects are DB rows, and bulk SQL
loading bypasses HTTP. These are synthetic backlog-drain measurements, not measured
HTTP end-to-end latency, real network delivery or production workload equivalence.
Rates suggest a delivery-side constraint but cannot identify WAL, storage, CPU
or round trips as its cause. Three repetitions on a shared host do not establish
confidence intervals. The recorded run was dirty; the audit establishes source
fingerprint equivalence for the baseline, not a clean-run history. Raw historical
DB rows were not retained. [Methodology](adr/0008-benchmark-methodology.md) and
[reproduction instructions](benchmarks.md) define the evidence contract.

## Boundaries and lessons

There is no authentication, tenancy, real downstream HTTP, automatic retention,
graceful worker drain or production-ready deployment. Database I/O has no
application-configured bounded timeout; supervision and restart remain manual.
Retained submission keys and receiver IDs are correctness inputs, so deletion
would require a designed retention contract.

The main lesson is that atomicity has a precise scope. One database transaction
can couple success and intent, but cannot erase an independently committed
recipient effect. Bounded retries improve operational visibility while making
final-attempt ambiguity unavoidable. Stable identities make replay useful;
resetting them would defeat its safety. Finally, the benchmark separated job
completion from acknowledgement: a single aggregate throughput number would
have concealed the delivery constraint. Explicit boundaries produced a smaller,
more defensible system and clearer questions for any future extension.
