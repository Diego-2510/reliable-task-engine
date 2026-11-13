# Phase 4A reliability audit

Audit date: 2026-11-14 (Europe/Berlin). Decision: **technical MVP checks passed;
public release remains NO-GO pending owner licensing review and publication checks**.
No confirmed P0 or P1 runtime defect was found. No production code, schema,
dependency, version or measured artifact was changed.

## Baseline and evidence boundaries

Started with a clean working tree on `main`, HEAD
`7a590c8b23ac064cedba95faa8104e9894bae413`. This matches the supplied remote
baseline; its GitHub Actions SUCCESS is user-supplied evidence, not a newly
queried or executed CI result. All Phase 4A changes remain local and uncommitted.
No commits, tags, releases, pushes, remote writes or Git identity changes occurred.

Read the local AGENTS.md, required efficiency/validation skills, runtime source,
migrations 0001–0007, ADRs 0001–0008, existing validation records, tests, benchmark
implementation/artifacts, lockfile, Docker/Compose and CI configuration.
`ENGINEERING_MARKTWERT_PLAYBOOK` was not found locally. Serena's symbol tools
reported no active language server; repository reads/searches supplied the audit.
No subagents or private project material were used.

Evidence below distinguishes current executions from historical recorded results.
Local logs and the clean export were retained under `/tmp/rte-phase4a-evidence`
and the export path recorded there; these are temporary local evidence, not
required inputs to the public repository. The tracked tests and saved Phase 3B
CSVs are the durable reproducibility inputs.

## Architecture and audit inventory

FastAPI validates and submits jobs. PostgreSQL is the authoritative queue,
lease, budget, key and outbox store. Workers claim and commit before synthetic
execution, then complete in a separate fenced transaction. The dispatcher uses
separate claim, synthetic receiver and acknowledgement commits. The receiver's
row is its entire effect, in the same PostgreSQL instance. The operator CLI
reopens dead letters; diagnostics performs a read-only aggregate SELECT.
Telemetry observes business outcomes and never decides durable state.

| ID | Area | Code/decision evidence | Verification and result |
| --- | --- | --- | --- |
| INV-01 | Request validation / keys | `api.JobInput`, `create_job`; ADR 0003 | Strict attempt integers 1–10, object payload, forbidden extras; invalid/duplicate headers rejected; eight fresh API contenders create one keyed job. `test_idempotency.py`, `test_retries.py`: pass. |
| INV-02 | Isolation / constraints | Submission explicitly READ COMMITTED; unique partial key index; FK/type/state/lease checks in migrations/models | Concurrent insert then separate fresh-snapshot SELECT; unrelated errors remain 500. All seven migration tests pass. Default PostgreSQL READ COMMITTED is assumed for other runtime sessions. |
| INV-03 | Job claiming | `worker.claim`, SKIP LOCKED, due time and budget predicates | Eight-process initial/recovery contention and independent claims pass. Claim commits before handler. |
| INV-04 | Worker leases / fencing | `worker._finish`; lock first, then token/status/strict DB expiry UPDATE | Wrong, expired, replaced and terminal owners reject; expiry after lock wait tested. No stale outbox insertion. |
| INV-05 | Retry / expiry recovery | `recover_expired`, batch 100, separate committed recovery, capped backoff | Classified failure, final budget, skipped locks, recovery/completion races, actual SIGKILL/fresh-process recovery: pass. |
| INV-06 | Atomic success / intent | `worker._finish`, single transaction and flush; unique `(job_id,event_type)` | Independent reader visibility and real SQL abort before/after INSERT both pass; recovery emits once. |
| INV-07 | Delivery leases / recovery | `dispatcher.claim_delivery`, `_recover_exhausted`, `_finish_delivery` | Eight-process claims, lock-wait expiry, stale ack/retry/dead-letter, exhausted recovery and integer limits pass. |
| INV-08 | Receiver deduplication | `deliver_to_synthetic_receiver`, PK event ID, conflict-do-nothing | Concurrent and repeated calls persist one row; crash after receiver commit preserves its original timestamp. |
| INV-09 | Dead letters / operator | `operator.replay_event`; ADR 0006; monotonic delivery-attempt trigger | Final-attempt ambiguity, stable-ID replay, preserved history, concurrent operators, invalid budget and overflow pass. |
| INV-10 | Commit uncertainty | Business returns/counters follow transaction-context exit | Real transaction abort and response loss after actual DBAPI commits at nine boundaries: no false confirmed success. |
| INV-11 | OpenTelemetry isolation | `telemetry.record`, safe exporters, `traced`; ADR 0007 | Broken exporters/instruments leave business commits intact; interruptions remain observations. Pass. |
| INV-12 | Privacy | Fixed error envelopes/codes, hidden SQL parameters, explicit telemetry vocabulary/JSON formatter | Synthetic secret markers absent from errors/traces/metrics/application failure logs. Full text secret review found only public local example credentials. See scope below. |
| INV-13 | Benchmarks | ADR 0008, `measurement`, `reporting`, `environment` | Independent sample/rate/percentile/scaling checks and exact regeneration pass for all five saved sets; provenance below. |
| INV-14 | Docker / startup | Explicit COPY inputs; migration dependency; loopback host ports | Clean HEAD export, fresh uv cache, empty-DB migrations, image startup and real HTTP smoke pass. |
| INV-15 | CI / reproducibility | Locked install, PG16 service, separate benchmark DB, static checks and full pytest | Local final suite 350 passed; no new remote CI claim. Baseline clean export needs no ignored AGENTS/.env/venv. |
| INV-16 | Shutdown / outage | Worker/dispatcher loops have no shutdown handler, DB timeout or supervisor policy | Database SIGKILL and idle service stop/restart exercised. See AUD-04 and actual outcomes below. |
| INV-17 | Operator / cleanup trust | Local credential access, pytest actual-name checks, benchmark marker/lock/identity checks | Unsafe-target rejection and no-deletion sentinel tests pass. All current destructive operations targeted newly created disposable resources only. |

## Guarantees and boundaries

All guarantees assume the current schema/binaries, no privileged direct SQL
corruption, retained identity/deduplication rows, and PostgreSQL transactional
integrity. Progress needs continued polling and database availability. Tokens
are random ownership capabilities, not externally enforced monotonic fences.

| Invariant | Actually enforced | Test evidence | Assumptions / non-guarantees |
| --- | --- | --- | --- |
| A: one valid owner per generation | Row lock, fresh token, status and expiry; claim increments attempts | Multiprocess claims, same-token concurrent finishes | Multiple handler executions can overlap after expiry; no heartbeat. |
| B: stale workers cannot mutate job/outbox | Locked conditional transition; INSERT only after accepted success UPDATE | Engine fencing, outbox stale owners, lock-wait tests | Validity at the locked UPDATE, not at COMMIT. External effects are not fenced. |
| C: success + intent atomic | Same PostgreSQL transaction; insert failure rolls back success | Visibility and before/after-insert SQL abort tests | Opt-in successful synthetic jobs only; no historical backfill. |
| D: replay cannot create another logical keyed job | Unique non-null key, READ COMMITTED insert/select, JSONB equality | Eight spawned submitters, conflicts, process restart | Global optional keys; unkeyed submissions create new jobs. No concurrent deletion API; removing associations removes protection. |
| E: same event ID, one persisted synthetic effect | Receiver PK insertion is the entire effect; duplicates leave row unchanged | Concurrent/repeated receiver and replay/crash tests | Retain receiver IDs. This says nothing about general external effects or exactly-once delivery. |
| F: dispatcher death cannot silently discard intent | Durable outbox, lease reclaim or visible exhausted dead letter | Receiver-before-ack process crash and final-attempt replay | Dead letter can coexist with an effect. No guaranteed eventual delivery or receiver invocation for each claim. |
| G: budgets cannot accidentally be bypassed through supported operations | Claims require attempts < maximum; retries/recovery retain attempts; replay adds explicit budget | Retry exhaustion, final valid ack, integer boundaries and overflow | Direct DB access is trusted. Delivery attempts additionally have a monotonic trigger; job attempts are protocol-enforced, not protected against arbitrary SQL resets. |
| H: replay preserves IDs/history | Locked dead-letter-only update; attempts unchanged, maximum/replay_count increase | Concurrent replay and original receiver timestamp preserved | Latest replay fields only; no append-only audit. Stored unused budget survives early permanent failure. |
| I: rejected/uncertain commit cannot claim confirmed success | Return/transition signal only after successful context exit; errors propagate/sanitize | Abort and lost-response tests for submit, job claim/finish/recover, delivery claim/finish/recover, receiver, replay | A reported error may already have committed. Inspect durable state or retry submission with the same key. Metrics can undercount. |
| J: application failure/telemetry paths exclude secrets and arbitrary payloads | Generic API errors, fixed classifications, allowlisted signals and event payloads | Real DB errors and privacy markers; exporter failure tests; tracked/untracked secret review | GET/POST intentionally return job payload to callers. Human access logs contain request URIs/UUIDs; never place secrets in URLs. External PG/infra logs and custom library callers/handlers are outside this privacy boundary. |

**No exactly-once processing or delivery claim is made.** This is an at-least-once
attempt design with finite budgets; a crash immediately after claim can consume
an attempt without invoking the handler/receiver. Terminal failures are not
automatically reopened. Fairness, global ordering, latency, eventual success,
independent receiver availability and production resilience are not guaranteed.

## Confirmed findings

P0: none confirmed. P1: none confirmed within the defined local synthetic MVP.
The following P2 findings include release/process blockers and accepted hardening
limits; they are not evidence of broken fencing or lost transactional intent.

| ID / severity | Evidence and reproduction | Impact | Recommended resolution / status | Verification |
| --- | --- | --- | --- | --- |
| AUD-01 / P2 | `git ls-files LICENSE '*LICENSE*'` returns no adopted license | Public source lacks an explicit reuse license; blocks this proposed open-source release | MIT proposed for owner review in `LICENSE.candidate`, copyright 2026 Diego Ringleb. Owner must choose/adopt before publication. **Open blocker.** | Candidate reviewed locally; no `LICENSE` installed and no publication. |
| AUD-02 / P2 | Existing worker tests simulated expiry; dispatcher tests used abrupt `os._exit`, but no actual worker SIGKILL test | Missing direct evidence for one requested crash mechanism | Added `tests/test_process_recovery.py`; no production change. **Resolved coverage gap.** | Real SIGKILL exit -9, committed ownership retained, fresh process recovers/backoff, attempts 1→2, stale completion rejected, one event. Focused and full suite pass. |
| AUD-03 / P2 | Original README first screen buried guarantees/results and said no successful CI run was claimed | Hard to assess project and distinguish historical/current evidence | Concise opening with guarantee, measured result, scope and audit/runbook links; supplied baseline CI separated from local changes. **Resolved.** | README diff and benchmark values reviewed; setup/upgrade/test details retained. |
| AUD-04 / P2 | `worker.main` / `dispatcher.main`: no signal/drain protocol; `db.py`: no bounded DB I/O timeout; Compose has no restart policy. Database kill left worker waiting; idle Docker stop required forced kills | No bound on shutdown/outage detection or autonomous restart; in-flight termination consumes budget and may produce ambiguous delivery | Document manual supervision/restart and lease recovery. **Accepted local-MVP limitation**, not a guarantee of clean worker drain. Future operational hardening requires its own decision. | Real DB SIGKILL, PostgreSQL WAL recovery, manual process restart and later successful job/delivery; idle stop results recorded below. |
| AUD-05 / P2 | No auth/tenancy, body-size/rate policy, retention/expiry or non-root image USER; development credentials and floating base images are explicit in configuration | Unsuitable for untrusted/public production use; no full environment identity across time | Keep trusted local scope and synthetic data, loopback host binding and explicit limits; defer production controls. **Accepted limitation.** | Compose/image review, fresh-cache install, actual image startup, lockfile provenance and secret scan. |
| AUD-06 / P2 | Full suite emits Starlette TestClient deprecation and Alembic path_separator warnings; host Docker reports legacy builder deprecation | Future tooling compatibility work; no current test/build failure | Record warnings; do not add dependencies or change runtime merely to silence them. **Open non-blocker.** | Final 350 passed / 26 warnings; Docker build succeeded. |

## Failure injection and clean HEAD smoke

Regression PostgreSQL: new `rte-phase4a-db`, `postgres:16` (16.15), tmpfs,
random loopback port 32788, dedicated `rte_phase4a_test` and separate
`rte_phase4a_benchmark_test`. The explicit authorization variables and actual DB
identity guards remained enabled. No existing DB, service or volume was reused.

Clean export: `git archive HEAD` into a new `/tmp/rte-phase4a-head-*` directory.
Verified AGENTS.md, .env and .venv absent. `uv sync --locked --project EXPORT
--cache-dir EXPORT/.uv-cache` downloaded into a new empty cache. The clean source
built `rte-phase4a:head` successfully. Docker's host builder warned about legacy
builder deprecation; existing base-image layers could be reused. This verifies
application installation without cached application dependencies, not an offline
build or a fully uncached base-image build.

Image smoke used a separate internal Docker network and a new PG16 database
`rte_phase4a_smoke_test`, empty before Alembic 0001→0007. A newly created labeled
volume preserved this database across the database-container SIGKILL/restart.
The earlier tmpfs startup trial was replaced before the recorded persistence
smoke; tmpfs is unsuitable evidence for container-stop persistence. No host API
port was exposed: real HTTP requests ran inside the API container using urllib.
The internal network did not publish requested host port mappings; checking this
led to using container-local HTTP. Development ports 5432/8000 stayed untouched.

| Injection/check | Actual result |
| --- | --- |
| Worker SIGKILL during execution | New focused test passed, then passed in full suite; fresh spawned process follows normal recovery/backoff and emits one event. Expiry/due times controlled relative to DB clock, without production-state-machine changes. |
| Stale completion after ownership changes | Existing job/outbox transition tests pass for wrong, expired, recovered/new-owner tokens and lock waits. |
| Receiver commit, dispatcher dies before ack | Existing subprocess `os._exit(23)` test passed; receiver row unchanged on attempt 2. This is abrupt death, not a claim that that test sends SIGKILL. |
| Final ambiguous attempt and manual replay | Existing final-attempt crash test passed: effect exists, expired delivery becomes dead letter, replay preserves ID/history and leaves one unchanged effect. |
| Invalid / duplicate submission keys | Boundary/duplicate-header/eight-process integration tests passed; clean HTTP returned 201/200/409/422 for create/replay/conflict/invalid key. |
| Transactional INSERT failure | Existing before- and after-INSERT real PostgreSQL division-by-zero tests passed; job success and event both roll back; recovery later inserts once. |
| Clean image initial workflow | GET succeeded; event delivered; exactly jobs=1, outbox=1, receiver=1, both attempts=1. |
| Database-container SIGKILL | Dedicated DB exit 137. Dispatcher exit 1 with fixed `database_error`; worker remained running/waiting when inspected and was manually stopped. No bounded outage-detection claim. |
| PostgreSQL restart | WAL crash recovery completed. Original job/event/effect persisted; original receiver timestamp unchanged. Manual worker/dispatcher restart processed a second job: succeeded/delivered, counts=2/2/2. |
| Diagnostics | Aggregate JSON shows succeeded/delivered counts, zero due work/expired leases/dead letters. Existing tests prove read-only operation and no row mutations. |
| Idle stop/restart | API orderly shutdown; worker/dispatcher limits and exit codes recorded in the release runbook. Restart preserves terminal state and receiver rows. |

## Benchmark evidence and provenance

No matrix rerun and no measured artifact mutation. All five saved directories
were copied to temporary directories before `python -m benchmarks summarize`;
summary JSON and Markdown regenerated byte-for-byte. Independent Python stdlib
arithmetic also verified every measured configuration/repetition and sample count,
per-run count/elapsed rates, median rates, nearest-rank p50/p95/p99, min/max/mean,
speedup and scaling efficiency. Warm-up IDs have no measured latency samples;
`job_outbox` manifests and every run hold exactly one dispatcher constant.

| Saved set | Warm-ups / measured runs / samples | Source/config equivalence with committed HEAD |
| --- | --- | --- |
| phase3b-baseline | 8 / 24 / 36,000 | Exact aggregate SHA-256 match |
| phase3b-telemetry-disabled | 1 / 3 / 6,000 | Exact match |
| phase3b-telemetry-console | 1 / 3 / 6,000 | Exact match |
| phase3b-permanent-failure-smoke | 4 / 4 / 80 | Exact match |
| phase3b-smoke (initial) | 8 / 8 / 240 | Does not match; original smoke implementation is not proven equivalent |

Baseline and both telemetry/permanent-failure sets were recorded with
`git_sha=33e83cf2e01fd22ce86128bfc142491e93e5e33e`, `git_dirty=true`.
The baseline's 30 sorted `source_fingerprint_paths` cover benchmark modules/config,
runtime Python, migrations, pyproject and uv.lock. Recomputed the original
algorithm over **committed HEAD bytes**, via `git show HEAD:path`, feeding each
`path + NUL + file bytes + NUL` into SHA-256. Result:

```text
192ab442c553227bc34ccba923720ee7568fbc107cce4acd93f94b91e859b81d
```

This equals the recorded baseline fingerprint exactly. Tests, README, Dockerfile,
main Compose and CI are outside that original fingerprint, so it does not attest
to their historical identity. Initial smoke recorded `6c46353a…`; recomputation
over its recorded path list yields `9c4bfb69…`. No per-file historical blobs are
saved, so exact initial-smoke equivalence cannot be established. The new full
suite independently passes the current eight-configuration benchmark smoke.
**None of this changes dirty-run provenance into a clean-run claim.**

Saved baseline gates report 24,000 succeeded jobs, zero failed, 12,000 distinct
events and matching receiver effects, no dead letters/active leases/duplicates;
CSV counts are internally consistent. Historical raw SQL rows are not saved:
this audit can verify saved gates and inspect their implementation, but cannot
independently reconstruct historical UUID/payload equality checks from latency
CSVs alone. It is not a fresh performance measurement.

Job-only medians reproduce 46.90/93.47/166.62/316.81 jobs/s; eight-worker speedup
6.76× and efficiency 84.4%. Outbox job terminalization reaches 284.42 jobs/s,
acknowledgements 39.94–42.20/s with one dispatcher. Controlled console comparison
reproduces the reported 10.66% job-rate and 13.07% ack-rate reductions, subject
to the original ordered/shared-host confounders. No HTTP E2E, production equivalence,
absolute performance threshold or population-tail/SLA claim is supported.

## Validation and release conclusion

| Check | Result |
| --- | --- |
| `uv sync --locked` | Passed in working tree and fresh-cache clean HEAD export |
| Empty dedicated DB `alembic upgrade head` | Passed for regression and clean image databases |
| `uv run ruff check .` / `uv run mypy src` | Passed; mypy checks nine runtime files |
| Both Compose `config -q` checks | Passed |
| Baseline full pytest | 349 passed, 26 warnings, 79.02 s |
| Added SIGKILL focused test | 1 passed, 1.77 s |
| Initial expanded full pytest including benchmark PG smoke | 350 passed, 26 warnings, 80.91 s |
| Final full pytest after test timing review | **350 passed, 26 warnings, 78.04 s**, no skips; includes benchmark PG smoke |
| Clean image / functional / crash-restart checks | Passed within the explicitly documented shutdown/outage limitations |
| Saved report regeneration / independent statistics | All five sets pass; four exact source matches, initial-smoke limitation retained |
| Whitespace / tracked + untracked review / secrets | Final local checks recorded in release-readiness.md |

Warnings: one Starlette/httpx TestClient deprecation; 25 Alembic path_separator
deprecations. No dependency was added to suppress them. The Docker restart
readiness probe initially returned “no response” during WAL recovery; subsequent
ready state and functional checks passed. These observations are not hidden as
successful first probes. No failed regression or static check was observed.

The final test review replaced a comparison against the parent's current clock
with the stored recovery schedule/transition timestamps, avoiding dependence on
how quickly the parent process resumes. No runtime behavior changed. Smoke
containers, regression container and the newly created smoke volume/network were
removed only after verifying their audit labels. The existing development
containers and volumes were left untouched. The clean export, logs and local
image remain available for review; they are not publication artifacts.

See [release readiness](release-readiness.md) for exact acceptance gates, operational
procedures, license review and remaining publication work. No CI success is claimed
for this uncommitted audit candidate.
