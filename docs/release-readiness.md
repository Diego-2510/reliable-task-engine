# First release readiness: proposed v0.1.0

**NO-GO for public release today.** The defined local synthetic MVP passes its
technical acceptance checks. Owner licensing review, review/commit of the local
candidate, and CI on that eventual release commit remain outstanding. No P0/P1
runtime release blocker was confirmed. Nothing was committed, tagged or published.

The audited baseline is clean `main` at
`7a590c8b23ac064cedba95faa8104e9894bae413`, with the user-supplied GitHub Actions
SUCCESS. Local Phase 4A changes have no CI result. `pyproject.toml` remains
`0.1.0`; recommend **v0.1.0** for the first public tag, consistent with the package.
If a separate prerelease is desired later, align its package/tag versions through
an explicit owner decision. No version bump or tag is justified by this audit.

[The reliability audit](reliability-audit.md) contains the component inventory,
A–J guarantee matrix, findings, exact benchmark provenance and test evidence.
The component boundary is API → PostgreSQL jobs → worker → atomic success/outbox
commit → dispatcher → independently committed synthetic receiver row → ack.
Operator replay and read-only diagnostics are local credentialed commands.

## Security and intended scope

This release candidate is a trusted-local portfolio demonstration, with synthetic
handlers/data only. It has **no authentication or tenant isolation**, no payload
byte/depth policy or rate limiting, no production-facing network deployment,
no real HTTP downstream receiver, no automatic retention or deduplication expiry,
no heartbeat/cancellation and no exactly-once processing or delivery. Tenant-less
submission keys are global. The API intentionally returns payloads to callers.
Retain keys and receiver IDs for as long as retries/replay require their protection.
Deleting deduplication state can repeat effects. No retention procedure is supplied.

Main and benchmark Compose publish PostgreSQL/API only on loopback; internal
containers still share their respective network. Do not expose these ports or
proxy the API publicly. Public example passwords (`task`/`benchmark`) are development
credentials, not secrets. The image runs as root and has no explicit sandbox,
resource limits or automated supervision; these are accepted local-MVP limits.
Base image tags can move; uv is version-pinned, Python/PG are not digest-pinned.

The lockfile pins dependencies and archive hashes from PyPI, with the project as
the only editable local package. No unexpected VCS/private dependency or new
dependency was found. Fresh-cache installation passed; this is not a vulnerability
advisory scan or a software-supply-chain attestation. Docker provenance in the
benchmark manifests applies to the recorded measurements, not all future builds.

Fixed application errors/classifications and allowlisted telemetry avoid arbitrary
payloads, keys, driver diagnostics and exceptions. JSON logging also removes raw
framework messages. Human Uvicorn access logs contain request URIs and job UUIDs;
keep secrets out of URLs and do not enable SQL/driver debug logs. PostgreSQL and
external infrastructure logging require their own privacy configuration. Local
operator credentials can also execute arbitrary SQL: supported CLI safeguards are
not a privilege boundary against an administrator. See AUD-04/AUD-05 in the audit.

## Install and startup

For a new installation on a machine with free ports 5432 and 8000:

```sh
uv sync --locked
docker compose config -q
docker compose -f benchmarks/compose.yaml config -q
docker compose up --build --scale worker=2
```

Compose waits for DB health and completed migrations before starting the API,
workers and one dispatcher. No `.env`, ignored AGENTS.md or telemetry backend is
needed. Stop if ports are already in use; do not stop an unknown service to free
them. This command may attach an existing Compose volume and is a fresh-install
recipe only when that volume is known to belong to the intended installation.

Use the README's keyed, opt-in POST example. Expect 201 on creation, 200 with the
same ID on matching replay, 409 for changed parameters, 422 for invalid input.
GET `/v1/jobs/UUID` should reach `succeeded` while a worker runs. A requested
`job.succeeded.v1` event should become `delivered` while a dispatcher runs. These
are eventual observations under availability/budget assumptions, not deadlines.
Read the aggregate snapshot:

```sh
docker compose exec api python -m task_engine.diagnostics
```

The snapshot does not return payloads or mutate state. To inspect a specific
trusted local event, use SQL projecting only operational metadata:

```sql
SELECT id, job_id, delivery_state, delivery_attempts, max_delivery_attempts,
       delivery_last_error_code, dead_letter_reason, dead_lettered_at,
       replay_count, last_replayed_at
FROM outbox_events WHERE id = 'EVENT_UUID';
SELECT event_id, job_id, event_type, processed_at
FROM synthetic_receiver_events WHERE event_id = 'EVENT_UUID';
```

In a local uv process, explicitly configure DATABASE_URL before Alembic, API,
worker, dispatcher, diagnostics or operator commands. Without it the runtime
uses the documented development default. Do not rely on defaults for audit/test
isolation. Positive integer `LEASE_SECONDS` and `DELIVERY_LEASE_SECONDS` default
to 60. Telemetry defaults to disabled; `console` is a local opt-in.

## Schema upgrade

1. Identify the intended database and installed schema using read-only inspection.
   Arrange a backup/restore procedure before a non-disposable upgrade.
2. Stop **all** APIs, workers and dispatchers, including separately started
   processes and older binaries. No mixed-version rolling upgrade is supported.
3. For the intended known Compose installation:

   ```sh
   docker compose stop api worker dispatcher
   docker compose build
   docker compose run --rm migrate
   docker compose up -d --scale worker=2
   ```

4. Verify schema revision 0007, process state, diagnostics and a new keyed synthetic
   job. Older migrations require locks, backfills and index work; allow downtime.

Never use downgrade as retry/reset. Downgrade 0007→0006 refuses with dead letters
and otherwise discards budget/replay metadata. Downgrade to 0005 destroys receiver
deduplication; further downgrade destroys intents. Those are destructive operations
requiring their own decision, outside this release procedure.

## Shutdown, interruption and recovery

Stop application processes before stopping PostgreSQL. The API supports orderly
Uvicorn shutdown. **Workers and dispatchers do not implement graceful drain or a
SIGTERM handler.** In the audited Docker PID-1 setup, `docker stop --timeout 3`
on idle services produced API exit **0**, worker/dispatcher exit **137** after the
forced-kill deadline. After restarting, the existing two succeeded jobs/delivered
events and two receiver effects remained unchanged. Do not call this a successful
graceful worker drain. Abrupt termination can also lose final telemetry exports.

If stopping with work in flight, expect ownership to remain until expiry and a
committed claim to consume budget. Recovery uses DB wall time, capped backoff and
continued polling. There is no heartbeat or guaranteed recovery deadline. Clock
jumps, locks and stalled I/O can delay progress. PostgreSQL connection/read/lock
operations have no application-configured bounded timeout; pool_pre_ping detects
some broken connections but does not establish a failure-detection deadline.
Compose has no automatic restart policy.

After an interruption:

1. Restore database availability and wait for `pg_isready`/WAL recovery to complete.
2. Inspect process state and diagnostics. Database failures can make worker or
   dispatcher exit 1 with a fixed diagnostic; stalled I/O may require manually
   stopping the affected process. The outage smoke observed both behaviors.
3. Restart upgraded worker/dispatcher processes explicitly, e.g.
   `docker compose up -d worker dispatcher`. Ordinary polls recover expired leases;
   no manual token/counter reset is needed. Exhausted jobs fail, exhausted deliveries
   become visible dead letters. Locked rows may wait for later polls.
4. Inspect durable state after an uncertain COMMIT. An error does not prove rollback.
   For submission, retry the same key/body; never invent another key just because
   the first response was lost. Do not assert success from process telemetry alone.

The executed clean-image outage killed the dedicated PostgreSQL container with
SIGKILL, preserved its new dedicated volume and restarted PostgreSQL. WAL recovery
completed; original state/effect timestamps persisted. Manually restarted processes
completed a second new job/event; final counts were **2 jobs / 2 outbox / 2 effects**.
This establishes the local tested boundary, not host power-loss survival, HA or
cross-database resilience.

## Dead-letter operator procedure

A dead letter can already have a receiver effect, especially after the final
receiver commit and before acknowledgement. Inspect the event and receiver row
before deciding to replay. Use its **original event UUID** and a deliberate finite
additional budget:

```sh
# For the intended known Compose installation:
docker compose exec api python -m task_engine.operator \
  replay-event EVENT_UUID --extra-attempts 3
# Or after explicitly configuring DATABASE_URL:
uv run python -m task_engine.operator replay-event EVENT_UUID --extra-attempts 3
```

Only dead letters can reopen. Allowed extra budget is 1–1000; addition is to the
stored maximum, not a reset of historical attempts. ID/type/payload/job association,
prior attempt count and receiver history remain; replay_count and last_replayed_at
record the new decision. The dispatcher later performs delivery. Exit codes:
0 confirmed scheduled, 1 DB/unexpected error, 2 invalid input/budget, 3 missing,
4 wrong state, 5 overflow. A code-1 uncertain commit may already have reopened the
event: inspect before repeating. Never delete the receiver ID, replace the event
ID, decrease counters or downgrade to make an event eligible. There is no supported
job replay command or append-only operator/failure history.

## Clean-checkout reproduction and validation

The executed smoke used `git archive HEAD`, an empty uv cache, a clean-source
Docker image, an isolated internal network and separate fresh PG16 storage.
To repeat installation/build without overwriting a working directory:

```sh
export_dir=$(mktemp -d /tmp/rte-clean-XXXXXX)
git archive HEAD | tar -x -C "$export_dir"
uv sync --locked --project "$export_dir" --cache-dir "$export_dir/.uv-cache"
docker build -t rte-local-review "$export_dir"
```

For isolated local process checks, allocate a new uniquely named database container
and volume; a Docker-assigned loopback port avoids development port conflicts:

```sh
smoke_dir=$(mktemp -d /tmp/rte-smoke-XXXXXX)
smoke_name=$(basename "$smoke_dir")
smoke_volume=$(docker volume create --label rte.release-smoke=1 "$smoke_name-data")
docker run --detach --name "$smoke_name" \
  --env POSTGRES_USER=task --env POSTGRES_PASSWORD=task \
  --env POSTGRES_DB=rte_release_test --publish 127.0.0.1::5432 \
  --mount "type=volume,source=$smoke_volume,target=/var/lib/postgresql/data" postgres:16
# Repeat readiness check until ready before migration:
docker exec "$smoke_name" pg_isready -U task -d rte_release_test
smoke_mapping=$(docker port "$smoke_name" 5432)
export DATABASE_URL="postgresql+psycopg://task:task@127.0.0.1:${smoke_mapping##*:}/rte_release_test"
cd "$export_dir"
uv run alembic upgrade head
uv run uvicorn task_engine.api:app --host 127.0.0.1 --port 0
# Read its assigned port from startup output; use it for POST/GET, not port 8000.
# In separate shells with the SAME explicit DATABASE_URL and export directory:
# uv run python -m task_engine.worker
# uv run python -m task_engine.dispatcher
# uv run python -m task_engine.diagnostics
```

Verify the keyed POST/replay/conflict/invalid-input statuses, GET success and SQL
state/effect counts as described above. Stop/restart only these processes and this
known database container to test persistence. A tmpfs container loses its data
when stopped; use the newly created dedicated volume for this restart check.
Before cleanup verify the container/volume name and label are those created by
this invocation. Stop the local processes, remove only `"$smoke_name"`, then remove
only `"$smoke_volume"` if no longer needed. Never prune or delete unknown volumes.

For full pytest, use a **different fresh dedicated** `_test` DB, migrate it, set
`TEST_DATABASE=1` and explicit `TEST_DATABASE_URL`. Set `BENCHMARK_DATABASE=1` and
`BENCHMARK_DATABASE_URL` to another empty disposable `_test` DB to include the
benchmark smoke. The smoke test invokes benchmark preparation itself. These
settings explicitly authorize row deletion/schema changes in their targets; the
name suffix alone is insufficient. Keep runtime processes off the test databases.

For example, while the uniquely named disposable container above still exists,
stop the smoke processes and create two additional fresh databases there:

```sh
docker exec "$smoke_name" createdb -U task rte_regression_test
docker exec "$smoke_name" createdb -U task rte_benchmark_test
export TEST_DATABASE_URL="postgresql+psycopg://task:task@127.0.0.1:${smoke_mapping##*:}/rte_regression_test"
export BENCHMARK_DATABASE_URL="postgresql+psycopg://task:task@127.0.0.1:${smoke_mapping##*:}/rte_benchmark_test"
DATABASE_URL="$TEST_DATABASE_URL" uv run alembic upgrade head
TEST_DATABASE=1 BENCHMARK_DATABASE=1 uv run pytest -ra
```

Abort on any database-creation failure; do not adopt an already existing database
as disposable. Run these commands before removing that container/volume. The
clean export tests the selected HEAD; the added Phase 4A test joins it only after
an authorized future commit, or runs now in the local candidate working tree.

Required check commands and actual audit results:

| Check | Actual result |
| --- | --- |
| `uv sync --locked` | Passed; fresh-cache clean export also passed |
| `uv run ruff check .` | Passed |
| `uv run mypy src` | Passed, nine files |
| `docker compose config -q` | Passed |
| `docker compose -f benchmarks/compose.yaml config -q` | Passed |
| Full `uv run pytest -ra` with both authorized DBs | **350 passed, 26 warnings, 78.04 s**, zero skipped |
| Worker SIGKILL focused test | 1 passed; then included in full run |
| Clean HEAD install/migration/image/HTTP/outage/stop/restart | Verified, with the shutdown and timeout limits above |
| `python -m benchmarks summarize COPIED_RESULT_DIR` | All five sets regenerated identically; originals untouched |
| `git diff --check` | Passed |
| Full tracked/untracked diff and text secret review | Reviewed all five candidate files and tracked repository; only public synthetic credential examples found |

Warnings remain: one Starlette/httpx TestClient and 25 Alembic path_separator
deprecations; the host Docker legacy builder deprecation; an initial readiness
probe returned no response during crash recovery. These did not invalidate the
subsequent successful results. No new remote CI run was performed.

## Benchmark provenance and release claims

[Phase 3B](phase-3b-validation.md) and [benchmark guide](benchmarks.md) remain the
performance source. All 24 baseline cases, eight warm-ups and 36,000 samples are
present, with saved gates internally consistent and independent statistics correct.
One dispatcher stayed constant; warm-ups are excluded from measured statistics.
The baseline's recorded aggregate source/config SHA-256 exactly matches the 30
covered files in committed `7a590c8`, despite being recorded dirty at `33e83cf`.
This is **source equivalence, not a clean-run claim**. The early functional smoke
fingerprint differs; exact original-smoke equivalence cannot be established.
Current functional benchmark smoke passed independently in the full suite.

Use the actual 46.90→316.81 jobs/s / 6.76× / 84.4% worker scaling and
39.94–42.20 ack/s single-dispatcher results with their synthetic/shared-host caveats.
No full matrix rerun was needed. No HTTP E2E performance, production equivalence,
independent network receiver, exactly-once or population p99 guarantee is supported.
Historical raw database rows were not retained; recorded row-level correctness
gates cannot be reconstructed from latency CSV alone.

## Acceptance checklist and remaining owner work

- [x] Baseline/branch/status captured; initial local modifications absent.
- [x] Inventory and A–J matrix distinguish enforced/tested/assumed/non-guarantees.
- [x] No confirmed P0/P1 defect in the defined MVP; P2 issues recorded.
- [x] Required failure boundaries tested on disposable PostgreSQL 16 only.
- [x] Clean committed-HEAD installation/build and runtime smoke verified.
- [x] Static/config/full-suite checks and saved-report regeneration passed.
- [x] Benchmark provenance verified without rewriting measurements.
- [x] README overview, operations/recovery and limitations prepared.
- [x] Candidate files, secret patterns and whitespace reviewed locally.
- [ ] Owner chooses a license. Review `LICENSE.candidate` (MIT, copyright 2026
  Diego Ringleb); if approved, adopt it as `LICENSE` through an authorized change.
  The candidate is a proposal and is not an adopted license.
- [ ] Owner reviews/accepts local-MVP limitations, especially AUD-04 shutdown/DB
  timeout/manual restart behavior and AUD-05 trusted-network scope.
- [ ] Owner authorizes commit/publishing work separately. Before any future commit,
  verify effective author **and** committer are Diego Ringleb with the configured,
  verified GitHub email; do not change identity settings or add AI coauthor trailers.
- [ ] Run required CI against the eventual exact release commit, including any
  owner-requested edits. The current clean-HEAD smoke validates baseline runtime
  inputs; local changes are README, audit/runbook, license candidate and one test.
- [ ] Before an authorized push, verify authenticated GitHub account **Diego-2510**.
- [ ] With explicit owner authorization, tag the reviewed passing commit `v0.1.0`
  and publish release notes linked to its evidence. Do not imply that this audit
  authorized a commit, push, tag, PR, license adoption or release.

Changed files: README.md, docs/reliability-audit.md, docs/release-readiness.md,
LICENSE.candidate and tests/test_process_recovery.py. Runtime, migrations, CI,
Docker/Compose, pyproject, uv.lock and all measured artifacts are unchanged.
No scope-expanding authentication, broker, adapter or architecture was added.
