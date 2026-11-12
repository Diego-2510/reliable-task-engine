# ADR 0007: Explicit process-local observability

Status: Accepted for phase 3A

## Decision and transaction boundaries

Use the official OpenTelemetry Python API/SDK for manual traces, counters and
histograms, and the standard library for JSON logging. No automatic HTTP, SQL,
driver or exception instrumentation, schema migration, public diagnostics
endpoint, backend service or transport adapter is introduced.

`task_engine.telemetry.initialize(component)` runs explicitly at process startup:
FastAPI lifespan, worker main, dispatcher main and operator main. It is protected
by a process-local lock; the first call wins, including disabled mode. Imports and
individual jobs never initialize providers. Library callers can explicitly
initialize their own process before using the operations. Private providers avoid
changing global OpenTelemetry providers. Service names are `task-engine-api`,
`task-engine-worker`, `task-engine-dispatcher` and `task-engine-operator`.

The default `TASK_ENGINE_TELEMETRY=disabled` creates no SDK provider, reader,
exporter or export thread. `console` explicitly installs console exporters, emits
spans on completion and cumulative metrics every 10 seconds and at orderly
shutdown. No network exporter is installed. Standard `OTEL_*` exporter variables
do not enable network export. OTLP/HTTP is deferred until a transport-specific
configuration, privacy and failure policy can be tested. No production telemetry
backend is required.

All state-transition counters are recorded **after** the existing transaction
context exits successfully. A rejected owner, transaction abort or lost commit
confirmation cannot emit a confirmed transition signal. Separate confirmed
transactions retain their measurements if a later transaction fails: a claim can
be counted even if receiver processing or acknowledgement fails. Recovery and
claim transaction boundaries, locks, database wall-time checks, retry budgets,
outbox atomicity, stable event IDs and receiver deduplication are preserved.
The dispatcher selects one additional fixed state column to distinguish an
expired reclaim; recovery uses `UPDATE ... RETURNING id` to count actual affected
rows. Neither changes eligibility or persisted values.

Span names are fixed `component.operation` values. API submission spans distinguish
created/replayed/conflict; lookup distinguishes found/not_found. Worker spans cover
claim, recovery, handler execution and fenced completion/failure. Dispatcher spans
cover claim, recovery scan, receiver invocation and fenced ack/retry/dead-letter.
Operator spans cover replay outcomes. Only explicit committed paths set transition
outcomes; errors use bounded codes with ERROR status and no status description.
The dispatcher recovery scan span ends inside the encompassing claim transaction:
its `observed` outcome describes the scan, **not** committed recovery. The enclosing
claim span includes commit, and recovery counters/logs follow confirmed commit.

Handler and receiver counters/durations describe **observations**, including
failures and interruptions. Handler return does not prove committed job success.
Receiver return accepts a duplicate as success and does not prove a new receiver
row or sender acknowledgement. Durations use `time.perf_counter()` in seconds;
receiver duration includes its independent transaction and commit confirmation.
Histogram boundaries are 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30 and 60 seconds.

A fresh API process cannot supply a parent trace to a later worker or dispatcher:
queue rows do not persist context. There is no incoming-header extraction or
end-to-end cross-process trace guarantee. Within a process, nested operations can
share context. Telemetry is never used to determine ownership or durable state.

## Schema and privacy

See the README metric table for the full instrument/outcome mapping. Every metric
point and span uses only `component`, `operation`, `outcome` and optional predefined
`error_code`. Instrument and span names are constant. No job/event IDs, payloads,
event JSON, idempotency keys, lease tokens, bind parameters, destinations,
credentials, exception names/messages or stack traces are exported. Exemplars are
disabled. Resources contain only the fixed service name; ambient resource detectors
and `OTEL_RESOURCE_ATTRIBUTES` are not used.

Both automatic exception recording and automatic exception-to-status conversion
are disabled in span scopes. Errors are classified without exporting their text;
status descriptions and exception events remain absent. Unknown exceptions become
`unexpected_error`; process interruptions propagate. Application tracing does not
install logging handlers for SDK or driver diagnostics.

`TASK_ENGINE_LOG_FORMAT=human` retains human formatting by default. Opt-in `json`
uses only timestamp, level, component, event and outcome. The formatter ignores
raw messages, arguments, extra fields, exception information and stack information,
including framework records routed through configured Uvicorn/root handlers.
Existing human diagnostics retain necessary job/event UUIDs and fixed error codes;
UUIDs never enter metric labels or spans. No verbose SQL/driver logging is enabled.
Integrators must preserve this policy when adding handlers or infrastructure logs.

## Availability and operational facts

Console exporter failures, instrument failures and span finishing failures are
isolated from business results without logging arbitrary diagnostics. Abrupt
process termination can still lose telemetry. Console output has formatting/I/O
cost and is intended for local use. Orderly shutdown is best-effort; no durable
telemetry queue or guarantee of final export exists.

Counters are cumulative within one process, reset on restart and are best-effort.
Crashes between commit and recording, unknown commits, disabled periods and failed
export mean their totals can differ from the database. Even confirmed-transition
counters are observations of confirmed commits, not an authoritative reconstruction
of current state. Never infer no receiver effect from a dead-letter measurement.

`python -m task_engine.diagnostics` uses a read-only connection and one SELECT
statement snapshot for aggregate jobs, outbox, due work, expired leases and dead
letters. This is authoritative for the statement's database snapshot/time, rather
than a sum of volatile counters. Due counts require both due time and remaining
budget. Empty grouped counts are `{}`. No rows, payloads, keys, cleanup or recovery
are exposed. Large-table aggregate cost remains a limitation; run it on demand.

Compose adds one dispatcher with the existing build, database configuration and
completed-migration dependency, without a public port. Existing fresh-install and
stopped-process upgrade paths remain. No collector, dashboard or extra service is
needed.

## Validation and follow-up

Use official `InMemorySpanExporter` and `InMemoryMetricReader`, real PostgreSQL
aborts, lost-response injection after actual commits, stale owners and concurrent
execution. Verify all signals against synthetic privacy markers and validate the
read-only snapshot without mutations. Retain all fail-closed disposable-database
safeguards. See [phase 3A validation](../phase-3a-validation.md).

References: [official Python instrumentation](https://opentelemetry.io/docs/languages/python/instrumentation/),
[official metric test reader](https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.export.html).
Future work includes OTLP transport/export policy, persisted trace correlation if
required, production aggregation/retention and real transport observability. These
require separate decisions; no new infrastructure is justified in this phase.
