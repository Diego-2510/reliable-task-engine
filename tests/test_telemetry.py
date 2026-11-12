"""Real PostgreSQL commit boundaries plus official in-memory SDK readers/exporters."""

import io
import json
import logging
import os
import subprocess
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from fastapi.testclient import TestClient
from opentelemetry.sdk.metrics import AlwaysOffExemplarFilter, MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import DBAPIError

from task_engine import dispatcher, operator, telemetry, worker
from task_engine.api import app
from task_engine.db import Session, engine
from task_engine.diagnostics import snapshot
from task_engine.models import Job, OutboxEvent, SyntheticReceiverEvent


@pytest.fixture
def observed(monkeypatch):
    reader = InMemoryMetricReader()
    exporter = InMemorySpanExporter()
    resource = Resource({"service.name": "task-engine-test"})
    traces = TracerProvider(resource=resource, shutdown_on_exit=False)
    traces.add_span_processor(SimpleSpanProcessor(exporter))
    metrics = MeterProvider(resource=resource, metric_readers=[reader], shutdown_on_exit=False,
                            exemplar_filter=AlwaysOffExemplarFilter())
    runtime = telemetry.Telemetry(traces, metrics)
    monkeypatch.setattr(telemetry, "_runtime", runtime)
    monkeypatch.setattr(telemetry, "_initialized", True)
    yield reader, exporter
    runtime.shutdown()


def points(observed, name):
    data = observed[0].get_metrics_data()
    if data is None:
        return []
    return [p for r in data.resource_metrics for s in r.scope_metrics for m in s.metrics
            if m.name == name for p in m.data.data_points]


def values(observed, name):
    return {p.attributes["outcome"]: p.value for p in points(observed, name)}


def submit(payload=None, key=None):
    with TestClient(app) as client:
        return client.post('/v1/jobs', json={"payload": payload or {}},
                           headers={} if key is None else {"Idempotency-Key": key})


def add_event():
    response = submit({"emit_outbox_event": True})
    assert response.status_code == 201
    worker.run_once()
    with Session() as session:
        return session.scalar(select(OutboxEvent.id))


def expire(table, row_id, field):
    with engine.begin() as connection:
        # All identifiers are test-owned constants, never inputs to the application.
        connection.execute(text(f"UPDATE {table} SET {field} = "
                                "clock_timestamp() - interval '1 second' WHERE id = :id"),
                           {"id": row_id})


def test_api_submission_replay_conflict_lookup(database, observed):
    first = submit({"synthetic": 1}, "synthetic-key")
    assert first.status_code == 201
    assert submit({"synthetic": 1}, "synthetic-key").status_code == 200
    assert submit({"synthetic": 2}, "synthetic-key").status_code == 409
    assert values(observed, "task_engine.job.submissions") == {
        "created": 1, "replayed": 1, "conflict": 1,
    }
    with TestClient(app) as client:
        assert client.get('/v1/jobs/' + first.json()['id']).status_code == 200
        assert client.get('/v1/jobs/' + str(uuid.uuid4())).status_code == 404
    spans = observed[1].get_finished_spans()
    assert [(s.name, s.attributes['outcome']) for s in spans] == [
        ('api.submit', 'created'), ('api.submit', 'replayed'), ('api.submit', 'conflict'),
        ('api.lookup', 'found'), ('api.lookup', 'not_found'),
    ]


@pytest.mark.parametrize('error,code,transition', [
    (None, 'succeeded', 'succeeded'),
    (worker.PermanentJobError('PRIVATE'), 'permanent_error', 'failed'),
    (worker.RetryableJobError('PRIVATE'), 'retryable_error', 'retry'),
    (RuntimeError('PRIVATE'), 'unexpected_error', 'retry'),
])
def test_worker_outcomes(database, observed, monkeypatch, error, code, transition):
    submit()
    if error:
        def fail(payload):
            raise error
        monkeypatch.setattr(worker, 'handle', fail)
    assert worker.run_once()
    assert values(observed, 'task_engine.job.claims') == {'claimed': 1}
    assert values(observed, 'task_engine.job.executions') == {code: 1}
    assert values(observed, 'task_engine.job.transitions') == {transition: 1}
    duration, = points(observed, 'task_engine.handler.duration')
    assert duration.count == 1 and duration.sum >= 0
    assert duration.attributes['outcome'] == code


@pytest.mark.parametrize('error,code,transition', [
    (None, 'succeeded', 'acknowledged'),
    (dispatcher.RetryableDeliveryError('PRIVATE'), 'retryable_error', 'retry'),
    (dispatcher.PermanentDeliveryError('PRIVATE'), 'permanent_error', 'dead_letter'),
    (RuntimeError('PRIVATE'), 'unexpected_error', None),
])
def test_delivery_outcomes(database, observed, monkeypatch, error, code, transition):
    add_event()
    if error:
        def fail(claimed):
            raise error
        monkeypatch.setattr(dispatcher, 'deliver_to_synthetic_receiver', fail)
    if transition is None:
        with pytest.raises(RuntimeError):
            dispatcher.run_once()
    else:
        assert dispatcher.run_once()
    assert values(observed, 'task_engine.delivery.attempts') == {'claimed': 1}
    assert values(observed, 'task_engine.delivery.receives') == {code: 1}
    assert values(observed, 'task_engine.delivery.transitions') == (
        {} if transition is None else {transition: 1})
    duration, = points(observed, 'task_engine.receiver.duration')
    assert duration.count == 1 and duration.sum >= 0


@pytest.mark.parametrize('exhausted', [False, True])
def test_expired_job_recovery_counts_after_commit(database, observed, exhausted):
    response = submit()
    job_id = uuid.UUID(response.json()['id'])
    claimed = worker.claim()
    if exhausted:
        with Session.begin() as session:
            session.execute(update(Job).values(max_attempts=1))
    expire('jobs', job_id, 'lease_expires_at')
    assert worker.recover_expired() == 1
    assert values(observed, 'task_engine.job.recoveries') == {
        'failed' if exhausted else 'retry': 1,
    }
    with pytest.raises(worker.LeaseLostError):
        worker.complete(*claimed[:2], 'succeeded')
    assert values(observed, 'task_engine.job.transitions') == {}


@pytest.mark.parametrize('exhausted', [False, True])
def test_delivery_recovery_and_replay(database, observed, exhausted):
    event_id = add_event()
    claimed = dispatcher.claim_delivery()
    if exhausted:
        with Session.begin() as session:
            session.execute(update(OutboxEvent).values(max_delivery_attempts=1))
    expire('outbox_events', event_id, 'delivery_lease_expires_at')
    replacement = dispatcher.claim_delivery()
    assert values(observed, 'task_engine.delivery.recoveries') == {
        'dead_letter' if exhausted else 'reclaimed': 1,
    }
    with pytest.raises(dispatcher.DeliveryLeaseLostError):
        dispatcher.acknowledge_delivery(event_id, claimed.token)
    assert values(observed, 'task_engine.delivery.transitions') == {}
    if exhausted:
        assert replacement is None
        operator.replay_event(event_id, 1)
        with pytest.raises(operator.ReplayError):
            operator.replay_event(event_id, 1)
        assert values(observed, 'task_engine.operator.replays') == {
            'replayed': 1, 'event_not_dead_letter': 1,
        }


@pytest.mark.parametrize('component', ['worker', 'dispatcher'])
@pytest.mark.parametrize('transition', ['success', 'retry', 'failure'])
@pytest.mark.parametrize('lost', ['wrong_token', 'expired'])
def test_stale_owners_never_count_transition(database, observed, component, transition, lost):
    if component == 'worker':
        response = submit()
        row_id, token, _ = worker.claim()
        assert str(row_id) == response.json()['id']
        if lost == 'expired':
            expire('jobs', row_id, 'lease_expires_at')
        else:
            token = uuid.uuid4()
        with pytest.raises(worker.LeaseLostError):
            if transition == 'success':
                worker.complete(row_id, token, 'succeeded')
            else:
                error = worker.RetryableJobError() if transition == 'retry' else worker.PermanentJobError()
                worker.fail(row_id, token, error)
        name = 'task_engine.job.transitions'
    else:
        row_id = add_event()
        claimed = dispatcher.claim_delivery()
        token = claimed.token
        if lost == 'expired':
            expire('outbox_events', row_id, 'delivery_lease_expires_at')
        else:
            token = uuid.uuid4()
        function = {'success': dispatcher.acknowledge_delivery,
                    'retry': dispatcher.schedule_delivery_retry,
                    'failure': dispatcher.dead_letter_delivery}[transition]
        with pytest.raises(dispatcher.DeliveryLeaseLostError):
            function(row_id, token)
        name = 'task_engine.delivery.transitions'
    assert values(observed, name) == {}
    assert observed[1].get_finished_spans()[-1].attributes['outcome'] == 'lease_lost'


@pytest.mark.parametrize('stage', ['submit', 'job_claim', 'job_finish', 'job_recover',
                                   'delivery_claim', 'delivery_finish', 'delivery_recover', 'receiver', 'replay'])
@pytest.mark.parametrize('uncertain', [False, True])
def test_commit_failure_never_counts_success(database, observed, monkeypatch, stage, uncertain):
    """Confirmed PostgreSQL abort or response loss after an actual DBAPI commit."""
    if stage.startswith('delivery') or stage in {'replay', 'receiver'}:
        row_id = add_event()
        if stage != 'delivery_claim':
            claimed = dispatcher.claim_delivery()
        if stage == 'delivery_recover':
            with Session.begin() as session:
                session.execute(update(OutboxEvent).values(max_delivery_attempts=1))
            expire('outbox_events', row_id, 'delivery_lease_expires_at')
        if stage == 'replay':
            dispatcher.dead_letter_delivery(row_id, claimed.token)
    elif stage in {'job_finish', 'job_recover'}:
        submit()
        claimed = worker.claim()
        if stage == 'job_recover':
            expire('jobs', claimed[0], 'lease_expires_at')
    elif stage == 'job_claim':
        submit()
    name = {
        'submit': 'task_engine.job.submissions', 'job_claim': 'task_engine.job.claims',
        'job_finish': 'task_engine.job.transitions', 'job_recover': 'task_engine.job.recoveries',
        'delivery_claim': 'task_engine.delivery.attempts',
        'delivery_finish': 'task_engine.delivery.transitions',
        'delivery_recover': 'task_engine.delivery.recoveries', 'replay': 'task_engine.operator.replays',
        'receiver': 'task_engine.delivery.receives',
    }[stage]
    before = values(observed, name)
    original = engine.dialect.do_commit
    calls = 0

    def fail_commit(connection):
        nonlocal calls
        calls += 1
        # Worker claim has its own independently committed empty recovery transaction.
        if stage == 'job_claim' and calls == 1:
            return original(connection)
        if uncertain:
            original(connection)
            raise psycopg.OperationalError('PRIVATE lost commit response')
        cursor = connection.cursor()
        try:
            cursor.execute('SELECT 1 / 0')  # Actual PostgreSQL transaction abort.
        finally:
            cursor.close()

    with monkeypatch.context() as scoped:
        scoped.setattr(engine.dialect, 'do_commit', fail_commit)
        if stage == 'submit':
            assert submit().status_code == 500
        else:
            with pytest.raises(DBAPIError):
                if stage == 'job_claim':
                    worker.claim()
                elif stage == 'job_finish':
                    worker.complete(*claimed[:2], 'succeeded')
                elif stage == 'job_recover':
                    worker.recover_expired()
                elif stage in {'delivery_claim', 'delivery_recover'}:
                    dispatcher.claim_delivery()
                elif stage == 'receiver':
                    dispatcher._invoke_receiver(claimed)
                elif stage == 'delivery_finish':
                    dispatcher.acknowledge_delivery(row_id, claimed.token)
                else:
                    operator.replay_event(row_id, 1)
    after = values(observed, name)
    after.pop('database_error', None)  # Operator rejection/error observations are allowed.
    assert after == before
    assert observed[1].get_finished_spans()[-1].attributes['outcome'] == 'database_error'
    if stage == 'submit':
        with Session() as session:
            assert len(session.scalars(select(Job)).all()) == int(uncertain)


@pytest.mark.parametrize('stage', ['receiver', 'job_finish', 'delivery_finish'])
def test_real_statement_rollback_no_false_success(database, observed, stage):
    event_id = add_event() if stage != 'job_finish' else None
    if stage == 'job_finish':
        submit()
        claimed = worker.claim()
    elif stage == 'delivery_finish':
        claimed = dispatcher.claim_delivery()
    name = {'receiver': 'task_engine.delivery.transitions', 'job_finish': 'task_engine.job.transitions',
            'delivery_finish': 'task_engine.delivery.transitions'}[stage]
    before = values(observed, name)

    def abort(conn, cursor, statement, parameters, context, executemany):
        prefix = 'INSERT INTO synthetic_receiver_events' if stage == 'receiver' else (
            'UPDATE jobs' if stage == 'job_finish' else 'UPDATE outbox_events')
        if statement.startswith(prefix):
            cursor.execute('SELECT 1 / 0')

    event.listen(engine, 'after_cursor_execute', abort)
    try:
        with pytest.raises(DBAPIError):
            if stage == 'receiver':
                dispatcher.run_once()
            elif stage == 'job_finish':
                worker.complete(*claimed[:2], 'succeeded')
            else:
                dispatcher.acknowledge_delivery(event_id, claimed.token)
    finally:
        event.remove(engine, 'after_cursor_execute', abort)
    assert values(observed, name) == before
    if stage == 'receiver':
        assert values(observed, 'task_engine.delivery.receives') == {'database_error': 1}
        with Session() as session:
            assert session.scalars(select(SyntheticReceiverEvent)).all() == []


def test_privacy_all_signals(database, observed, monkeypatch, caplog):
    marker = 'PRIVATE-payload-key-exception-credential'
    caplog.set_level(logging.INFO, logger='task_engine')
    response = submit({'secret': marker, 'emit_outbox_event': True}, marker)
    assert response.status_code == 201
    worker.run_once()
    def fail(claimed):
        raise dispatcher.RetryableDeliveryError(marker)
    monkeypatch.setattr(dispatcher, 'deliver_to_synthetic_receiver', fail)
    dispatcher.run_once()
    spans = observed[1].get_finished_spans()
    serialized = str([(s.name, s.attributes, s.events, s.status.description, s.resource.attributes)
                      for s in spans])
    assert marker not in serialized + str(observed[0].get_metrics_data()) + caplog.text
    for span in spans:
        assert set(span.attributes) <= {'component', 'operation', 'outcome', 'error_code'}
        assert not span.events and span.status.description is None
    for resource in observed[0].get_metrics_data().resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                for point in metric.data.data_points:
                    assert set(point.attributes) <= {'component', 'operation', 'outcome', 'error_code'}
                    assert not point.exemplars
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(telemetry.JsonFormatter())
    for record in caplog.records:
        handler.handle(record)
    hostile = logging.LogRecord('task_engine.worker', logging.ERROR, __file__, 1, marker,
                                (marker,), (RuntimeError, RuntimeError(marker), None))
    hostile.stack_info = marker
    hostile.event = marker
    hostile.outcome = marker
    handler.handle(hostile)
    assert marker not in stream.getvalue()
    for line in stream.getvalue().splitlines():
        assert set(json.loads(line)) == {'timestamp', 'level', 'component', 'event', 'outcome'}


def test_concurrent_submission_execution_and_delivery(database, observed):
    barrier = threading.Barrier(8)
    def create(_):
        barrier.wait(timeout=10)
        return submit({'emit_outbox_event': True}, 'synthetic-concurrent').status_code
    with ThreadPoolExecutor(8) as pool:
        assert sorted(pool.map(create, range(8))) == [200] * 7 + [201]
        list(pool.map(lambda _: worker.run_once(), range(8)))
        list(pool.map(lambda _: dispatcher.run_once(), range(8)))
    assert values(observed, 'task_engine.job.submissions') == {'created': 1, 'replayed': 7}
    assert values(observed, 'task_engine.job.claims') == {'claimed': 1}
    assert values(observed, 'task_engine.job.transitions') == {'succeeded': 1}
    assert values(observed, 'task_engine.delivery.attempts') == {'claimed': 1}
    assert values(observed, 'task_engine.delivery.transitions') == {'acknowledged': 1}
    with Session() as session:
        assert len(session.scalars(select(Job)).all()) == 1
        assert len(session.scalars(select(OutboxEvent)).all()) == 1
        assert len(session.scalars(select(SyntheticReceiverEvent)).all()) == 1


def test_disabled_has_no_providers_or_network_export(database, monkeypatch):
    monkeypatch.setattr(telemetry, '_runtime', None)
    monkeypatch.setattr(telemetry, '_initialized', False)
    monkeypatch.delenv('TASK_ENGINE_TELEMETRY', raising=False)
    def forbidden(*args, **kwargs):
        pytest.fail('disabled telemetry constructed an SDK provider')
    monkeypatch.setattr(telemetry, 'TracerProvider', forbidden)
    monkeypatch.setattr(telemetry, 'MeterProvider', forbidden)
    with TestClient(app):
        event_id = add_event()
        assert dispatcher.run_once()
    assert telemetry._initialized and telemetry._runtime is None
    with Session() as session:
        assert session.get(OutboxEvent, event_id).delivery_state == 'delivered'
    # Separate interpreter: SDK/exporter environment variables cannot enable network I/O.
    code = '''
import socket
socket.create_connection = lambda *a, **k: (_ for _ in ()).throw(AssertionError("network"))
from task_engine import telemetry
assert not telemetry._initialized and telemetry._runtime is None
telemetry.initialize("worker")
telemetry.record("worker", "claim", "claimed")
assert telemetry._runtime is None
'''
    environ = dict(os.environ, TASK_ENGINE_TELEMETRY='disabled', OTEL_TRACES_EXPORTER='otlp',
                   OTEL_METRICS_EXPORTER='otlp', OTEL_EXPORTER_OTLP_ENDPOINT='http://127.0.0.1:1')
    result = subprocess.run([sys.executable, '-c', code], env=environ,
                            capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0 and not result.stdout and not result.stderr


def test_console_initialized_once_and_explicit_resources(monkeypatch, capsys):
    monkeypatch.setattr(telemetry, '_runtime', None)
    monkeypatch.setattr(telemetry, '_initialized', False)
    monkeypatch.setenv('TASK_ENGINE_TELEMETRY', 'console')
    monkeypatch.setenv('OTEL_RESOURCE_ATTRIBUTES', 'private=PRIVATE')
    monkeypatch.setenv('OTEL_SERVICE_NAME', 'PRIVATE')
    monkeypatch.setattr(telemetry.atexit, 'register', lambda callback: None)
    telemetry.initialize('worker')
    runtime = telemetry._runtime
    try:
        telemetry.initialize('dispatcher')
        assert telemetry._runtime is runtime
        worker._execute_handler({})
        runtime.metrics.force_flush()
        output = capsys.readouterr().out
        assert 'worker.handle' in output and 'task_engine.handler.duration' in output
        assert 'task-engine-worker' in output and 'PRIVATE' not in output
    finally:
        runtime.shutdown()


def test_broken_exporter_and_instruments_do_not_change_commits(database, observed, monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError('PRIVATE exporter unavailable')
    monkeypatch.setattr(telemetry.ConsoleSpanExporter, 'export', unavailable)
    monkeypatch.setattr(telemetry.ConsoleMetricExporter, 'export', unavailable)
    assert telemetry.SafeConsoleSpanExporter().export([]).name == 'FAILURE'
    assert telemetry.SafeConsoleMetricExporter().export(None).name == 'FAILURE'
    runtime = telemetry._runtime
    for counter in runtime.counters.values():
        monkeypatch.setattr(counter, 'add', unavailable)
    monkeypatch.setattr(runtime.tracer, 'start_span', unavailable)
    add_event()
    assert dispatcher.run_once()
    assert snapshot()['outbox_by_delivery_state'] == {'delivered': 1}


def test_diagnostics_readonly_consistent_and_unchanged(database):
    assert snapshot() == {
        'jobs_by_status': {}, 'outbox_by_delivery_state': {}, 'due_pending_jobs': 0,
        'due_pending_outbox_events': 0, 'expired_job_leases': 0, 'expired_delivery_leases': 0,
        'dead_letter_events': 0,
    }
    event_id = add_event()
    claimed = dispatcher.claim_delivery()
    expire('outbox_events', event_id, 'delivery_lease_expires_at')
    pending = submit()
    running = worker.claim()
    expire('jobs', running[0], 'lease_expires_at')
    submit()
    def rows():
        with engine.connect() as connection:
            return [connection.execute(text(f'SELECT * FROM {table} ORDER BY 1')).all()
                    for table in ('jobs', 'outbox_events', 'synthetic_receiver_events')]
    before = rows()
    statements = []
    def inspect(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
        cursor.execute('SHOW transaction_read_only')
        assert cursor.fetchone()[0] == 'on'
    event.listen(engine, 'before_cursor_execute', inspect)
    try:
        result = snapshot()
    finally:
        event.remove(engine, 'before_cursor_execute', inspect)
    assert len(statements) == 1 and statements[0].lstrip().startswith('WITH')
    assert rows() == before
    assert result == {
        'jobs_by_status': {'succeeded': 1, 'running': 1, 'queued': 1},
        'outbox_by_delivery_state': {'delivering': 1}, 'due_pending_jobs': 1,
        'due_pending_outbox_events': 0, 'expired_job_leases': 1, 'expired_delivery_leases': 1,
        'dead_letter_events': 0,
    }
    assert claimed and pending.status_code == 201
    environ = dict(os.environ, DATABASE_URL=os.environ['TEST_DATABASE_URL'])
    process = subprocess.run([sys.executable, '-m', 'task_engine.diagnostics'], env=environ,
                             capture_output=True, text=True, timeout=10, check=False)
    assert process.returncode == 0 and json.loads(process.stdout) == result
    assert rows() == before


@pytest.mark.parametrize('component', ['worker', 'dispatcher'])
def test_concurrent_same_owner_has_one_transition_signal(database, observed, component):
    if component == 'worker':
        submit()
        claimed = worker.claim()
        finish = lambda: worker.complete(*claimed[:2], 'succeeded')
        error = worker.LeaseLostError
        name, value = 'task_engine.job.transitions', 'succeeded'
    else:
        add_event()
        claimed = dispatcher.claim_delivery()
        finish = lambda: dispatcher.acknowledge_delivery(claimed.event_id, claimed.token)
        error = dispatcher.DeliveryLeaseLostError
        name, value = 'task_engine.delivery.transitions', 'acknowledged'
    before = values(observed, name).get(value, 0)
    barrier = threading.Barrier(2)
    def contend(_):
        barrier.wait(timeout=10)
        try:
            finish()
        except error:
            return 'lost'
        return 'committed'
    with ThreadPoolExecutor(2) as pool:
        assert sorted(pool.map(contend, range(2))) == ['committed', 'lost']
    assert values(observed, name)[value] == before + 1


def test_monotonic_duration(database, observed, monkeypatch):
    times = iter([10.0, 10.125])
    monkeypatch.setattr(telemetry.time, 'perf_counter', lambda: next(times))
    worker._execute_handler({})
    duration, = points(observed, 'task_engine.handler.duration')
    assert duration.sum == 0.125


def test_diagnostics_dead_letter_and_due_counts(database):
    add_event()
    assert snapshot()['due_pending_outbox_events'] == 1
    claimed = dispatcher.claim_delivery()
    dispatcher.dead_letter_delivery(claimed.event_id, claimed.token)
    result = snapshot()
    assert result['outbox_by_delivery_state'] == {'dead_letter': 1}
    assert result['dead_letter_events'] == 1 and result['due_pending_outbox_events'] == 0


@pytest.mark.parametrize('case,code', [
    ('budget', 'invalid_budget'), ('missing', 'event_not_found'),
    ('state', 'event_not_dead_letter'), ('overflow', 'budget_or_counter_overflow'),
])
def test_operator_rejected_outcomes(database, observed, case, code):
    event_id = uuid.uuid4()
    if case in {'state', 'overflow'}:
        event_id = add_event()
    if case == 'overflow':
        claimed = dispatcher.claim_delivery()
        dispatcher.dead_letter_delivery(event_id, claimed.token)
        with Session.begin() as session:
            session.execute(update(OutboxEvent).values(replay_count=operator.PG_INTEGER_MAX))
    with pytest.raises(operator.ReplayError):
        operator.replay_event(event_id, 0 if case == 'budget' else 1)
    assert values(observed, 'task_engine.operator.replays') == {code: 1}
    assert observed[1].get_finished_spans()[-1].attributes['outcome'] == code


def test_json_logging_optin_and_human_default(monkeypatch):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root = logging.getLogger()
    monkeypatch.setattr(root, 'handlers', [handler])
    monkeypatch.setenv('TASK_ENGINE_LOG_FORMAT', 'human')
    telemetry.configure_logging()
    assert handler.formatter is None
    monkeypatch.setenv('TASK_ENGINE_LOG_FORMAT', 'json')
    telemetry.configure_logging()
    logger = logging.getLogger('task_engine.worker')
    logger.warning('PRIVATE', extra={'event': 'handle', 'outcome': 'retryable_error'})
    result = json.loads(stream.getvalue())
    assert result['component'] == 'worker' and result['event'] == 'handle'
    assert result['outcome'] == 'retryable_error' and 'PRIVATE' not in stream.getvalue()


@pytest.mark.parametrize('component', ['worker', 'dispatcher'])
@pytest.mark.parametrize('interrupt', [KeyboardInterrupt, SystemExit])
def test_processing_interruptions_remain_observations(
    database, observed, monkeypatch, component, interrupt,
):
    def stop(*args):
        raise interrupt('PRIVATE')
    if component == 'worker':
        submit()
        monkeypatch.setattr(worker, 'handle', stop)
        function = worker.run_once
        count_name, transition_name = 'task_engine.job.executions', 'task_engine.job.transitions'
    else:
        add_event()
        monkeypatch.setattr(dispatcher, 'deliver_to_synthetic_receiver', stop)
        function = dispatcher.run_once
        count_name = 'task_engine.delivery.receives'
        transition_name = 'task_engine.delivery.transitions'
    with pytest.raises(interrupt):
        function()
    assert values(observed, count_name) == {'interrupted': 1}
    assert values(observed, transition_name) == {}
