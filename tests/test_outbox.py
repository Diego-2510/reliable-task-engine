import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import event, func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from test_engine import expire, make_due
from test_idempotency import submit

from task_engine import worker
from task_engine.db import Session, engine
from task_engine.models import Job, OutboxEvent
from task_engine.worker import (
    LeaseLostError,
    PermanentJobError,
    RetryableJobError,
    SyntheticEventIntent,
    claim,
    complete,
    handle,
    recover_expired,
    run_once,
)

INTENT = SyntheticEventIntent.JOB_SUCCEEDED


def add_outbox_job(**payload):
    with Session.begin() as session:
        job = Job(payload={"emit_outbox_event": True, **payload})
        session.add(job)
        session.flush()
        return job.id


def events():
    with Session() as session:
        return session.scalars(select(OutboxEvent)).all()


def job_snapshot(job_id):
    with engine.connect() as connection:
        return dict(connection.execute(
            text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id},
        ).one()._mapping)


def succeed(job_id, token):
    complete(job_id, token, "succeeded", event_intent=INTENT)


@pytest.mark.parametrize("opt_in", [True, False, None, "true", 1])
def test_success_opt_in_contract(database, opt_in):
    payload = {} if opt_in is None else {"emit_outbox_event": opt_in}
    response = submit(payload=payload)
    assert response.status_code == 201
    job_id = uuid.UUID(response.json()["id"])
    assert run_once()
    job = job_snapshot(job_id)
    assert job["status"] == "succeeded" and job["attempts"] == 1
    assert job["lease_token"] is None and job["lease_expires_at"] is None
    emitted = events()
    assert len(emitted) == (1 if opt_in is True else 0)
    if opt_in is True:
        record = emitted[0]
        assert isinstance(record.id, uuid.UUID)
        assert record.job_id == job_id and record.event_type == "job.succeeded.v1"
        assert record.payload == {"job_id": str(job_id), "event_version": 1}
        assert record.created_at.tzinfo is not None
        assert record.id == events()[0].id  # Stable across independent sessions.
    assert not run_once()
    assert len(events()) == len(emitted)


def test_no_event_before_handler_returns(database, monkeypatch):
    job_id = add_outbox_job()

    def inspect(payload):
        assert job_snapshot(job_id)["status"] == "running"
        assert events() == []
        return handle(payload)

    monkeypatch.setattr(worker, "handle", inspect)
    assert run_once()
    assert len(events()) == 1


def test_synthetic_permanent_error_has_no_event(database):
    job_id = add_outbox_job(fail=True)
    assert run_once()
    assert job_snapshot(job_id)["status"] == "failed"
    assert events() == []


@pytest.mark.parametrize("error,code", [
    (PermanentJobError("sensitive-handler-message"), "permanent_error"),
    (RetryableJobError("sensitive-handler-message"), "retryable_error"),
    (RuntimeError("sensitive-handler-message"), "unexpected_error"),
])
def test_failure_then_success_or_exhaustion(database, monkeypatch, caplog, error, code):
    job_id = add_outbox_job()

    def fail_handler(payload):
        raise error

    monkeypatch.setattr(worker, "handle", fail_handler)
    assert run_once()
    snapshot = job_snapshot(job_id)
    assert snapshot["last_error_code"] == code
    assert events() == []
    if isinstance(error, PermanentJobError):
        assert snapshot["status"] == "failed" and not run_once()
    else:
        assert snapshot["status"] == "queued" and not run_once()
        make_due(job_id)
        monkeypatch.setattr(worker, "handle", handle)
        assert run_once()
        assert job_snapshot(job_id)["status"] == "succeeded"
        assert job_snapshot(job_id)["attempts"] == 2
        assert len(events()) == 1
    assert "sensitive-handler-message" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.parametrize("error", [RetryableJobError(), RuntimeError()])
def test_exhausted_failures_never_enqueue(database, monkeypatch, error):
    job_id = add_outbox_job()

    def failing(payload):
        raise error

    monkeypatch.setattr(worker, "handle", failing)
    for attempt in (1, 2, 3):
        assert run_once()
        assert events() == []
        assert job_snapshot(job_id)["attempts"] == attempt
        make_due(job_id)
    assert job_snapshot(job_id)["status"] == "failed"


@pytest.mark.parametrize("lost", ["wrong_token", "expired", "recovered", "missing"])
def test_lost_lease_never_enqueues(database, lost):
    job_id = add_outbox_job()
    _, token, _ = claim()
    if lost == "wrong_token":
        token = uuid.uuid4()
    elif lost in ("expired", "recovered"):
        expire(job_id)
        if lost == "recovered":
            assert recover_expired() == 1
    elif lost == "missing":
        job_id = uuid.uuid4()
    before = None if lost == "missing" else job_snapshot(job_id)
    with pytest.raises(LeaseLostError):
        succeed(job_id, token)
    assert events() == []
    if before is not None:
        assert job_snapshot(job_id) == before


@pytest.mark.parametrize("duplicate_owner", [False, True])
def test_concurrent_completions_only_current_owner_enqueues(database, duplicate_owner):
    job_id = add_outbox_job()
    _, old_token, _ = claim()
    if duplicate_owner:
        new_token = old_token
    else:
        expire(job_id)
        assert recover_expired() == 1
        make_due(job_id)
        _, new_token, _ = claim()
    barrier = threading.Barrier(2)

    def contender(token):
        barrier.wait(timeout=5)
        try:
            succeed(job_id, token)
        except LeaseLostError:
            return "lost"
        return "committed"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(contender, token) for token in (old_token, new_token)]
        outcomes = [future.result(timeout=10) for future in futures]
    assert sorted(outcomes) == ["committed", "lost"]
    if not duplicate_owner:
        assert outcomes == ["lost", "committed"]
    snapshot = job_snapshot(job_id)
    assert snapshot["status"] == "succeeded"
    assert snapshot["attempts"] == (1 if duplicate_owner else 2)
    emitted = events()
    assert len(emitted) == 1
    with pytest.raises(LeaseLostError):
        succeed(job_id, new_token)
    assert events()[0].id == emitted[0].id


def test_expiry_checked_after_waiting_for_job_lock(database):
    job_id = add_outbox_job()
    _, token, _ = claim()
    starting = threading.Event()
    backend = []

    def observe_lock(conn, cursor, statement, parameters, context, executemany):
        if "FOR UPDATE" in statement:
            backend.append(conn.connection.driver_connection.info.backend_pid)
            starting.set()

    event.listen(engine, "before_cursor_execute", observe_lock)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            # Hold the lock until the completing connection is demonstrably waiting.
            with Session.begin() as holder:
                holder.execute(select(Job).where(Job.id == job_id).with_for_update())
                starting.clear()
                backend.clear()
                future = pool.submit(succeed, job_id, token)
                assert starting.wait(timeout=5)
                deadline = time.monotonic() + 5
                while not holder.scalar(select(func.cardinality(func.pg_blocking_pids(backend[0])))):
                    assert time.monotonic() < deadline, "Completion did not wait for the row lock"
                holder.execute(text(
                    "UPDATE jobs SET lease_expires_at = clock_timestamp() WHERE id = :id"
                ), {"id": job_id})
            with pytest.raises(LeaseLostError):
                future.result(timeout=10)
    finally:
        event.remove(engine, "before_cursor_execute", observe_lock)
    assert job_snapshot(job_id)["status"] == "running"
    assert events() == []


def test_both_writes_invisible_until_commit(database):
    job_id = add_outbox_job()
    _, token, _ = claim()
    inserted, release = threading.Event(), threading.Event()

    def pause_after_insert(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO outbox_events"):
            inserted.set()
            assert release.wait(timeout=10)

    event.listen(engine, "after_cursor_execute", pause_after_insert)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(succeed, job_id, token)
            try:
                assert inserted.wait(timeout=5)
                assert job_snapshot(job_id)["status"] == "running"
                assert events() == []
                assert recover_expired() == 0
            finally:
                release.set()
            future.result(timeout=10)
    finally:
        event.remove(engine, "after_cursor_execute", pause_after_insert)
    assert job_snapshot(job_id)["status"] == "succeeded"
    assert len(events()) == 1


@pytest.mark.parametrize("stage", ["before_insert", "after_insert"])
def test_insert_failure_rolls_back_and_recovery_enqueues_once(database, caplog, stage):
    job_id = add_outbox_job(private_value="sensitive-synthetic-marker")
    claimed = []

    def fail_insert(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO outbox_events"):
            # Same connection sees the UPDATE, but an independent connection cannot.
            assert conn.scalar(text("SELECT status FROM jobs WHERE id = :id"),
                               {"id": job_id}) == "succeeded"
            claimed.append(job_snapshot(job_id))
            assert claimed[-1]["status"] == "running"
            cursor.execute("SELECT 1 / 0")  # Real PostgreSQL transaction abort.

    hook = "before_cursor_execute" if stage == "before_insert" else "after_cursor_execute"
    event.listen(engine, hook, fail_insert)
    try:
        with pytest.raises(DBAPIError):
            run_once()
    finally:
        event.remove(engine, hook, fail_insert)
    assert len(claimed) == 1
    assert job_snapshot(job_id) == claimed[0]  # Includes timestamps, lease and attempt.
    assert events() == []
    old_token = claimed[0]["lease_token"]
    expire(job_id)
    assert recover_expired() == 1
    assert events() == []
    make_due(job_id)
    assert run_once()
    emitted = events()
    assert len(emitted) == 1
    snapshot = job_snapshot(job_id)
    assert snapshot["status"] == "succeeded" and snapshot["attempts"] == 2
    assert snapshot["last_error_code"] == "lease_expired"
    with pytest.raises(LeaseLostError):
        succeed(job_id, old_token)
    assert events()[0].id == emitted[0].id
    assert "sensitive-synthetic-marker" not in caplog.text


def test_event_payload_allowlist_and_submission_replay(database, caplog):
    marker = "sensitive-synthetic-marker"
    payload = {"emit_outbox_event": True, "destination_url": marker,
               "credentials": marker, "person": marker, "event_type": marker}
    response = submit(marker, payload)
    job_id = uuid.UUID(response.json()["id"])
    assert run_once()
    record = events()[0]
    assert record.payload == {"job_id": str(job_id), "event_version": 1}
    assert record.event_type == "job.succeeded.v1"
    replay = submit(marker, payload)
    assert replay.status_code == 200 and replay.json()["status"] == "succeeded"
    assert not run_once()
    assert events()[0].id == record.id
    assert marker not in str(record.payload) + caplog.text
    assert all(log.exc_info is None for log in caplog.records)


def test_outbox_database_constraints(database):
    job_id = add_outbox_job()
    _, token, _ = claim()
    succeed(job_id, token)
    record = events()[0]
    for values in (
        {"job_id": job_id, "event_type": record.event_type},
        {"job_id": uuid.uuid4(), "event_type": record.event_type},
        {"job_id": job_id, "event_type": "unapproved.event"},
    ):
        with pytest.raises(IntegrityError), Session.begin() as session:
            session.add(OutboxEvent(payload={}, **values))
    assert len(events()) == 1


def test_failed_completion_rejects_intent(database):
    job_id = add_outbox_job()
    _, token, _ = claim()
    before = job_snapshot(job_id)
    with pytest.raises(ValueError, match="successful completion"):
        complete(job_id, token, "failed", event_intent=INTENT)
    assert job_snapshot(job_id) == before
    assert events() == []
