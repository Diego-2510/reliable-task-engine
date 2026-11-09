import multiprocessing as mp
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields
from datetime import timedelta

import psycopg
import pytest
from sqlalchemy import event, func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from task_engine import dispatcher, worker
from task_engine.db import Session, engine
from task_engine.dispatcher import (
    DeliveryClaim,
    DeliveryLeaseLostError,
    RetryableDeliveryError,
    acknowledge_delivery,
    claim_delivery,
    deliver_to_synthetic_receiver,
    delivery_backoff_seconds,
    delivery_lease_seconds,
    run_once,
    schedule_delivery_retry,
)
from task_engine.models import Job, OutboxEvent


@pytest.fixture(autouse=True)
def delivery_duration(monkeypatch):
    monkeypatch.setenv("DELIVERY_LEASE_SECONDS", "60")


def add_event(**payload):
    with Session.begin() as session:
        job = Job(payload={"emit_outbox_event": True, **payload})
        session.add(job)
    assert worker.run_once()
    with Session() as session:
        return session.scalar(select(OutboxEvent.id).where(OutboxEvent.job_id == job.id))


def snapshot(event_id):
    with engine.connect() as connection:
        return dict(connection.execute(text("SELECT * FROM outbox_events WHERE id = :id"),
                                       {"id": event_id}).one()._mapping)


def receiver_rows():
    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(
            text("SELECT * FROM synthetic_receiver_events ORDER BY event_id"),
        )]


def db_now():
    with Session() as session:
        return session.scalar(select(func.clock_timestamp()))


def expire_delivery(event_id):
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE outbox_events SET delivery_lease_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id"
        ), {"id": event_id})


def make_delivery_due(event_id):
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE outbox_events SET next_attempt_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id"
        ), {"id": event_id})


def _parallel_dispatcher(barrier, results, mode):
    engine.dispose(close=False)
    barrier.wait(timeout=10)
    results.put(claim_delivery() if mode == "claim" else run_once())


def parallel_dispatchers(mode="claim"):
    ctx = mp.get_context("fork")
    barrier, results = ctx.Barrier(8), ctx.Queue()
    processes = [ctx.Process(target=_parallel_dispatcher, args=(barrier, results, mode))
                 for _ in range(8)]
    try:
        for process in processes:
            process.start()
        outcomes = [results.get(timeout=15) for _ in processes]
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
        return outcomes
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            if process.pid is not None:
                process.join(timeout=5)
        results.close()
        results.join_thread()


def test_pending_delivered_and_never_reclaimed(database):
    event_id = add_event()
    before = snapshot(event_id)
    assert before["delivery_state"] == "pending" and before["delivery_attempts"] == 0
    earliest = db_now()
    assert run_once()
    latest = db_now()
    after = snapshot(event_id)
    assert after["delivery_state"] == "delivered" and after["delivery_attempts"] == 1
    assert earliest <= after["delivered_at"] <= latest
    assert after["delivery_token"] is None and after["delivery_lease_expires_at"] is None
    assert all(after[key] == before[key] for key in ("id", "job_id", "event_type", "payload",
                                                    "created_at"))
    rows = receiver_rows()
    assert len(rows) == 1 and rows[0]["event_id"] == event_id
    assert earliest <= rows[0]["processed_at"] <= after["delivered_at"]
    make_delivery_due(event_id)  # Even due delivered rows remain terminal.
    assert claim_delivery() is None and not run_once()
    assert receiver_rows() == rows


def test_claim_committed_before_receiver_and_separate_ack(database, monkeypatch):
    event_id = add_event()
    original_receiver = deliver_to_synthetic_receiver
    original_ack = acknowledge_delivery
    calls = []

    def receive(claimed):
        assert engine.pool.checkedout() == 0
        committed = snapshot(event_id)
        assert committed["delivery_state"] == "delivering"
        assert committed["delivery_token"] == claimed.token
        assert committed["delivery_attempts"] == 1
        assert receiver_rows() == []
        original_receiver(claimed)
        calls.append("receiver_commit")

    def ack(received_id, token):
        assert engine.pool.checkedout() == 0
        assert len(receiver_rows()) == 1
        assert snapshot(event_id)["delivery_state"] == "delivering"
        calls.append("ack")
        original_ack(received_id, token)

    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", receive)
    monkeypatch.setattr(dispatcher, "acknowledge_delivery", ack)
    assert run_once()
    assert calls == ["receiver_commit", "ack"]


def test_claim_lease_uses_database_time_and_config(database, monkeypatch):
    event_id = add_event()
    monkeypatch.setenv("DELIVERY_LEASE_SECONDS", "120")
    earliest = db_now()
    claimed = claim_delivery()
    latest = db_now()
    row = snapshot(event_id)
    assert earliest + timedelta(seconds=120) <= row["delivery_lease_expires_at"]
    assert row["delivery_lease_expires_at"] <= latest + timedelta(seconds=120)
    assert row["delivery_token"] == claimed.token
    assert claimed.event_id == event_id
    assert claim_delivery() is None


@pytest.mark.parametrize("recovery", [False, True])
def test_eight_processes_claim_one_event_once(database, recovery):
    event_id = add_event()
    old = None
    if recovery:
        old = claim_delivery()
        expire_delivery(event_id)
    claims = [claimed for claimed in parallel_dispatchers() if claimed is not None]
    assert len(claims) == 1 and claims[0].event_id == event_id
    row = snapshot(event_id)
    assert row["delivery_state"] == "delivering"
    assert row["delivery_attempts"] == (2 if recovery else 1)
    assert row["delivery_token"] == claims[0].token
    assert old is None or old.token != claims[0].token
    assert row["delivery_lease_expires_at"] > db_now()


def test_eight_processes_own_different_events(database):
    ids = {add_event() for _ in range(8)}
    claims = parallel_dispatchers()
    assert {claimed.event_id for claimed in claims} == ids
    assert len({claimed.token for claimed in claims}) == 8
    assert claim_delivery() is None
    for claimed in claims:
        assert snapshot(claimed.event_id)["delivery_token"] == claimed.token
        assert snapshot(claimed.event_id)["delivery_attempts"] == 1
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(deliver_to_synthetic_receiver, claims))
        list(pool.map(lambda claimed: acknowledge_delivery(claimed.event_id, claimed.token), claims))
    assert len(receiver_rows()) == 8
    assert all(snapshot(event_id)["delivery_state"] == "delivered" for event_id in ids)


def test_eight_parallel_dispatchers_complete_independent_events(database):
    ids = {add_event() for _ in range(8)}
    assert parallel_dispatchers("run") == [True] * 8
    assert {row["event_id"] for row in receiver_rows()} == ids
    assert all(snapshot(event_id)["delivery_attempts"] == 1 for event_id in ids)
    assert all(snapshot(event_id)["delivery_state"] == "delivered" for event_id in ids)


def test_locked_event_is_skipped(database):
    locked, ready = add_event(), add_event()
    with Session.begin() as holder:
        holder.execute(select(OutboxEvent.id).where(OutboxEvent.id == locked).with_for_update())
        claimed = claim_delivery()
        assert claimed.event_id == ready
        assert claim_delivery() is None
    assert claim_delivery().event_id == locked


def test_receiver_deduplicates_concurrent_and_repeated_deliveries(database):
    add_event()
    claimed = claim_delivery()
    barrier = threading.Barrier(8)

    def receive(_):
        barrier.wait(timeout=5)
        deliver_to_synthetic_receiver(claimed)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(receive, range(8)))
    before = receiver_rows()
    assert len(before) == 1 and before[0]["event_id"] == claimed.event_id
    deliver_to_synthetic_receiver(claimed)
    assert receiver_rows() == before  # Original processed_at is never overwritten.
    other = DeliveryClaim(uuid.uuid4(), claimed.job_id, claimed.event_type, uuid.uuid4())
    deliver_to_synthetic_receiver(other)
    assert {row["event_id"] for row in receiver_rows()} == {claimed.event_id, other.event_id}
    assert snapshot(claimed.event_id)["delivery_state"] == "delivering"


def _crash_after_receiver():
    engine.dispose(close=False)
    claimed = claim_delivery()
    assert claimed is not None
    deliver_to_synthetic_receiver(claimed)
    os._exit(23)  # Abrupt process death, after real receiver COMMIT, without ack/finalizers.


def test_crash_after_receiver_commit_recovers_one_effect(database):
    event_id = add_event()
    process = mp.get_context("fork").Process(target=_crash_after_receiver)
    try:
        process.start()
        process.join(timeout=10)
        assert process.exitcode == 23
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=5)
    before = snapshot(event_id)
    rows = receiver_rows()
    assert before["delivery_state"] == "delivering" and before["delivery_attempts"] == 1
    assert len(rows) == 1 and rows[0]["event_id"] == event_id
    assert claim_delivery() is None
    expire_delivery(event_id)
    assert run_once()
    after = snapshot(event_id)
    assert after["delivery_state"] == "delivered" and after["delivery_attempts"] == 2
    assert after["id"] == event_id and receiver_rows() == rows
    with pytest.raises(DeliveryLeaseLostError):
        acknowledge_delivery(event_id, before["delivery_token"])


@pytest.mark.parametrize("transition", [acknowledge_delivery, schedule_delivery_retry])
@pytest.mark.parametrize("lost", ["reclaimed", "expired", "wrong_token", "missing", "delivered"])
def test_fences_ack_and_retry(database, transition, lost):
    event_id = add_event()
    claimed = claim_delivery()
    token = claimed.token
    if lost in ("reclaimed", "expired"):
        expire_delivery(event_id)
        if lost == "reclaimed":
            replacement = claim_delivery()
            assert replacement.event_id == event_id and replacement.token != token
    elif lost == "wrong_token":
        token = uuid.uuid4()
    elif lost == "missing":
        event_id = uuid.uuid4()
    elif lost == "delivered":
        deliver_to_synthetic_receiver(claimed)
        acknowledge_delivery(event_id, token)
    before = None if lost == "missing" else snapshot(event_id)
    with pytest.raises(DeliveryLeaseLostError):
        transition(event_id, token)
    if before is not None:
        assert snapshot(event_id) == before


@pytest.mark.parametrize("transition", [acknowledge_delivery, schedule_delivery_retry])
def test_expiry_checked_after_lock_wait(database, transition):
    event_id = add_event()
    claimed = claim_delivery()
    starting = threading.Event()
    backend = []

    def observe(conn, cursor, statement, parameters, context, executemany):
        if "FOR UPDATE" in statement and "outbox_events" in statement:
            backend.append(conn.connection.driver_connection.info.backend_pid)
            starting.set()

    event.listen(engine, "before_cursor_execute", observe)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            with Session.begin() as holder:
                holder.execute(select(OutboxEvent.id).where(
                    OutboxEvent.id == event_id,
                ).with_for_update())
                starting.clear()
                backend.clear()
                future = pool.submit(transition, event_id, claimed.token)
                assert starting.wait(timeout=5)
                deadline = time.monotonic() + 5
                while not holder.scalar(select(func.cardinality(func.pg_blocking_pids(backend[0])))):
                    assert time.monotonic() < deadline, "Dispatcher did not wait for row lock"
                holder.execute(text(
                    "UPDATE outbox_events SET delivery_lease_expires_at = clock_timestamp() "
                    "WHERE id = :id"
                ), {"id": event_id})
            with pytest.raises(DeliveryLeaseLostError):
                future.result(timeout=10)
    finally:
        event.remove(engine, "before_cursor_execute", observe)
    assert snapshot(event_id)["delivery_state"] == "delivering"
    assert snapshot(event_id)["delivery_attempts"] == 1


def test_temporary_receiver_error_schedules_backoff_and_later_delivers(database, monkeypatch, caplog):
    event_id = add_event()

    def fail(claimed):
        assert engine.pool.checkedout() == 0
        assert snapshot(event_id)["delivery_state"] == "delivering"
        raise RetryableDeliveryError("private-synthetic-error")

    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", fail)
    earliest = db_now()
    assert run_once()
    latest = db_now()
    row = snapshot(event_id)
    assert row["delivery_state"] == "pending" and row["delivery_attempts"] == 1
    assert row["delivery_token"] is None and row["delivery_lease_expires_at"] is None
    assert row["delivered_at"] is None
    assert earliest + timedelta(seconds=2) <= row["next_attempt_at"]
    assert row["next_attempt_at"] <= latest + timedelta(seconds=2)
    assert claim_delivery() is None and receiver_rows() == []
    make_delivery_due(event_id)
    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", deliver_to_synthetic_receiver)
    assert run_once()
    assert snapshot(event_id)["delivery_state"] == "delivered"
    assert snapshot(event_id)["delivery_attempts"] == 2
    assert "private-synthetic-error" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_retry_backoff_caps_without_attempt_budget(database):
    event_id = add_event()
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE outbox_events SET delivery_attempts = 1000 WHERE id = :id"
        ), {"id": event_id})
    for attempt in (1001, 1002):
        claimed = claim_delivery()
        assert claimed is not None
        earliest = db_now()
        schedule_delivery_retry(event_id, claimed.token)
        latest = db_now()
        row = snapshot(event_id)
        assert row["delivery_attempts"] == attempt and row["delivery_state"] == "pending"
        assert earliest + timedelta(seconds=60) <= row["next_attempt_at"]
        assert row["next_attempt_at"] <= latest + timedelta(seconds=60)
        make_delivery_due(event_id)
    assert run_once()
    assert snapshot(event_id)["delivery_attempts"] == 1003


@pytest.mark.parametrize("stage", ["before_insert", "after_insert"])
def test_receiver_database_error_rolls_back_and_never_acks(database, stage):
    event_id = add_event()

    def fail(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO synthetic_receiver_events"):
            cursor.execute("SELECT 1 / 0")

    hook = "before_cursor_execute" if stage == "before_insert" else "after_cursor_execute"
    event.listen(engine, hook, fail)
    try:
        with pytest.raises(DBAPIError):
            run_once()
    finally:
        event.remove(engine, hook, fail)
    row = snapshot(event_id)
    assert row["delivery_state"] == "delivering" and row["delivery_attempts"] == 1
    assert row["delivered_at"] is None and receiver_rows() == []
    expire_delivery(event_id)
    assert run_once()
    assert snapshot(event_id)["delivery_state"] == "delivered" and len(receiver_rows()) == 1


@pytest.mark.parametrize("fail_commit", [1, 2, 3])
def test_uncertain_commit_never_returns_confirmed_success(database, monkeypatch, fail_commit):
    """Lose the commit response after a real claim/receiver/ack commit, respectively."""
    event_id = add_event()
    original_commit = engine.dialect.do_commit
    commits = []

    def commit_then_disconnect(connection):
        original_commit(connection)
        commits.append(True)
        if len(commits) == fail_commit:
            raise psycopg.OperationalError("synthetic lost commit response")

    with monkeypatch.context() as scoped:
        scoped.setattr(engine.dialect, "do_commit", commit_then_disconnect)
        with pytest.raises(DBAPIError):
            run_once()
    assert len(commits) == fail_commit
    row = snapshot(event_id)
    assert row["delivery_attempts"] == 1
    if fail_commit == 3:
        assert row["delivery_state"] == "delivered"
        assert len(receiver_rows()) == 1 and claim_delivery() is None
    else:
        assert row["delivery_state"] == "delivering" and row["delivered_at"] is None
        assert len(receiver_rows()) == (1 if fail_commit == 2 else 0)
        expire_delivery(event_id)
        assert run_once()
        assert snapshot(event_id)["delivery_state"] == "delivered"
        assert len(receiver_rows()) == 1


def test_ack_database_failure_preserves_receiver_commit(database):
    event_id = add_event()

    def fail(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE outbox_events") and "delivered_at=" in statement:
            cursor.execute("SELECT 1 / 0")

    event.listen(engine, "after_cursor_execute", fail)
    try:
        with pytest.raises(DBAPIError):
            run_once()
    finally:
        event.remove(engine, "after_cursor_execute", fail)
    assert snapshot(event_id)["delivery_state"] == "delivering"
    rows = receiver_rows()
    assert len(rows) == 1
    expire_delivery(event_id)
    assert run_once()
    assert snapshot(event_id)["delivery_state"] == "delivered" and receiver_rows() == rows


def test_run_once_discards_stale_ack_after_overlapping_delivery(database, monkeypatch, caplog):
    event_id = add_event()
    replacement = []

    def overlap(claimed):
        deliver_to_synthetic_receiver(claimed)
        expire_delivery(event_id)
        replacement.append(claim_delivery())

    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", overlap)
    assert run_once()
    assert "Delivery lease lost" in caplog.text
    assert snapshot(event_id)["delivery_token"] == replacement[0].token
    assert snapshot(event_id)["delivery_state"] == "delivering"
    deliver_to_synthetic_receiver(replacement[0])
    acknowledge_delivery(event_id, replacement[0].token)
    assert len(receiver_rows()) == 1


def test_receiver_and_logs_exclude_sensitive_payloads(database, monkeypatch, caplog):
    marker = "private-synthetic-marker"
    event_id = add_event(credentials=marker, destination_url=marker, person=marker)
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE outbox_events SET payload = jsonb_build_object('private', CAST(:value AS text)) "
            "WHERE id = :id"
        ), {"id": event_id, "value": marker})
    original = deliver_to_synthetic_receiver

    def fail_once(claimed):
        assert {field.name for field in fields(claimed)} == {"event_id", "job_id", "event_type",
                                                            "token"}
        assert marker not in str(claimed)
        raise RetryableDeliveryError(marker)

    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", fail_once)
    assert run_once()
    make_delivery_due(event_id)
    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", original)
    assert run_once()
    rows = receiver_rows()
    assert len(rows) == 1 and set(rows[0]) == {"event_id", "job_id", "event_type", "processed_at"}
    assert rows[0]["event_type"] == "job.succeeded.v1"
    assert marker not in str(rows) + caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_main_stops_visibly_without_database_diagnostics(database, monkeypatch, caplog):
    add_event()

    def fail(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO synthetic_receiver_events"):
            cursor.execute("SELECT CAST('private-synthetic-marker' AS integer)")

    event.listen(engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(SystemExit) as stopped:
            dispatcher.main()
        assert stopped.value.code == 1
    finally:
        event.remove(engine, "before_cursor_execute", fail)
    assert "database_error" in caplog.text and "private-synthetic-marker" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)
    assert receiver_rows() == []


@pytest.mark.parametrize("transition", [acknowledge_delivery, schedule_delivery_retry])
def test_concurrent_same_token_transitions_have_one_winner(database, transition):
    event_id = add_event()
    claimed = claim_delivery()
    barrier = threading.Barrier(2)

    def finish(_):
        barrier.wait(timeout=5)
        try:
            transition(event_id, claimed.token)
        except DeliveryLeaseLostError:
            return "lost"
        return "committed"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(finish, range(2))) == ["committed", "lost"]
    assert snapshot(event_id)["delivery_attempts"] == 1


@pytest.mark.parametrize("values", [
    {"delivery_state": "unknown"},
    {"delivery_attempts": -1},
    {"delivery_state": "delivering"},
    {"delivery_token": uuid.uuid4()},
    {"delivery_lease_expires_at": func.clock_timestamp()},
    {"delivery_state": "delivered"},
    {"delivered_at": func.clock_timestamp()},
])
def test_delivery_database_constraints(database, values):
    event_id = add_event()
    with pytest.raises(IntegrityError), Session.begin() as session:
        session.execute(OutboxEvent.__table__.update().where(
            OutboxEvent.id == event_id,
        ).values(**values))
    assert snapshot(event_id)["delivery_state"] == "pending"


@pytest.mark.parametrize("attempt,expected", [(1, 2), (2, 4), (3, 8), (4, 16), (5, 32),
                                             (6, 60), (1000000, 60)])
def test_delivery_backoff(attempt, expected):
    assert delivery_backoff_seconds(attempt) == expected


@pytest.mark.parametrize("value", ["0", "-1", "abc", "1.5", ""])
def test_invalid_delivery_lease(monkeypatch, value):
    monkeypatch.setenv("DELIVERY_LEASE_SECONDS", value)
    with pytest.raises(ValueError):
        delivery_lease_seconds()


def test_default_delivery_lease(monkeypatch):
    monkeypatch.delenv("DELIVERY_LEASE_SECONDS", raising=False)
    assert delivery_lease_seconds() == 60
