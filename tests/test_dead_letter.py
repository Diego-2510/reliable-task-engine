"""Phase 2D-3 protocol checks against explicitly disposable PostgreSQL."""

import multiprocessing as mp
import os
import subprocess
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from sqlalchemy import event, func, select, update
from sqlalchemy.exc import IntegrityError
from test_dispatcher import (
    _crash_after_receiver,
    add_event,
    db_now,
    expire_delivery,
    make_delivery_due,
    receiver_rows,
    snapshot,
)

from task_engine import dispatcher, operator
from task_engine.db import Session, engine
from task_engine.dispatcher import (
    DeliveryLeaseLostError,
    PermanentDeliveryError,
    RetryableDeliveryError,
    acknowledge_delivery,
    claim_delivery,
    dead_letter_delivery,
    run_once,
    schedule_delivery_retry,
)
from task_engine.models import OutboxEvent
from task_engine.operator import PG_INTEGER_MAX, ReplayError, replay_event


@pytest.fixture(autouse=True)
def delivery_duration(monkeypatch):
    monkeypatch.setenv("DELIVERY_LEASE_SECONDS", "60")


def set_fields(event_id, **values):
    with Session.begin() as session:
        session.execute(update(OutboxEvent).where(OutboxEvent.id == event_id).values(**values))


def dead_event():
    event_id = add_event()
    set_fields(event_id, max_delivery_attempts=1)
    claimed = claim_delivery()
    dead_letter_delivery(event_id, claimed.token)
    return event_id, claimed


def assert_dead(row, reason, code):
    assert row["delivery_state"] == "dead_letter"
    assert row["dead_letter_reason"] == reason
    assert row["delivery_last_error_code"] == code
    assert row["dead_lettered_at"] is not None
    assert row["delivery_token"] is None and row["delivery_lease_expires_at"] is None
    assert row["delivered_at"] is None


def test_retryable_failures_stop_at_default_budget(database, monkeypatch, caplog):
    event_id = add_event(private="synthetic-private-marker")

    def reject(claimed):
        raise RetryableDeliveryError("synthetic-private-marker")

    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", reject)
    for attempt in range(1, 6):
        assert run_once()
        row = snapshot(event_id)
        assert row["delivery_attempts"] == attempt and row["max_delivery_attempts"] == 5
        assert row["delivery_last_error_code"] == "retryable_error"
        if attempt < 5:
            assert row["delivery_state"] == "pending"
            assert row["next_attempt_at"] > db_now()
            assert claim_delivery() is None
            make_delivery_due(event_id)
    assert_dead(row, "attempts_exhausted", "retryable_error")
    before = snapshot(event_id)
    assert claim_delivery() is None and not run_once()
    assert snapshot(event_id) == before and receiver_rows() == []
    assert "synthetic-private-marker" not in caplog.text


def test_permanent_receiver_failure_dead_letters_first_attempt(database, monkeypatch, caplog):
    event_id = add_event()

    def reject(claimed):
        raise PermanentDeliveryError("synthetic-private-marker")

    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", reject)
    earliest = db_now()
    assert run_once()
    row = snapshot(event_id)
    assert_dead(row, "permanent_error", "permanent_error")
    assert row["delivery_attempts"] == 1 < row["max_delivery_attempts"]
    assert earliest <= row["dead_lettered_at"] <= db_now()
    assert not run_once() and receiver_rows() == []
    assert "synthetic-private-marker" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_final_attempt_can_acknowledge(database):
    event_id = add_event()
    set_fields(event_id, max_delivery_attempts=1)
    assert run_once()
    row = snapshot(event_id)
    assert row["delivery_state"] == "delivered" and row["delivery_attempts"] == 1
    assert row["dead_lettered_at"] is None and row["dead_letter_reason"] is None
    assert claim_delivery() is None


def test_expired_exhausted_recovers_without_claim(database):
    event_id = add_event()
    set_fields(event_id, max_delivery_attempts=1)
    claimed = claim_delivery()
    assert claim_delivery() is None  # A valid final owner remains able to acknowledge.
    expire_delivery(event_id)
    earliest = db_now()
    assert claim_delivery() is None
    row = snapshot(event_id)
    assert_dead(row, "attempts_exhausted", "lease_expired")
    assert row["delivery_attempts"] == 1
    assert earliest <= row["dead_lettered_at"] <= db_now()
    assert row["id"] == claimed.event_id


def test_recovery_is_bounded_and_skips_locked_rows(database, monkeypatch):
    ids = [add_event() for _ in range(4)]
    for event_id in ids:
        set_fields(event_id, max_delivery_attempts=1)
        assert claim_delivery().event_id == event_id
    for event_id in ids:
        expire_delivery(event_id)
    monkeypatch.setattr(dispatcher, "RECOVERY_BATCH_SIZE", 2)
    with Session.begin() as holder:
        holder.execute(select(OutboxEvent.id).where(OutboxEvent.id == ids[0]).with_for_update())
        assert claim_delivery() is None
        assert sum(snapshot(i)["delivery_state"] == "dead_letter" for i in ids) == 2
        assert snapshot(ids[0])["delivery_state"] == "delivering"
        assert claim_delivery() is None
        assert sum(snapshot(i)["delivery_state"] == "dead_letter" for i in ids) == 3
    assert claim_delivery() is None
    assert all(snapshot(i)["delivery_state"] == "dead_letter" for i in ids)
    assert all(snapshot(i)["delivery_attempts"] == 1 for i in ids)


@pytest.mark.parametrize("transition", [acknowledge_delivery, schedule_delivery_retry,
                                         dead_letter_delivery])
@pytest.mark.parametrize("phase", ["recovery", "replay", "new_owner"])
def test_old_owner_cannot_overwrite_recovery_or_replay(database, transition, phase):
    event_id = add_event()
    set_fields(event_id, max_delivery_attempts=1)
    old = claim_delivery()
    expire_delivery(event_id)
    assert claim_delivery() is None
    if phase != "recovery":
        replay_event(event_id, 3)
    if phase == "new_owner":
        assert claim_delivery().token != old.token
    before = snapshot(event_id)
    with pytest.raises(DeliveryLeaseLostError):
        transition(event_id, old.token)
    assert snapshot(event_id) == before


def test_final_attempt_process_crash_and_replay_deduplicate(database):
    event_id = add_event()
    set_fields(event_id, max_delivery_attempts=1)
    process = mp.get_context("fork").Process(target=_crash_after_receiver)
    try:
        process.start()
        process.join(timeout=10)
        assert process.exitcode == 23
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=5)
    rows = receiver_rows()
    before = snapshot(event_id)
    assert len(rows) == 1 and rows[0]["event_id"] == event_id
    assert before["delivery_state"] == "delivering" and before["delivery_attempts"] == 1
    expire_delivery(event_id)
    assert not run_once()
    assert_dead(snapshot(event_id), "attempts_exhausted", "lease_expired")
    assert receiver_rows() == rows  # Dead-letter does not mean no receiver effect.
    replay_event(event_id, 3)
    assert run_once()
    after = snapshot(event_id)
    assert after["delivery_state"] == "delivered" and after["delivery_attempts"] == 2
    assert after["id"] == event_id and after["max_delivery_attempts"] == 4
    assert receiver_rows() == rows  # Including original processed_at.


def test_replay_preserves_history_and_grants_only_added_budget(database):
    event_id, claimed = dead_event()
    dispatcher.deliver_to_synthetic_receiver(claimed)
    rows = receiver_rows()
    before = snapshot(event_id)
    earliest = db_now()
    replay_event(event_id, 3)
    after = snapshot(event_id)
    assert after["delivery_state"] == "pending"
    assert after["max_delivery_attempts"] == before["max_delivery_attempts"] + 3
    assert after["delivery_attempts"] == before["delivery_attempts"] == 1
    assert after["replay_count"] == before["replay_count"] + 1
    assert earliest <= after["last_replayed_at"] <= db_now()
    assert earliest <= after["next_attempt_at"] <= db_now()
    for field in ("dead_letter_reason", "dead_lettered_at", "delivery_token",
                  "delivery_lease_expires_at", "delivered_at"):
        assert after[field] is None
    for field in ("id", "job_id", "event_type", "payload", "created_at",
                  "delivery_last_error_code"):
        assert after[field] == before[field]
    assert receiver_rows() == rows
    for attempt in (2, 3, 4):
        owner = claim_delivery()
        assert owner.event_id == event_id and owner.token != claimed.token
        schedule_delivery_retry(event_id, owner.token)
        assert snapshot(event_id)["delivery_attempts"] == attempt
        if attempt < 4:
            make_delivery_due(event_id)
    assert_dead(snapshot(event_id), "attempts_exhausted", "retryable_error")
    assert claim_delivery() is None
    replay_event(event_id, 1)
    assert snapshot(event_id)["replay_count"] == 2
    assert snapshot(event_id)["delivery_attempts"] == 4
    assert snapshot(event_id)["max_delivery_attempts"] == 5


def test_two_operator_replays_have_one_winner(database):
    event_id, _ = dead_event()
    barrier = threading.Barrier(2)

    def replay(_):
        barrier.wait(timeout=5)
        try:
            replay_event(event_id, 3)
        except ReplayError as error:
            return error.code
        return "committed"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(replay, range(2))) == ["committed", "event_not_dead_letter"]
    row = snapshot(event_id)
    assert row["max_delivery_attempts"] == 4 and row["replay_count"] == 1
    assert row["delivery_attempts"] == 1 and row["delivery_state"] == "pending"


@pytest.mark.parametrize("state", ["pending", "delivering", "delivered"])
def test_replay_rejects_other_states(database, state):
    event_id = add_event()
    if state == "delivering":
        claim_delivery()
    elif state == "delivered":
        assert run_once()
    before, rows = snapshot(event_id), receiver_rows()
    with pytest.raises(ReplayError) as rejected:
        replay_event(event_id, 3)
    assert rejected.value.code == "event_not_dead_letter" and rejected.value.exit_code == 4
    assert snapshot(event_id) == before and receiver_rows() == rows


@pytest.mark.parametrize("budget", [0, -1, 1001, PG_INTEGER_MAX, True, 1.5, "3"])
def test_invalid_replay_budgets_rejected_without_database(budget):
    with pytest.raises(ReplayError) as rejected:
        replay_event(uuid.uuid4(), budget)
    assert rejected.value.code == "invalid_budget" and rejected.value.exit_code == 2


@pytest.mark.parametrize("ending", ["ack", "retry", "recovery"])
def test_claim_at_integer_boundary_cannot_overflow(database, ending):
    event_id = add_event()
    set_fields(event_id, delivery_attempts=PG_INTEGER_MAX - 1,
               max_delivery_attempts=PG_INTEGER_MAX)
    claimed = claim_delivery()
    assert snapshot(event_id)["delivery_attempts"] == PG_INTEGER_MAX
    if ending == "ack":
        acknowledge_delivery(event_id, claimed.token)
        assert snapshot(event_id)["delivery_state"] == "delivered"
    elif ending == "retry":
        schedule_delivery_retry(event_id, claimed.token)
        assert_dead(snapshot(event_id), "attempts_exhausted", "retryable_error")
    else:
        expire_delivery(event_id)
    assert claim_delivery() is None
    assert snapshot(event_id)["delivery_attempts"] == PG_INTEGER_MAX
    if ending != "ack":
        before = snapshot(event_id)
        with pytest.raises(ReplayError) as rejected:
            replay_event(event_id, 1)
        assert rejected.value.exit_code == 5
        assert snapshot(event_id) == before


@pytest.mark.parametrize("field", ["replay_count", "max_delivery_attempts"])
def test_replay_overflow_rejected_atomically(database, field):
    event_id, _ = dead_event()
    set_fields(event_id, **{field: PG_INTEGER_MAX})
    before = snapshot(event_id)
    with pytest.raises(ReplayError) as rejected:
        replay_event(event_id, 3)
    assert rejected.value.code == "budget_or_counter_overflow"
    assert snapshot(event_id) == before


def test_replay_exact_integer_budget_boundary(database):
    event_id, _ = dead_event()
    set_fields(event_id, max_delivery_attempts=PG_INTEGER_MAX - 3)
    replay_event(event_id, 3)
    assert snapshot(event_id)["max_delivery_attempts"] == PG_INTEGER_MAX
    assert snapshot(event_id)["delivery_attempts"] == 1


@pytest.mark.parametrize("values", [
    {"max_delivery_attempts": 0}, {"max_delivery_attempts": -1},
    {"replay_count": -1}, {"delivery_last_error_code": "sensitive-free-text"},
    {"dead_letter_reason": "sensitive-free-text"},
    {"dead_letter_reason": "attempts_exhausted"},
    {"dead_lettered_at": func.clock_timestamp()},
    {"delivery_state": "dead_letter"},
    {"delivery_state": "dead_letter", "dead_lettered_at": func.clock_timestamp()},
    {"delivery_state": "dead_letter", "dead_letter_reason": "permanent_error"},
    {"delivery_state": "dead_letter", "dead_letter_reason": "attempts_exhausted",
     "dead_lettered_at": func.clock_timestamp(), "delivery_token": uuid.uuid4(),
     "delivery_lease_expires_at": func.clock_timestamp()},
])
def test_dead_letter_constraints(database, values):
    event_id = add_event()
    before = snapshot(event_id)
    with pytest.raises(IntegrityError):
        set_fields(event_id, **values)
    assert snapshot(event_id) == before


def test_attempt_counter_cannot_be_reset_by_sql(database):
    event_id, _ = dead_event()
    before = snapshot(event_id)
    with pytest.raises(IntegrityError):
        set_fields(event_id, delivery_attempts=0)
    assert snapshot(event_id) == before


def test_unknown_receiver_failure_is_visible_and_left_for_recovery(database, monkeypatch, caplog):
    event_id = add_event()

    def reject(claimed):
        raise RuntimeError("synthetic-private-marker")

    monkeypatch.setattr(dispatcher, "deliver_to_synthetic_receiver", reject)
    with pytest.raises(RuntimeError):
        run_once()
    row = snapshot(event_id)
    assert row["delivery_state"] == "delivering"
    assert row["delivery_last_error_code"] is None and receiver_rows() == []
    expire_delivery(event_id)
    with pytest.raises(SystemExit) as stopped:
        dispatcher.main()
    assert stopped.value.code == 1
    assert "unexpected_error" in caplog.text and "synthetic-private-marker" not in caplog.text
    assert snapshot(event_id)["delivery_state"] == "delivering"


def test_operator_database_failure_rolls_back_and_sanitizes(database, capsys):
    event_id, _ = dead_event()
    before = snapshot(event_id)

    def fail(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE outbox_events"):
            cursor.execute("SELECT CAST('synthetic-private-marker' AS integer)")

    event.listen(engine, "after_cursor_execute", fail)
    try:
        assert operator.main(["replay-event", str(event_id), "--extra-attempts", "3"]) == 1
    finally:
        event.remove(engine, "after_cursor_execute", fail)
    output = capsys.readouterr()
    assert output.out == "" and output.err == "operator: database_error\n"
    assert snapshot(event_id) == before


def test_operator_uncertain_commit_is_not_reported_success(database, monkeypatch, capsys):
    event_id, _ = dead_event()
    original_commit = engine.dialect.do_commit

    def commit_then_disconnect(connection):
        original_commit(connection)
        raise psycopg.OperationalError("synthetic-private-marker")

    with monkeypatch.context() as scoped:
        scoped.setattr(engine.dialect, "do_commit", commit_then_disconnect)
        assert operator.main(["replay-event", str(event_id), "--extra-attempts", "3"]) == 1
    assert capsys.readouterr().err == "operator: database_error\n"
    row = snapshot(event_id)
    assert row["delivery_state"] == "pending" and row["max_delivery_attempts"] == 4
    assert row["replay_count"] == 1
    with pytest.raises(ReplayError):
        replay_event(event_id, 3)
    assert snapshot(event_id) == row


def cli(*args):
    return subprocess.run([sys.executable, "-m", "task_engine.operator", *args],
                          check=False, env=dict(os.environ), capture_output=True, text=True, timeout=10)


def test_local_operator_command_success_and_deterministic_errors(database):
    event_id, _ = dead_event()
    result = cli("replay-event", str(event_id), "--extra-attempts", "3")
    assert result.returncode == 0 and result.stdout == "operator: replay_scheduled\n"
    assert result.stderr == "" and snapshot(event_id)["max_delivery_attempts"] == 4
    again = cli("replay-event", str(event_id), "--extra-attempts", "3")
    assert again.returncode == 4 and again.stderr == "operator: event_not_dead_letter\n"
    missing = cli("replay-event", str(uuid.uuid4()), "--extra-attempts", "3")
    assert missing.returncode == 3 and missing.stderr == "operator: event_not_found\n"


@pytest.mark.parametrize("args", [
    ["replay-event", "synthetic-private-marker", "--extra-attempts", "3"],
    ["replay-event", str(uuid.uuid4()), "--extra-attempts", "synthetic-private-marker"],
    ["synthetic-private-marker"],
    ["replay-event", str(uuid.uuid4()), "--extra-attempts", "0"],
    ["replay-event", str(uuid.uuid4()), "--extra-attempts", "1001"],
])
def test_operator_invalid_input_never_echoes_input(args):
    result = cli(*args)
    assert result.returncode == 2
    assert "synthetic-private-marker" not in result.stderr + result.stdout
    assert "Traceback" not in result.stderr


def test_replay_adds_to_stored_limit_even_after_early_permanent_failure(database):
    event_id = add_event()
    claimed = claim_delivery()
    dead_letter_delivery(event_id, claimed.token)
    assert snapshot(event_id)["max_delivery_attempts"] == 5
    replay_event(event_id, 3)
    row = snapshot(event_id)
    assert row["delivery_attempts"] == 1 and row["max_delivery_attempts"] == 8


@pytest.mark.parametrize("stage", ["before_cursor_execute", "after_cursor_execute"])
def test_failed_claim_transaction_does_not_consume_an_attempt(database, stage):
    from sqlalchemy.exc import DBAPIError

    event_id = add_event()
    before = snapshot(event_id)

    def fail(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE outbox_events"):
            cursor.execute("SELECT 1 / 0")

    event.listen(engine, stage, fail)
    try:
        with pytest.raises(DBAPIError):
            claim_delivery()
    finally:
        event.remove(engine, stage, fail)
    assert snapshot(event_id) == before
    assert claim_delivery().event_id == event_id
    assert snapshot(event_id)["delivery_attempts"] == 1
