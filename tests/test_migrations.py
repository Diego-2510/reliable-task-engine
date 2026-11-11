import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from task_engine.db import Session, engine
from task_engine.models import Job
from task_engine.worker import claim


@pytest.mark.parametrize("start", ["0001", "0002"])
def test_upgrade_preserves_legacy_jobs_and_recovers_running(database, start):
    """Both upgrade paths preserve historical data, including attempts above 3/10."""
    config = Config("alembic.ini")
    command.downgrade(config, start)
    rows = [(status, attempts, uuid.uuid4())
            for status in ("queued", "running", "succeeded", "failed")
            for attempts in (0, 1, 3, 12)]
    try:
        with engine.begin() as connection:
            for status, attempts, job_id in rows:
                lease_columns = ", lease_token, lease_expires_at" if start == "0002" else ""
                lease_values = (
                    ", gen_random_uuid(), clock_timestamp()" if status == "running" else ", NULL, NULL"
                ) if start == "0002" else ""
                connection.execute(
                    text(f"INSERT INTO jobs (id, payload, status, attempts{lease_columns}) "
                         f"VALUES (:id, CAST(:payload AS jsonb), :status, :attempts{lease_values})"),
                    {"id": job_id, "status": status, "attempts": attempts, "payload": '{"legacy":true}'},
                )
            original = {row.id: row for row in connection.execute(text("SELECT * FROM jobs"))}
        command.upgrade(config, "head")
        with Session() as session:
            for status, attempts, job_id in rows:
                job = session.get(Job, job_id)
                exhausted = status == "queued" and attempts >= 3
                assert job.status == ("failed" if exhausted else status)
                assert job.attempts == attempts and job.max_attempts == max(3, attempts)
                assert job.payload == {"legacy": True}
                assert job.created_at == original[job_id].created_at
                assert job.updated_at == original[job_id].updated_at
                assert job.available_at == job.created_at
                assert job.last_error_code is None
                assert job.terminal_reason == (
                    "attempts_exhausted" if exhausted else "legacy_failure" if status == "failed" else None
                )
                if status == "running":
                    assert job.lease_token is not None
                    assert job.lease_expires_at <= session.scalar(select(func.clock_timestamp()))
                else:
                    assert job.lease_token is None and job.lease_expires_at is None
        # Polling recovers all expired rows even if they are ineligible for a claim.
        while claim() is not None:
            pass
        with Session() as session:
            for status, attempts, job_id in rows:
                job = session.get(Job, job_id)
                if status == "running":
                    assert job.status == ("queued" if attempts < 3 else "failed")
                    assert job.attempts == attempts
                    assert job.last_error_code == "lease_expired"
        with engine.begin() as connection:
            indexes = connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename = 'jobs' AND indexname = 'ix_jobs_due'"
            )).scalar_one()
            assert "available_at, created_at, id" in indexes and "queued" in indexes
        with pytest.raises(IntegrityError), engine.begin() as connection:
            connection.execute(text("INSERT INTO jobs (id,payload,status,attempts,max_attempts) "
                                    "VALUES (gen_random_uuid(),'{}','queued',0,0)"))
    finally:
        command.upgrade(config, "head")


def test_0004_preserves_every_existing_column(database):
    config = Config('alembic.ini')
    command.downgrade(config, '0003')
    try:
        with engine.begin() as connection:
            for status in ('queued', 'running', 'succeeded', 'failed'):
                connection.execute(text(
                    "INSERT INTO jobs (id,payload,status,attempts,max_attempts,lease_token,"
                    "lease_expires_at,last_error_code,terminal_reason) VALUES "
                    "(gen_random_uuid(),CAST(:payload AS jsonb),CAST(:status AS varchar),2,4,"
                    "CASE WHEN :status = 'running' THEN gen_random_uuid() END,"
                    "CASE WHEN :status = 'running' THEN clock_timestamp() END,"
                    "'retryable_error',CASE WHEN :status = 'failed' THEN 'attempts_exhausted' END)"
                ), {'status': status, 'payload': '{"legacy":true}'})
            before = {row.id: dict(row._mapping)
                      for row in connection.execute(text('SELECT * FROM jobs'))}
        command.upgrade(config, '0004')
        with engine.connect() as connection:
            after = {row.id: dict(row._mapping)
                     for row in connection.execute(text('SELECT * FROM jobs'))}
        assert before.keys() == after.keys()
        for job_id, values in after.items():
            assert values.pop('idempotency_key') is None
            assert values == before[job_id]
    finally:
        command.upgrade(config, 'head')


def test_0005_upgrade_and_downgrade_preserve_jobs(database):
    config = Config("alembic.ini")
    command.downgrade(config, "0004")
    try:
        with engine.begin() as connection:
            for status in ("queued", "running", "succeeded", "failed"):
                for attempts in (0, 2, 12):
                    connection.execute(text(
                        "INSERT INTO jobs (id,payload,status,attempts,max_attempts,idempotency_key,"
                        "created_at,updated_at,available_at,lease_token,lease_expires_at,"
                        "last_error_code,terminal_reason) VALUES "
                        "(:id,CAST(:payload AS jsonb),CAST(:status AS varchar),:attempts,14,:key,"
                        "clock_timestamp() - interval '3 days',"
                        "clock_timestamp() - interval '2 days',"
                        "clock_timestamp() - interval '1 day',"
                        "CASE WHEN :status = 'running' THEN gen_random_uuid() END,"
                        "CASE WHEN :status = 'running' THEN clock_timestamp() + interval '1 hour' END,"
                        "'retryable_error',CASE WHEN :status = 'failed' THEN 'attempts_exhausted' END)"
                    ), {"id": uuid.uuid4(), "status": status, "attempts": attempts,
                        "payload": '{"emit_outbox_event":true}',
                        "key": f"legacy-{status}-{attempts}" if attempts else None})
            before = [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM jobs ORDER BY id"),
            )]
            indexes_before = connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename = 'jobs' ORDER BY indexname"
            )).all()
        command.upgrade(config, "0005")
        with engine.begin() as connection:
            assert [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM jobs ORDER BY id"),
            )] == before
            assert connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename = 'jobs' ORDER BY indexname"
            )).all() == indexes_before
            assert connection.scalar(text("SELECT count(*) FROM outbox_events")) == 0
            connection.execute(text(
                "INSERT INTO outbox_events (id,job_id,event_type,payload) VALUES "
                "(gen_random_uuid(),:id,'job.succeeded.v1',jsonb_build_object("
                "'job_id',CAST(:id AS text),'event_version',1))"
            ), {"id": next(row["id"] for row in before if row["status"] == "succeeded")})
        command.downgrade(config, "0004")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT to_regclass('outbox_events')")) is None
            assert [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM jobs ORDER BY id"),
            )] == before
            assert connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename = 'jobs' ORDER BY indexname"
            )).all() == indexes_before
    finally:
        command.upgrade(config, "head")


def test_0006_preserves_all_jobs_and_existing_outbox_data(database):
    config = Config("alembic.ini")
    command.downgrade(config, "0005")
    try:
        with engine.begin() as connection:
            for status in ("queued", "running", "succeeded", "failed"):
                for attempts in (0, 2, 12):
                    job_id = uuid.uuid4()
                    connection.execute(text(
                        "INSERT INTO jobs (id,payload,status,attempts,max_attempts,idempotency_key,"
                        "created_at,updated_at,available_at,lease_token,lease_expires_at,"
                        "last_error_code,terminal_reason) VALUES "
                        "(:id,CAST(:payload AS jsonb),CAST(:status AS varchar),:attempts,14,:key,"
                        "clock_timestamp() - interval '3 days',"
                        "clock_timestamp() - interval '2 days',"
                        "clock_timestamp() - interval '1 day',"
                        "CASE WHEN :status = 'running' THEN gen_random_uuid() END,"
                        "CASE WHEN :status = 'running' THEN clock_timestamp() + interval '1 hour' END,"
                        "'retryable_error',CASE WHEN :status = 'failed' THEN 'attempts_exhausted' END)"
                    ), {"id": job_id, "status": status, "attempts": attempts,
                        "payload": '{"legacy":{"keep":[1,true,null]}}',
                        "key": f"legacy-{status}-{attempts}" if attempts else None})
                    if status == "succeeded":
                        connection.execute(text(
                            "INSERT INTO outbox_events (id,job_id,event_type,payload,created_at) "
                            "VALUES (:id,:job_id,'job.succeeded.v1',CAST(:payload AS jsonb),"
                            "clock_timestamp() - interval '2 days')"
                        ), {"id": uuid.uuid4(), "job_id": job_id,
                            "payload": '{"legacy":{"keep":[1,true,null]}}'})
            jobs_before = [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM jobs ORDER BY id"),
            )]
            events_before = [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM outbox_events ORDER BY id"),
            )]
            indexes_before = connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename IN ('jobs','outbox_events') "
                "ORDER BY indexname"
            )).all()
        earliest = None
        with engine.connect() as connection:
            earliest = connection.scalar(select(func.clock_timestamp()))
        command.upgrade(config, "0006")
        with engine.connect() as connection:
            latest = connection.scalar(select(func.clock_timestamp()))
            assert [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM jobs ORDER BY id"),
            )] == jobs_before
            events_after = [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM outbox_events ORDER BY id"),
            )]
            assert len(events_after) == len(events_before) == 3
            for old, new in zip(events_before, events_after, strict=True):
                assert new.pop("delivery_state") == "pending"
                assert new.pop("delivery_attempts") == 0
                assert earliest <= new.pop("next_attempt_at") <= latest
                assert new.pop("delivery_token") is None
                assert new.pop("delivery_lease_expires_at") is None
                assert new.pop("delivered_at") is None
                assert new == old
            assert connection.scalar(text("SELECT count(*) FROM synthetic_receiver_events")) == 0
            indexes_after = connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename IN ('jobs','outbox_events') "
                "ORDER BY indexname"
            )).all()
            assert set(indexes_before).issubset(set(indexes_after))
            delivery_indexes = [row[0] for row in indexes_after if "ix_outbox_" in row[0]]
            assert len(delivery_indexes) == 2
            assert any("next_attempt_at, created_at, id" in value and "pending" in value
                       for value in delivery_indexes)
            assert any("delivery_lease_expires_at, id" in value and "delivering" in value
                       for value in delivery_indexes)
        # Populate all delivery states and receiver data before testing the downgrade.
        # Seed the historical schema directly: current dispatcher requires 0007.
        with engine.begin() as connection:
            ids = connection.scalars(text("SELECT id FROM outbox_events ORDER BY id")).all()
            connection.execute(text(
                "UPDATE outbox_events SET delivery_state = 'delivered', delivery_attempts = 1, "
                "delivered_at = clock_timestamp() WHERE id = :id"
            ), {"id": ids[0]})
            connection.execute(text(
                "INSERT INTO synthetic_receiver_events (event_id,job_id,event_type) "
                "SELECT id,job_id,event_type FROM outbox_events WHERE id = :id"
            ), {"id": ids[0]})
            connection.execute(text(
                "UPDATE outbox_events SET delivery_state = 'delivering', delivery_attempts = 1, "
                "delivery_token = gen_random_uuid(), "
                "delivery_lease_expires_at = clock_timestamp() + interval '1 hour' WHERE id = :id"
            ), {"id": ids[1]})
        command.downgrade(config, "0005")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT to_regclass('synthetic_receiver_events')")) is None
            assert [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM jobs ORDER BY id"),
            )] == jobs_before
            assert [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM outbox_events ORDER BY id"),
            )] == events_before
            assert connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename IN ('jobs','outbox_events') "
                "ORDER BY indexname"
            )).all() == indexes_before
        command.upgrade(config, "0006")
        with engine.connect() as connection:
            assert connection.scalar(text(
                "SELECT count(*) FROM outbox_events WHERE delivery_state = 'pending' "
                "AND delivery_attempts = 0"
            )) == 3
            assert connection.scalar(text("SELECT count(*) FROM synthetic_receiver_events")) == 0
    finally:
        command.upgrade(config, "head")


def test_0007_preserves_history_and_recovers_only_exhausted_claims(database):
    from sqlalchemy.exc import DBAPIError

    from task_engine.dispatcher import acknowledge_delivery, claim_delivery

    config = Config("alembic.ini")
    command.downgrade(config, "0006")
    try:
        with engine.begin() as connection:
            for state in ("pending", "delivered", "delivering_valid", "delivering_expired"):
                for attempts in (0, 1, 5, 12, 2_147_483_647):
                    job_id, event_id = uuid.uuid4(), uuid.uuid4()
                    connection.execute(text(
                        "INSERT INTO jobs (id,payload,status,attempts) "
                        "VALUES (:id,CAST(:payload AS jsonb),'succeeded',1)"
                    ), {"id": job_id, "payload": '{"legacy":true}'})
                    connection.execute(text(
                        "INSERT INTO outbox_events (id,job_id,event_type,payload,created_at,"
                        "delivery_state,delivery_attempts,next_attempt_at,delivery_token,"
                        "delivery_lease_expires_at,delivered_at) VALUES "
                        "(:id,:job_id,'job.succeeded.v1','{\"legacy\":[1,true,null]}',"
                        "clock_timestamp() - interval '3 days',CAST(:state AS varchar),:attempts,"
                        "clock_timestamp() - interval '2 days',"
                        "CASE WHEN :state = 'delivering' THEN gen_random_uuid() END,"
                        "CASE WHEN :state = 'delivering' THEN clock_timestamp() + "
                        "CAST(:lease_delta AS interval) END,"
                        "CASE WHEN :state = 'delivered' THEN "
                        "clock_timestamp() - interval '1 day' END)"
                    ), {"id": event_id, "job_id": job_id, "attempts": attempts,
                        "state": state.split("_")[0],
                        "lease_delta": "1 hour" if state.endswith("valid") else "-1 hour"})
                    if state in ("delivered", "delivering_expired"):
                        connection.execute(text(
                            "INSERT INTO synthetic_receiver_events (event_id,job_id,event_type,"
                            "processed_at) VALUES (:id,:job_id,'job.succeeded.v1',"
                            "clock_timestamp() - interval '1 day')"
                        ), {"id": event_id, "job_id": job_id})
            before = [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM outbox_events ORDER BY id"))]
            jobs_before = connection.execute(text("SELECT * FROM jobs ORDER BY id")).all()
            receiver_before = connection.execute(text(
                "SELECT * FROM synthetic_receiver_events ORDER BY event_id")).all()
            indexes_before = connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename IN "
                "('jobs','outbox_events','synthetic_receiver_events') ORDER BY indexname")).all()
            earliest = connection.scalar(select(func.clock_timestamp()))
        command.upgrade(config, "0007")
        with engine.connect() as connection:
            latest = connection.scalar(select(func.clock_timestamp()))
            after = [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM outbox_events ORDER BY id"))]
            assert len(before) == len(after) == 20
            for old, new in zip(before, after, strict=True):
                exhausted = old["delivery_state"] == "pending" and old["delivery_attempts"] >= 5
                assert new.pop("max_delivery_attempts") == max(5, old["delivery_attempts"])
                assert new.pop("delivery_last_error_code") is None
                assert new.pop("replay_count") == 0 and new.pop("last_replayed_at") is None
                assert new.pop("dead_letter_reason") == ("attempts_exhausted" if exhausted else None)
                timestamp = new.pop("dead_lettered_at")
                if exhausted:
                    assert earliest <= timestamp <= latest
                    assert new.pop("delivery_state") == "dead_letter"
                    old = {key: value for key, value in old.items() if key != "delivery_state"}
                else:
                    assert timestamp is None
                assert new == old  # Every pre-existing column, including tokens and deadlines.
            assert connection.execute(text("SELECT * FROM jobs ORDER BY id")).all() == jobs_before
            assert connection.execute(text(
                "SELECT * FROM synthetic_receiver_events ORDER BY event_id")).all() == receiver_before
            assert connection.execute(text(
                "SELECT indexdef FROM pg_indexes WHERE tablename IN "
                "('jobs','outbox_events','synthetic_receiver_events') ORDER BY indexname"
            )).all() == indexes_before
        # Existing valid claims, including the integer maximum, can still acknowledge.
        now = latest
        for row in before:
            if (row["delivery_state"] == "delivering"
                    and row["delivery_lease_expires_at"] > now):
                acknowledge_delivery(row["id"], row["delivery_token"])
        claim_delivery()  # Also recovers exhausted expired claims; may claim an unrelated due row.
        with engine.connect() as connection:
            for row in before:
                if (row["delivery_state"] == "delivering"
                        and row["delivery_lease_expires_at"] < now
                        and row["delivery_attempts"] >= 5):
                    new = connection.execute(text("SELECT * FROM outbox_events WHERE id = :id"),
                                             {"id": row["id"]}).one()
                    assert new.delivery_state == "dead_letter"
                    assert new.delivery_attempts == row["delivery_attempts"]
                    assert new.dead_letter_reason == "attempts_exhausted"
                    assert new.delivery_last_error_code == "lease_expired"
            assert connection.execute(text(
                "SELECT * FROM synthetic_receiver_events ORDER BY event_id")).all() == receiver_before
            state_before_downgrade = connection.execute(text(
                "SELECT * FROM outbox_events ORDER BY id")).all()
        with pytest.raises(DBAPIError, match="resolve dead_letter"):
            command.downgrade(config, "0006")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0007"
            assert connection.execute(text(
                "SELECT * FROM outbox_events ORDER BY id")).all() == state_before_downgrade
    finally:
        command.upgrade(config, "head")


def test_0007_downgrade_preserves_0006_columns_and_receiver(database):
    from test_dispatcher import add_event, receiver_rows, snapshot

    from task_engine.dispatcher import claim_delivery, dead_letter_delivery, run_once
    from task_engine.operator import replay_event

    config = Config("alembic.ini")
    delivered = add_event()
    assert run_once()
    pending = add_event()
    owner = claim_delivery()
    dead_letter_delivery(pending, owner.token)
    replay_event(pending, 3)
    active = add_event()
    # Keep the replay pending and claim the other event.
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE outbox_events SET next_attempt_at = clock_timestamp() + interval '1 day' "
            "WHERE id = :id"
        ), {"id": pending})
    assert claim_delivery().event_id == active
    new_columns = {"max_delivery_attempts", "delivery_last_error_code", "dead_letter_reason",
                   "dead_lettered_at", "replay_count", "last_replayed_at"}
    before = {i: {key: value for key, value in snapshot(i).items() if key not in new_columns}
              for i in (delivered, pending, active)}
    rows = receiver_rows()
    try:
        command.downgrade(config, "0006")
        with engine.connect() as connection:
            assert {row.id: dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM outbox_events"))} == before
        assert receiver_rows() == rows
        command.upgrade(config, "0007")
        assert receiver_rows() == rows
        assert snapshot(pending)["replay_count"] == 0  # Downgrade discards replay audit.
        assert snapshot(pending)["max_delivery_attempts"] == 5
    finally:
        command.upgrade(config, "head")
