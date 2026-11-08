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
