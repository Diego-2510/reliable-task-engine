import uuid

from alembic import command
from alembic.config import Config
from sqlalchemy import func, select, text

from task_engine.db import Session, engine
from task_engine.models import Job
from task_engine.worker import claim


def test_upgrade_preserves_legacy_jobs_and_recovers_running(database):
    """Exercise 0001 -> 0002, including existing rows, on the guarded test database."""
    config = Config("alembic.ini")
    command.downgrade(config, "0001")
    ids = {status: uuid.uuid4() for status in ("queued", "running", "succeeded", "failed")}
    try:
        with engine.begin() as connection:
            for status, job_id in ids.items():
                connection.execute(
                    text("INSERT INTO jobs (id, payload, status, attempts) "
                         "VALUES (:id, CAST(:payload AS jsonb), :status, 3)"),
                    {"id": job_id, "status": status, "payload": '{"legacy":true}'},
                )
        command.upgrade(config, "head")
        with Session() as session:
            for status, job_id in ids.items():
                job = session.get(Job, job_id)
                assert job.status == status and job.attempts == 3
                assert job.payload == {"legacy": True}
                if status == "running":
                    assert job.lease_token is not None
                    assert job.lease_expires_at <= session.scalar(select(func.clock_timestamp()))
                else:
                    assert job.lease_token is None and job.lease_expires_at is None
        claimed = [claim(), claim()]
        assert {result[0] for result in claimed} == {ids["queued"], ids["running"]}
        assert claim() is None
        with Session() as session:
            for job_id in (ids["queued"], ids["running"]):
                assert session.get(Job, job_id).attempts == 4
    finally:
        command.upgrade(config, "head")
