import multiprocessing as mp
import uuid
from contextlib import nullcontext
from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, update

from task_engine import worker as worker_module
from task_engine.api import app
from task_engine.db import Session
from task_engine.models import Job
from task_engine.worker import LeaseLostError, claim, complete, handle, run_once


def test_handler():
    handle({"anything": 1})
    with pytest.raises(ValueError):
        handle({"fail": True})


def test_api_and_worker(database):
    client = TestClient(app)
    assert client.post('/v1/jobs', json={"payload": []}).status_code == 422
    assert client.get('/v1/jobs/not-a-uuid').json()['error']['code'] == 'invalid_input'
    assert client.get('/v1/jobs/00000000-0000-0000-0000-000000000000').status_code == 404
    assert not run_once()
    response = client.post('/v1/jobs', json={'payload': {'fail': True}})
    assert response.status_code == 201
    assert run_once()
    job = client.get('/v1/jobs/' + response.json()['id']).json()
    assert job['status'] == 'failed' and job['attempts'] == 1
    response = client.post('/v1/jobs', json={'payload': {}})
    assert run_once()
    assert client.get('/v1/jobs/' + response.json()['id']).json()['status'] == 'succeeded'


def _contender(barrier, results):
    # Forked workers must establish their own database connections.
    from task_engine.db import engine
    engine.dispose(close=False)
    barrier.wait(timeout=10)
    results.put(claim())


@pytest.mark.parametrize("recovery", [False, True])
def test_concurrent_claim(database, recovery):
    job_id = add_job()
    old_token = None
    if recovery:
        first = claim()
        assert first is not None
        old_token = first[1]
        expire(job_id)
    ctx = mp.get_context("fork")
    barrier = ctx.Barrier(8)
    results = ctx.Queue()
    workers = [ctx.Process(target=_contender, args=(barrier, results)) for _ in range(8)]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=15)
            assert not worker.is_alive()
            assert worker.exitcode == 0
        claimed = [results.get(timeout=3) for _ in workers]
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            if worker.pid is not None:
                worker.join(timeout=5)
        results.close()
        results.join_thread()
    successful = [result for result in claimed if result is not None]
    assert len(successful) == 1
    with Session() as session:
        job = session.query(Job).one()
        assert job.status == "running" and job.attempts == (2 if recovery else 1)
        assert job.lease_token == successful[0][1]
        assert job.lease_token != old_token
        assert job.lease_expires_at > session.scalar(select(func.clock_timestamp()))


def add_job():
    with Session.begin() as session:
        job = Job(payload={"example": 1})
        session.add(job)
        session.flush()
        return job.id


def expire(job_id):
    with Session.begin() as session:
        session.execute(
            update(Job).where(Job.id == job_id)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )


def test_claim_commits_lease(database, monkeypatch):
    monkeypatch.setenv("LEASE_SECONDS", "120")
    job_id = add_job()
    with Session() as session:
        before = session.scalar(select(func.clock_timestamp()))
    claimed = claim()
    assert claimed is not None
    assert claimed[0] == job_id
    assert isinstance(claimed[1], uuid.UUID)
    assert claimed[2] == {"example": 1}
    with Session() as session:
        job = session.get(Job, job_id)
        after = session.scalar(select(func.clock_timestamp()))
        assert job.status == "running" and job.attempts == 1
        assert job.lease_token == claimed[1]
        assert before + timedelta(seconds=120) <= job.lease_expires_at
        assert job.lease_expires_at <= after + timedelta(seconds=120)
    assert claim() is None


def test_reclaim_changes_token_and_generation(database):
    job_id = add_job()
    first = claim()
    assert first is not None
    expire(job_id)
    second = claim()
    assert second is not None
    assert first[0] == second[0] == job_id
    assert first[1] != second[1]
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.attempts == 2 and job.status == "running"
        assert job.lease_expires_at > session.scalar(select(func.clock_timestamp()))
    assert claim() is None


@pytest.mark.parametrize("status", ["succeeded", "failed"])
def test_fenced_completion_after_reclaim(database, status):
    job_id = add_job()
    first = claim()
    assert first is not None
    expire(job_id)
    second = claim()
    assert second is not None
    with pytest.raises(LeaseLostError):
        complete(job_id, first[1], status)
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == "running" and job.attempts == 2
        assert job.lease_token == second[1]
    complete(job_id, second[1], status)
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == status and job.attempts == 2
        assert job.lease_token is None and job.lease_expires_at is None
    assert claim() is None
    with pytest.raises(LeaseLostError):
        complete(job_id, second[1], status)


@pytest.mark.parametrize("status", ["succeeded", "failed"])
def test_expired_owner_cannot_complete_without_reclaim(database, status):
    job_id = add_job()
    claimed = claim()
    assert claimed is not None
    expire(job_id)
    with pytest.raises(LeaseLostError):
        complete(job_id, claimed[1], status)
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == "running" and job.attempts == 1
        assert job.lease_token == claimed[1]


def test_wrong_token_and_missing_job_cannot_complete(database):
    job_id = add_job()
    claimed = claim()
    assert claimed is not None
    with pytest.raises(LeaseLostError):
        complete(job_id, uuid.uuid4(), "succeeded")
    with pytest.raises(LeaseLostError):
        complete(uuid.uuid4(), claimed[1], "failed")
    complete(job_id, claimed[1], "succeeded")


def test_completion_uses_wall_time_not_transaction_start(database, monkeypatch):
    job_id = add_job()
    claimed = claim()
    assert claimed is not None
    with Session.begin() as stale_transaction:
        transaction_start = stale_transaction.scalar(select(func.now()))
        with Session.begin() as newer_transaction:
            expired_at = newer_transaction.scalar(
                update(Job).where(Job.id == job_id)
                .values(lease_expires_at=func.clock_timestamp())
                .returning(Job.lease_expires_at)
            )
        assert transaction_start < expired_at
        monkeypatch.setattr(
            worker_module, "Session", SimpleNamespace(begin=lambda: nullcontext(stale_transaction))
        )
        with pytest.raises(LeaseLostError):
            complete(job_id, claimed[1], "succeeded")


def test_handler_observes_committed_claim(database, monkeypatch):
    job_id = add_job()
    observed = []

    def inspect_claim(payload):
        with Session() as session:
            job = session.get(Job, job_id)
            assert job.status == "running" and job.attempts == 1
            assert job.lease_token is not None and job.lease_expires_at is not None
            observed.append(payload)

    monkeypatch.setattr(worker_module, "handle", inspect_claim)
    assert run_once()
    assert observed == [{"example": 1}]


@pytest.mark.parametrize("fails", [False, True])
def test_worker_recognizes_lost_lease(database, monkeypatch, caplog, fails):
    job_id = add_job()
    replacement = []

    def lose_lease(payload):
        expire(job_id)
        replacement.append(claim())
        if fails:
            raise ValueError("late synthetic failure")

    monkeypatch.setattr(worker_module, "handle", lose_lease)
    assert run_once()
    assert "Lease lost" in caplog.text
    assert f"discarded {'failed' if fails else 'succeeded'} completion" in caplog.text
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == "running" and job.attempts == 2
        assert job.lease_token == replacement[0][1]
