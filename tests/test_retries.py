import multiprocessing as mp
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, update
from test_engine import add_job, expire, make_due

from task_engine import worker
from task_engine.api import app
from task_engine.db import Session
from task_engine.models import Job
from task_engine.worker import (
    LeaseLostError,
    PermanentJobError,
    RetryableJobError,
    backoff_seconds,
    claim,
    complete,
    fail,
    recover_expired,
    run_once,
)


@pytest.mark.parametrize("attempts,seconds", [(1, 2), (2, 4), (3, 8), (5, 32), (6, 60), (1000000, 60)])
def test_backoff(attempts, seconds):
    assert backoff_seconds(attempts) == seconds


def test_api_retry_contract(database):
    client = TestClient(app)
    for invalid in (0, -1, 11, 1.5, True, "3", None):
        assert client.post('/v1/jobs', json={"payload": {}, "max_attempts": invalid}).status_code == 422
    for limit in (1, 3, 10):
        response = client.post('/v1/jobs', json={"payload": {}, "max_attempts": limit})
        assert response.status_code == 201
        data = response.json()
        assert data['max_attempts'] == limit and data['attempts'] == 0
        assert data['available_at'] and data['last_error_code'] is None
        assert data['terminal_reason'] is None
        assert client.get('/v1/jobs/' + data['id']).json() == data
    assert client.post('/v1/jobs', json={"payload": {}}).json()['max_attempts'] == 3


@pytest.mark.parametrize("error,code", [
    (RetryableJobError("SECRET"), "retryable_error"),
    (RuntimeError("SECRET"), "unexpected_error"),
])
def test_retry_and_exhaustion(database, monkeypatch, caplog, error, code):
    job_id = add_job()

    def failing(payload):
        raise error

    monkeypatch.setattr(worker, "handle", failing)
    for attempt in (1, 2, 3):
        with Session() as session:
            before = session.scalar(select(func.clock_timestamp()))
        assert run_once()
        with Session() as session:
            after = session.scalar(select(func.clock_timestamp()))
            job = session.get(Job, job_id)
            assert job.attempts == attempt and job.max_attempts == 3
            assert job.last_error_code == code
            assert job.lease_token is None and job.lease_expires_at is None
            if attempt < 3:
                assert job.status == "queued" and job.terminal_reason is None
                assert before + timedelta(seconds=backoff_seconds(attempt)) <= job.available_at
                assert job.available_at <= after + timedelta(seconds=backoff_seconds(attempt))
            else:
                assert job.status == "failed" and job.terminal_reason == "attempts_exhausted"
        assert claim() is None
        make_due(job_id)
    assert not run_once()
    assert "SECRET" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_success_after_temporary_failures(database, monkeypatch):
    job_id = add_job()
    failures = iter([True, True, False])

    def temporary(payload):
        if next(failures):
            raise RetryableJobError("do not persist")

    monkeypatch.setattr(worker, "handle", temporary)
    for _ in range(3):
        assert run_once()
        make_due(job_id)
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == "succeeded" and job.attempts == 3
        assert job.last_error_code == "retryable_error" and job.terminal_reason is None
    assert claim() is None


def test_permanent_failure_first_attempt(database, monkeypatch, caplog):
    job_id = add_job()

    def permanent(payload):
        raise PermanentJobError("SECRET")

    monkeypatch.setattr(worker, "handle", permanent)
    assert run_once()
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.attempts == 1 and job.status == "failed"
        assert job.last_error_code == job.terminal_reason == "permanent_error"
    assert "SECRET" not in caplog.text
    assert not run_once()


@pytest.mark.parametrize("limit", [1, 3])
def test_crash_recovery_and_stale_transitions(database, limit):
    job_id = add_job()
    with Session.begin() as session:
        session.execute(update(Job).where(Job.id == job_id).values(max_attempts=limit))
    first = claim()
    expire(job_id)
    assert claim() is None  # Also recovers when no job can be claimed.
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.attempts == 1
        assert job.status == ("failed" if limit == 1 else "queued")
        assert job.last_error_code == "lease_expired"
        assert job.terminal_reason == ("attempts_exhausted" if limit == 1 else None)
        assert job.available_at > session.scalar(select(func.clock_timestamp()))
        snapshot = (job.status, job.attempts, job.available_at, job.last_error_code, job.terminal_reason)
        assert job.lease_token is None and job.lease_expires_at is None
    for transition in (
        lambda: complete(job_id, first[1], "succeeded"),
        lambda: complete(job_id, first[1], "failed"),
        lambda: fail(job_id, first[1], RetryableJobError("SECRET")),
    ):
        with pytest.raises(LeaseLostError):
            transition()
    with Session() as session:
        job = session.get(Job, job_id)
        assert snapshot == (job.status, job.attempts, job.available_at, job.last_error_code, job.terminal_reason)
    # Another ready job proceeds while this one backs off.
    other = add_job()
    assert claim()[0] == other
    assert recover_expired() == 0
    make_due(job_id)
    next_claim = claim()
    if limit == 1:
        assert next_claim is None
    else:
        assert next_claim[0] == job_id and next_claim[1] != first[1]
        with pytest.raises(LeaseLostError):
            fail(job_id, first[1], RetryableJobError())


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_base_exceptions_propagate(database, monkeypatch, interrupt):
    job_id = add_job()

    def interrupted(payload):
        raise interrupt()

    monkeypatch.setattr(worker, "handle", interrupted)
    with pytest.raises(interrupt):
        run_once()
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == "running" and job.attempts == 1


def _race(job_id, token, barrier, results, recovery):
    from task_engine.db import engine
    engine.dispose(close=False)
    barrier.wait(timeout=10)
    if recovery:
        results.put(recover_expired())
    else:
        try:
            complete(job_id, token, "succeeded")
        except LeaseLostError:
            results.put("lost")


def test_recovery_completion_race(database):
    job_id = add_job()
    first = claim()
    expire(job_id)
    ctx = mp.get_context("fork")
    barrier, results = ctx.Barrier(2), ctx.Queue()
    processes = [ctx.Process(target=_race, args=(job_id, first[1], barrier, results, recovery))
                 for recovery in (False, True)]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
        outcomes = [results.get(timeout=3) for _ in processes]
        assert "lost" in outcomes
        # SKIP LOCKED may legitimately skip the row held by the rejecting completion.
        recover_expired()
        with Session() as session:
            job = session.get(Job, job_id)
            assert job.status == "queued" and job.attempts == 1
            assert job.last_error_code == "lease_expired"
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            if process.pid is not None:
                process.join(timeout=5)
        results.close()
        results.join_thread()


def test_expiry_on_final_default_attempt(database):
    job_id = add_job()
    for attempt in (1, 2, 3):
        assert claim()[0] == job_id
        expire(job_id)
        assert recover_expired() == 1
        with Session() as session:
            job = session.get(Job, job_id)
            assert job.attempts == attempt
            assert job.status == ("failed" if attempt == 3 else "queued")
            assert job.lease_token is None and job.lease_expires_at is None
        assert claim() is None
        make_due(job_id)
    assert claim() is None
    with Session() as session:
        assert session.get(Job, job_id).terminal_reason == "attempts_exhausted"


@pytest.mark.parametrize("error", [RetryableJobError(), RuntimeError()])
def test_expired_retry_rejected_before_recovery(database, error):
    job_id = add_job()
    first = claim()
    expire(job_id)
    with pytest.raises(LeaseLostError):
        fail(job_id, first[1], error)
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == "running" and job.attempts == 1
        assert job.last_error_code is None


def test_database_failure_propagates(database, monkeypatch):
    add_job()

    def broken_completion(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(worker, "complete", broken_completion)
    with pytest.raises(RuntimeError, match="database unavailable"):
        run_once()


def test_unclaimed_job_rejects_retry(database):
    import uuid

    job_id = add_job()
    with pytest.raises(LeaseLostError):
        fail(job_id, uuid.uuid4(), RetryableJobError())
    with Session() as session:
        job = session.get(Job, job_id)
        assert job.status == "queued" and job.attempts == 0
        assert job.last_error_code is None
