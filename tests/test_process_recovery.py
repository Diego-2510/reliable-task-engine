"""Real SIGKILL of an executing worker, with a fresh-process recovery path."""

import multiprocessing as mp
import os
import signal
from datetime import timedelta

import pytest
from sqlalchemy import func, select, update

from task_engine import worker
from task_engine.db import Session
from task_engine.models import Job, OutboxEvent


def _blocked_worker(ready):
    def blocked_handler(payload):
        ready.send(True)  # run_once has committed the claim before invoking us.
        signal.pause()

    worker.handle = blocked_handler
    worker.run_once()


def _restarted_worker(ready, proceed):
    # The normal poll recovers expiry but cannot bypass the recovery backoff.
    ready.send(worker.run_once())
    assert proceed.wait(15)
    ready.send(worker.run_once())


@pytest.mark.skipif(os.name != "posix", reason="SIGKILL requires POSIX")
def test_sigkill_owned_job_recovers_in_fresh_worker(database):
    with Session.begin() as session:
        job = Job(payload={"emit_outbox_event": True})
        session.add(job)
    ctx = mp.get_context("spawn")
    receive, send = ctx.Pipe(duplex=False)
    killed = ctx.Process(target=_blocked_worker, args=(send,))
    proceed = ctx.Event()
    recovery_receive, recovery_send = ctx.Pipe(duplex=False)
    restarted = ctx.Process(target=_restarted_worker, args=(recovery_send, proceed))
    try:
        killed.start()
        send.close()
        assert receive.poll(15) and receive.recv() is True
        with Session() as session:
            owned = session.get(Job, job.id)
            old_token = owned.lease_token
            assert owned.status == "running" and owned.attempts == 1
            assert old_token is not None
            assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
        killed.kill()
        killed.join(10)
        assert killed.exitcode == -signal.SIGKILL
        with Session.begin() as session:
            owned = session.get(Job, job.id)
            assert owned.status == "running" and owned.lease_token == old_token
            session.execute(update(Job).where(Job.id == job.id).values(
                lease_expires_at=func.clock_timestamp() - timedelta(seconds=1),
            ))
        restarted.start()
        recovery_send.close()
        assert recovery_receive.poll(15) and recovery_receive.recv() is False
        with Session.begin() as session:
            recovered = session.get(Job, job.id)
            assert recovered.status == "queued" and recovered.attempts == 1
            assert recovered.last_error_code == "lease_expired"
            assert recovered.lease_token is None and recovered.lease_expires_at is None
            assert recovered.available_at > recovered.updated_at
            session.execute(update(Job).where(Job.id == job.id).values(
                available_at=func.clock_timestamp() - timedelta(seconds=1),
            ))
        proceed.set()
        assert recovery_receive.poll(15) and recovery_receive.recv() is True
        restarted.join(10)
        assert restarted.exitcode == 0
        with pytest.raises(worker.LeaseLostError):
            worker.complete(job.id, old_token, "succeeded",
                            event_intent=worker.SyntheticEventIntent.JOB_SUCCEEDED)
        with Session() as session:
            completed = session.get(Job, job.id)
            assert completed.status == "succeeded" and completed.attempts == 2
            assert completed.lease_token is None and completed.lease_expires_at is None
            events = session.scalars(select(OutboxEvent)).all()
            assert len(events) == 1 and events[0].job_id == job.id
    finally:
        proceed.set()
        for process in (killed, restarted):
            if process.pid is not None:
                if process.is_alive():
                    process.kill()
                process.join(5)
        for connection in (receive, send, recovery_receive, recovery_send):
            connection.close()
