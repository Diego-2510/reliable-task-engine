"""Authoritative DB invariants; controller monotonic confirmation intervals."""
import time

from sqlalchemy import text

from benchmarks.safety import BenchmarkError


def wait_for_completion(connection, processes, size, outbox, started, timeout, poll=.02):
    job_elapsed = None
    while True:
        processes.check()
        terminal, delivered = connection.execute(text(
            "SELECT (SELECT count(*) FROM jobs WHERE status IN ('succeeded','failed')), "
            "(SELECT count(*) FROM outbox_events WHERE delivery_state='delivered')"
        )).one()
        elapsed = time.perf_counter() - started
        if elapsed > timeout:
            raise BenchmarkError("workload_timeout")
        if terminal > size or delivered > (size if outbox else 0):
            raise BenchmarkError("unexpected_row_count")
        if terminal == size and job_elapsed is None:
            job_elapsed = elapsed
        if job_elapsed is not None and (not outbox or delivered == size):
            return job_elapsed, elapsed if outbox else None
        time.sleep(poll)


def verify_rows(jobs, events, effects, expected, start_db):
    def require(condition, code):
        if not condition:
            raise BenchmarkError(code)

    expected_by_id = {row["id"]: row for row in expected}
    require(len(jobs) == len(expected) == len(expected_by_id), "job_count_invariant")
    require({r["id"] for r in jobs} == set(expected_by_id), "job_identity_invariant")
    expected_events = set()
    job_latencies = []
    failed = 0
    for row in jobs:
        payload = expected_by_id[row["id"]]["payload"]
        failure = payload["fail"]
        require(row["payload"] == payload, "payload_contamination")
        require(row["status"] == ("failed" if failure else "succeeded"), "job_status_invariant")
        require(row["attempts"] == 1 and row["max_attempts"] == 3, "job_attempt_invariant")
        require(row["lease_token"] is None and row["lease_expires_at"] is None,
                "active_job_lease")
        require(row["terminal_reason"] == ("permanent_error" if failure else None)
                and row["last_error_code"] == ("permanent_error" if failure else None),
                "job_error_invariant")
        require(row["idempotency_key"] is None, "unexpected_submission_key")
        latency = (row["updated_at"] - start_db).total_seconds() * 1000
        require(latency >= 0, "database_clock_or_timestamp_invariant")
        job_latencies.append(latency)
        failed += failure
        if payload["emit_outbox_event"] and not failure:
            expected_events.add(row["id"])
    require(len(events) == len(expected_events), "event_count_invariant")
    require({r["job_id"] for r in events} == expected_events, "event_job_identity_invariant")
    event_ids = {r["id"] for r in events}
    require(len(event_ids) == len(events), "duplicate_event_ids")
    require(len(effects) == len(events) and {r["event_id"] for r in effects} == event_ids,
            "receiver_count_or_identity_invariant")
    effects_by_id = {r["event_id"]: r for r in effects}
    event_latencies = []
    for row in events:
        require(row["delivery_state"] == "delivered", "delivery_state_invariant")
        require(row["delivery_attempts"] == 1 and row["max_delivery_attempts"] == 5,
                "delivery_attempt_invariant")
        require(all(row[key] is None for key in (
            "delivery_token", "delivery_lease_expires_at", "dead_lettered_at",
            "dead_letter_reason", "delivery_last_error_code", "last_replayed_at"))
            and row["replay_count"] == 0, "delivery_lease_or_error_invariant")
        require(row["event_type"] == "job.succeeded.v1"
                and row["payload"] == {"job_id": str(row["job_id"]), "event_version": 1},
                "event_payload_invariant")
        effect = effects_by_id[row["id"]]
        require(effect["job_id"] == row["job_id"]
                and effect["event_type"] == row["event_type"], "receiver_effect_invariant")
        require(row["delivered_at"] is not None
                and start_db <= row["created_at"] <= effect["processed_at"] <= row["delivered_at"],
                "delivery_timestamp_invariant")
        event_latencies.append((row["delivered_at"] - start_db).total_seconds() * 1000)
    return {"jobs": len(jobs), "succeeded": len(jobs) - failed, "failed": failed,
            "events": len(events), "delivered": len(effects), "dead_letters": 0,
            "active_leases": 0, "duplicate_ids": 0}, job_latencies, event_latencies


def verify_database(connection, expected, start_db):
    return verify_rows(*[
        connection.execute(text(f"SELECT * FROM {table}")).mappings().all()
        for table in ("jobs", "outbox_events", "synthetic_receiver_events")
    ], expected, start_db)
