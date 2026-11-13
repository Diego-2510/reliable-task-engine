"""Independent unit tests; no application imports, no implicit database access."""
import argparse
import csv
import json
import multiprocessing as mp
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from benchmarks.measurement import verify_rows, wait_for_completion
from benchmarks.processes import Processes
from benchmarks.reporting import regenerate, save
from benchmarks.runner import parser, run_one, worker_counts
from benchmarks.safety import BenchmarkError, database_url, prepare, verify_identity, verify_owned
from benchmarks.statistics import distribution, percentile, throughput
from benchmarks.workloads import workload

SAFE = "postgresql+psycopg://synthetic@127.0.0.1/rte_benchmark_test"


@pytest.mark.parametrize("env", [
    {}, {"BENCHMARK_DATABASE": "1"}, {"BENCHMARK_DATABASE_URL": SAFE},
    {"BENCHMARK_DATABASE": "true", "BENCHMARK_DATABASE_URL": SAFE},
    {"BENCHMARK_DATABASE": "1", "DATABASE_URL": SAFE},
])
def test_explicit_authorization_required(env):
    with pytest.raises(BenchmarkError, match="authorization"):
        database_url(env)


@pytest.mark.parametrize("url", [
    "not a url", "sqlite:///rte_test", "postgresql+psycopg:///rte_test",
    "postgresql+psycopg://x@localhost/task", "postgresql+psycopg://x@localhost/production",
    SAFE + "?dbname=production", SAFE + "?options=-csearch_path=other",
    "postgresql://x@localhost/rte_test",
])
def test_unsafe_selection_rejected(url):
    with pytest.raises(BenchmarkError, match="unsafe"):
        database_url({"BENCHMARK_DATABASE": "1", "BENCHMARK_DATABASE_URL": url})


def test_database_identity_mismatch():
    connection = Mock()
    connection.scalar.return_value = "production"
    with pytest.raises(BenchmarkError, match="identity_mismatch"):
        verify_identity(connection, SAFE)
    connection.execute.assert_not_called()


def test_identity_checked_before_ownership():
    connection = Mock()
    connection.scalar.side_effect = ["production"]
    with pytest.raises(BenchmarkError, match="identity_mismatch"):
        verify_owned(connection, SAFE)
    assert connection.scalar.call_count == 1


def test_nonempty_unknown_database_never_migrated(monkeypatch):
    connection = Mock()
    connection.scalar.side_effect = ["rte_benchmark_test", "rte_benchmark_test", None]
    connection.execute.return_value.all.return_value = [("public", "existing_development_data")]
    migrate = Mock()
    monkeypatch.setattr("benchmarks.safety.subprocess.run", migrate)
    with pytest.raises(BenchmarkError, match="not_benchmark_owned"):
        prepare(connection, SAFE)
    migrate.assert_not_called()
    assert connection.execute.call_count == 1


def test_statistics():
    assert percentile([4, 1, 3, 2], .5) == 2
    assert percentile(range(1, 101), .95) == 95
    assert percentile(range(1, 1001), .99) == 990
    assert distribution([1, 2, 3, 4]) == {
        "count": 4, "p50_ms": 2, "p95_ms": 4, "p99_ms": None,
        "min_ms": 1, "max_ms": 4, "mean_ms": 2.5}
    assert throughput(1000, 2.5) == 400
    assert throughput(0, 2.5) == 0
    with pytest.raises(ValueError):
        throughput(1, 0)
    with pytest.raises(ValueError):
        distribution([-1])


@pytest.mark.parametrize("raw", ["", "1,1", "0,2", "-1", "abc", "1,2,", "65"])
def test_invalid_worker_counts(raw):
    with pytest.raises(argparse.ArgumentTypeError):
        worker_counts(raw)


def test_worker_counts_and_defaults():
    assert worker_counts("1,2,4,8") == [1, 2, 4, 8]
    assert parser().parse_args(["run"]).workers == [1, 2, 4, 8]


def test_deterministic_workload():
    first = workload("job_outbox", 1000, 42)
    assert first == workload("job_outbox", 1000, 42)
    assert first != workload("job_outbox", 1000, 43)
    assert len({r["id"] for r in first}) == 1000
    assert all(r["payload"]["emit_outbox_event"] for r in first)
    assert not any(r["payload"]["emit_outbox_event"] for r in workload("job_only", 1000, 42))
    failed = workload("permanent_failure_5pct", 1000, 42)
    assert sum(r["payload"]["fail"] for r in failed) == 50


def test_timeout_invalidates():
    connection = Mock()
    connection.execute.return_value.one.return_value = (0, 0)
    with pytest.raises(BenchmarkError, match="workload_timeout"):
        wait_for_completion(connection, Mock(), 10, False, time.perf_counter() - 2, .01)


def valid_rows():
    expected = workload("job_only", 1, 42)
    start = datetime.now(UTC)
    jobs = [dict(expected[0], status="succeeded", attempts=1, lease_token=None,
                 lease_expires_at=None, terminal_reason=None, last_error_code=None,
                 idempotency_key=None, updated_at=start + timedelta(seconds=1))]
    return jobs, [], [], expected, start


@pytest.mark.parametrize("key,value", [
    ("status", "running"), ("attempts", 2), ("lease_token", "stale"),
    ("last_error_code", "lease_expired"), ("payload", {}), ("terminal_reason", "permanent_error"),
])
def test_invariants_reject_corruption(key, value):
    jobs, events, effects, expected, start = valid_rows()
    jobs[0][key] = value
    with pytest.raises(BenchmarkError):
        verify_rows(jobs, events, effects, expected, start)


def test_invariants_reject_missing_and_extra_rows():
    jobs, events, effects, expected, start = valid_rows()
    for bad in ([], jobs * 2):
        with pytest.raises(BenchmarkError, match="job_count"):
            verify_rows(bad, events, effects, expected, start)
    counts, samples, event_samples = verify_rows(jobs, events, effects, expected, start)
    assert counts["jobs"] == 1 and samples == [1000] and event_samples == []


@pytest.mark.parametrize("failure", ["workload_timeout", "job_attempt_invariant"])
def test_failed_run_never_reports_rates(monkeypatch, failure):
    monkeypatch.setattr("benchmarks.runner.clean", Mock())
    monkeypatch.setattr("benchmarks.runner.workload", Mock(return_value=[]))
    monkeypatch.setattr("benchmarks.runner.Processes", Mock(side_effect=BenchmarkError(failure)))
    args = SimpleNamespace(seed=42, lease_seconds=60, telemetry="disabled", timeout=1)
    row, samples = run_one(Mock(), SAFE, args, "job_only", 1, 10, 1, "measured", 1)
    assert not row["valid"] and row["error"] == failure and not samples
    assert "job_throughput" not in row and "delivery_throughput" not in row


def sleeper(*_):
    time.sleep(60)


def ready_sleeper(component, env, ready, start, stop):
    ready.send(True)
    time.sleep(60)


@pytest.mark.parametrize("failure", ["start", "body", "readiness"])
def test_all_owned_processes_cleaned_after_failure(monkeypatch, failure):
    context = mp.get_context("spawn")
    original_process = context.Process
    started = []
    monkeypatch.setattr("benchmarks.processes.child", sleeper)

    def make_process(**kwargs):
        process = original_process(**kwargs)
        if failure == "start" and started:
            process.start = Mock(side_effect=OSError("synthetic startup failure"))
        started.append(process)
        return process

    monkeypatch.setattr(context, "Process", make_process)
    monkeypatch.setattr("benchmarks.processes.mp.get_context", lambda _: context)
    group = Processes(2, 0, {}, startup_timeout=10 if failure == "body" else .1)
    if failure == "body":
        monkeypatch.setattr("benchmarks.processes.child", ready_sleeper)
    with (pytest.raises((BenchmarkError, OSError, RuntimeError), match="timeout|startup|injected"),
          group):
        raise RuntimeError("injected body failure")
    assert started
    assert all(not p.is_alive() for p in started)
    assert all(p.exitcode is not None for p in group.processes)


def report_manifest():
    return {"profiles": ["job_only"], "workers": [1], "repetitions": 1,
            "executed_at_utc": "2026-01-01T00:00:00+00:00", "git_sha": "synthetic",
            "git_dirty": True, "smoke": True, "cpu_model": "synthetic", "logical_cpus": 1,
            "python_version": "3.12", "postgresql": {"server_version": "16"}, "size": 2,
            "seed": 42, "warmup_size": 1, "telemetry": "disabled"}


def test_results_reproducible_and_missing_cases_explicit(tmp_path):
    manifest = report_manifest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    rows = [{"run": "001", "phase": "measured", "profile": "job_only", "workers": 1,
             "repetition": 1, "valid": True, "job_throughput": 100, "succeeded": 2,
             "failed": 0, "delivered": 0}]
    samples = [{"run": "001", "metric": "job", "sample_ms": value} for value in (1, 2)]
    summary = save(tmp_path, manifest, rows, samples)
    assert summary["complete"]
    assert summary["groups"][0]["job_latency"]["count"] == 2
    original = (tmp_path / "summary.json").read_text()
    regenerate(tmp_path)
    assert (tmp_path / "summary.json").read_text() == original
    manifest["workers"] += [2, 4, 8]
    summary = save(tmp_path, manifest, rows, samples)
    assert not summary["complete"] and len(summary["missing_cases"]) == 3


def test_smoke_on_explicit_disposable_postgresql(tmp_path):
    import subprocess
    import sys

    if "BENCHMARK_DATABASE_URL" not in os.environ:
        pytest.skip("set explicit BENCHMARK_DATABASE=1 and BENCHMARK_DATABASE_URL for real DB smoke")
    database_url(os.environ)
    for command in (["prepare"], ["run", "--smoke", "--output", str(tmp_path / "smoke")]):
        result = subprocess.run([sys.executable, "-m", "benchmarks", *command],
                                capture_output=True, text=True, timeout=120, check=False)
        assert result.returncode == 0, result.stdout
    summary = json.loads((tmp_path / "smoke/summary.json").read_text())
    assert summary["complete"] and len(summary["groups"]) == 8
    assert all(g["job_latency"]["p99_ms"] is None for g in summary["groups"])
    with (tmp_path / "smoke/runs.csv").open() as stream:
        assert all(r["valid"] == "True" for r in csv.DictReader(stream))
    # No database credentials, URLs or private hostname fields in persisted artifacts.
    for artifact in Path(tmp_path / "smoke").iterdir():
        assert "postgresql+psycopg://" not in artifact.read_text()


def valid_outbox_rows():
    import uuid

    jobs, _, _, _, start = valid_rows()
    expected = workload("job_outbox", 1, 42)
    jobs[0]["payload"] = expected[0]["payload"]
    event_id = uuid.UUID("00000000-0000-4000-8000-000000000001")
    event = {"id": event_id, "job_id": jobs[0]["id"], "event_type": "job.succeeded.v1",
             "payload": {"job_id": str(jobs[0]["id"]), "event_version": 1},
             "created_at": start + timedelta(seconds=1), "delivery_state": "delivered",
             "delivery_attempts": 1, "max_delivery_attempts": 5, "delivery_token": None,
             "delivery_lease_expires_at": None, "dead_lettered_at": None, "dead_letter_reason": None,
             "delivery_last_error_code": None, "last_replayed_at": None, "replay_count": 0,
             "delivered_at": start + timedelta(seconds=2)}
    effect = {"event_id": event_id, "job_id": jobs[0]["id"], "event_type": "job.succeeded.v1",
              "processed_at": start + timedelta(seconds=1.5)}
    return jobs, [event], [effect], expected, start


@pytest.mark.parametrize("corruption", ["missing_event", "missing_effect", "duplicate_event",
                                        "duplicate_effect", "dead_letter", "lease", "attempts"])
def test_outbox_invariants(corruption):
    jobs, events, effects, expected, start = valid_outbox_rows()
    counts, _, latencies = verify_rows(jobs, events, effects, expected, start)
    assert counts["delivered"] == 1 and latencies == [2000]
    if corruption == "missing_event":
        events.clear()
    elif corruption == "missing_effect":
        effects.clear()
    elif corruption == "duplicate_event":
        events *= 2
    elif corruption == "duplicate_effect":
        effects *= 2
    elif corruption == "dead_letter":
        events[0]["delivery_state"] = "dead_letter"
    elif corruption == "lease":
        events[0]["delivery_token"] = "stale"
    else:
        events[0]["delivery_attempts"] = 2
    with pytest.raises(BenchmarkError):
        verify_rows(jobs, events, effects, expected, start)


def test_missing_or_volatile_storage_rejected(monkeypatch):
    from benchmarks.environment import storage_evidence

    assert not storage_evidence(None)["verified_persistent_volume"]
    monkeypatch.setattr("benchmarks.environment.command", lambda _: json.dumps([
        {"Mounts": [{"Destination": "/var/lib/postgresql/data", "Type": "tmpfs"}]}]))
    with pytest.raises(BenchmarkError, match="persistent_docker_volume"):
        storage_evidence("synthetic")


def test_final_cleanup_failure_invalidates_evidence(monkeypatch, tmp_path):
    from benchmarks.runner import run

    args = parser().parse_args(["run", "--workers", "1", "--profiles", "job_only",
                                "--output", str(tmp_path / "result")])
    manifest = report_manifest()
    monkeypatch.setattr("benchmarks.runner.manifest", lambda *_: manifest)
    monkeypatch.setattr("benchmarks.runner.storage_evidence",
                        lambda _: {"verified_persistent_volume": True})
    monkeypatch.setattr("benchmarks.runner.clean", Mock(side_effect=BenchmarkError("synthetic")))
    rows = [{"run": f"{i:03}", "phase": phase, "profile": "job_only", "workers": 1, "repetition": 1,
                 "valid": True, "job_throughput": 100, "succeeded": 1000, "failed": 0, "delivered": 0}
            for i, phase in ((1, "warmup"), (2, "measured"))]
    monkeypatch.setattr("benchmarks.runner.run_one", Mock(side_effect=[(r, []) for r in rows]))
    args.repetitions = 1
    with pytest.raises(BenchmarkError, match="final_cleanup_failed"):
        run(args, Mock(), SAFE)
    summary = json.loads((tmp_path / "result/summary.json").read_text())
    assert not summary["complete"] and summary["invalid_runs"][0]["error"] == "final_cleanup_failed"
    assert not summary["invalid_runs"][0]["job_throughput"]
