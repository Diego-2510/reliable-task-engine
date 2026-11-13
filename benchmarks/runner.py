"""CLI: python -m benchmarks {prepare,run,summarize}."""
import argparse
import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import text

from benchmarks.environment import manifest, storage_evidence
from benchmarks.measurement import verify_database, wait_for_completion
from benchmarks.processes import Processes
from benchmarks.reporting import regenerate, save
from benchmarks.safety import BenchmarkError, clean, database_url, exclusive_database, prepare
from benchmarks.statistics import throughput
from benchmarks.workloads import PROFILES, workload


def worker_counts(raw):
    try:
        counts = [int(x) for x in raw.split(",")]
    except ValueError:
        raise argparse.ArgumentTypeError("workers_must_be_unique_positive_integers") from None
    if not counts or len(set(counts)) != len(counts) or any(x < 1 or x > 64 for x in counts):
        raise argparse.ArgumentTypeError("workers_must_be_unique_integers_in_1_64")
    return counts


def positive(raw):
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("positive_integer_required")
    return value


def positive_seconds(raw):
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("positive_finite_seconds_required")
    return value


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("prepare", help="initialize only a fresh, empty authorized database")
    report = commands.add_parser("summarize", help="regenerate summary/report from saved CSV")
    report.add_argument("directory", type=Path)
    run = commands.add_parser("run")
    run.add_argument("--smoke", action="store_true", help="small functional check, no performance claim")
    run.add_argument("--workers", type=worker_counts, default=[1, 2, 4, 8])
    run.add_argument("--profiles", nargs="+", choices=PROFILES, default=list(PROFILES[:2]))
    run.add_argument("--size", type=positive)
    run.add_argument("--warmup", type=positive)
    run.add_argument("--repetitions", type=positive)
    run.add_argument("--seed", type=int, default=20261113)
    run.add_argument("--timeout", type=positive_seconds, default=120)
    run.add_argument("--lease-seconds", type=positive, default=60)
    run.add_argument("--telemetry", choices=("disabled", "console"), default="disabled")
    run.add_argument("--docker-container", help="inspect dedicated volume and verify DB cluster identity")
    run.add_argument("--output", type=Path, help="new directory; never overwrite existing evidence")
    return root


def run_one(connection, url, args, profile, workers, size, repetition, phase, number):
    import time

    row = {"run": f"{number:03}", "phase": phase, "profile": profile, "workers": workers,
           "dispatchers": int(profile == "job_outbox"), "repetition": repetition,
           "size": size, "seed": args.seed, "telemetry": args.telemetry, "valid": False}
    try:
        clean(connection, url)
        expected = workload(profile, size, args.seed)
        # Bulk submission is setup only, never part of reported ingress/throughput timing.
        from sqlalchemy.dialects.postgresql import JSONB
        from sqlalchemy.sql import bindparam
        statement = text("INSERT INTO jobs(id,payload,status,attempts,max_attempts) "
                         "VALUES (:id,:payload,:status,:attempts,:max_attempts)")
        connection.execute(statement.bindparams(bindparam("payload", type_=JSONB)), expected)
        env = {"BENCHMARK_DATABASE": "1", "BENCHMARK_DATABASE_URL": url,
               "LEASE_SECONDS": str(args.lease_seconds),
               "DELIVERY_LEASE_SECONDS": str(args.lease_seconds),
               "TASK_ENGINE_TELEMETRY": args.telemetry, "TASK_ENGINE_LOG_FORMAT": "human"}
        with Processes(workers, row["dispatchers"], env) as processes:
            start_db = connection.scalar(text("SELECT clock_timestamp()"))
            started = time.perf_counter()
            processes.start.set()
            job_time, delivery_time = wait_for_completion(
                connection, processes, size, profile == "job_outbox", started, args.timeout)
            processes.check()
        counts, job_samples, event_samples = verify_database(connection, expected, start_db)
        row.update({k: v for k, v in counts.items() if k != "jobs"})
        row.update(valid=True, job_elapsed_seconds=job_time, delivery_elapsed_seconds=delivery_time,
                   job_throughput=throughput(size, job_time),
                   delivery_throughput=throughput(size, delivery_time) if delivery_time else None,
                   start_db_utc=start_db.isoformat())
        samples = ([{"run": row["run"], "metric": metric, "sample_ms": round(value, 3)}
                    for metric, values in (("job", job_samples), ("outbox", event_samples))
                    for value in sorted(values)] if phase == "measured" else [])
        return row, samples
    except Exception as error:  # noqa: BLE001 - sanitized evidence/CLI boundary
        row["error"] = str(error) if isinstance(error, BenchmarkError) else "benchmark_execution_failed"
        return row, []


def run(args, connection, url):
    args.size = args.size or (20 if args.smoke else 1000)
    args.warmup = args.warmup or (5 if args.smoke else 100)
    args.repetitions = args.repetitions or (1 if args.smoke else 3)
    if not args.smoke and args.size < 1000:
        raise BenchmarkError("measured_dataset_requires_at_least_1000_jobs")
    if len(set(args.profiles)) != len(args.profiles):
        raise BenchmarkError("duplicate_profiles")
    storage = storage_evidence(args.docker_container)
    if not args.smoke and not storage["verified_persistent_volume"]:
        raise BenchmarkError("official_measurement_requires_verified_persistent_storage")
    evidence = manifest(connection, args, storage)
    directory = args.output or Path("benchmarks/results") / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "manifest.json").write_text(json.dumps(evidence, indent=2) + "\n")
    rows, latencies, warmed = [], [], set()
    save(directory, evidence, rows, latencies)
    try:
        for repetition in range(1, args.repetitions + 1):
            # Rotate first worker count to reduce a fixed time-order bias.
            offset = (repetition - 1) % len(args.workers)
            order = args.workers[offset:] + args.workers[:offset]
            for profile in args.profiles:
                for workers in order:
                    phases = [("measured", args.size)]
                    if (profile, workers) not in warmed:
                        phases.insert(0, ("warmup", args.warmup))
                        warmed.add((profile, workers))
                    for phase, size in phases:
                        row, samples = run_one(connection, url, args, profile, workers, size,
                                               repetition, phase, len(rows) + 1)
                        rows.append(row)
                        latencies.extend(samples)
                        save(directory, evidence, rows, latencies)
                        print(json.dumps({k: row.get(k) for k in (
                            "run", "phase", "profile", "workers", "valid", "error",
                            "job_throughput", "delivery_throughput")}), flush=True)
                        if not row["valid"]:
                            return 1
    finally:
        # Owned processes have been joined before cleanup, including on timeouts/interruptions.
        try:
            clean(connection, url)
        except Exception:  # noqa: BLE001 - cleanup failures invalidate published evidence
            if rows:
                failed = rows[-1]
                failed["valid"] = False
                failed["error"] = "final_cleanup_failed"
                for key in ("job_throughput", "delivery_throughput", "job_elapsed_seconds",
                            "delivery_elapsed_seconds"):
                    failed.pop(key, None)
                latencies = [r for r in latencies if r["run"] != failed["run"]]
                save(directory, evidence, rows, latencies)
            raise BenchmarkError("final_cleanup_failed") from None
    print(f"results: {directory}", flush=True)
    return 0


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "summarize":
            regenerate(args.directory)
            return 0
        url = database_url(os.environ)  # Never use DATABASE_URL as a fallback.
        with exclusive_database(url) as connection:
            if args.command == "prepare":
                prepare(connection, url)
                print("benchmark_database_prepared")
                return 0
            from benchmarks.safety import verify_owned
            verify_owned(connection, url)
            return run(args, connection, url)
    except KeyboardInterrupt:
        print("benchmark_interrupted")
        return 130
    except Exception as error:  # noqa: BLE001 - sanitized evidence/CLI boundary
        print(str(error) if isinstance(error, BenchmarkError) else "benchmark_command_failed")
        return 1
