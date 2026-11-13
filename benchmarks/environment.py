"""Allowlisted environment evidence. Never serialize URLs, hostnames or ambient env."""
import hashlib
import json
import os
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import text

from benchmarks.safety import BenchmarkError


def command(args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=15, check=True)
    return result.stdout.strip()


def storage_evidence(container):
    if not container:
        return {"verified_persistent_volume": False, "description": "unverified_smoke_only"}
    data = json.loads(command(["docker", "inspect", container]))[0]
    mounts = [m for m in data["Mounts"] if m["Destination"] == "/var/lib/postgresql/data"]
    if len(mounts) != 1 or mounts[0]["Type"] != "volume":
        raise BenchmarkError("official_measurement_requires_persistent_docker_volume")
    mount = mounts[0]
    volume = json.loads(command(["docker", "volume", "inspect", mount["Name"]]))[0]
    if (volume.get("Labels") or {}).get("reliable-task-engine.benchmark") != "1":
        raise BenchmarkError("volume_not_dedicated_to_benchmarks")
    if volume["Driver"] != "local" or volume.get("Options"):
        raise BenchmarkError("unsupported_volume_storage_configuration")
    # Record filesystem type, never host mount paths or private device/host names.
    fs = command(["docker", "exec", container, "df", "--output=fstype",
                  "/var/lib/postgresql/data"]).splitlines()[-1].strip()
    if fs in {"tmpfs", "ramfs"}:
        raise BenchmarkError("volatile_storage_not_official_baseline")
    return {"verified_persistent_volume": True, "volume": mount["Name"],
            "driver": volume["Driver"], "filesystem": fs, "mount": mount["Destination"],
            "image_id": data["Image"], "postgres_image": data["Config"]["Image"],
            "container_id": data["Id"]}


def manifest(connection, args, storage):
    settings = dict(connection.execute(text(
        "SELECT name, setting FROM pg_settings WHERE name IN "
        "('server_version','fsync','synchronous_commit','full_page_writes','shared_buffers',"
        "'wal_sync_method','max_connections','checkpoint_timeout','max_wal_size',"
        "'track_io_timing','block_size')"
    )).all())
    if not args.smoke and (settings["fsync"] != "on" or settings["synchronous_commit"] != "on"
                           or settings["full_page_writes"] != "on"):
        raise BenchmarkError("official_measurement_requires_durable_postgres_settings")
    if storage["verified_persistent_volume"]:
        # Tie inspected volume to the actual connected server, not merely a named container.
        actual = str(connection.scalar(text("SELECT system_identifier FROM pg_control_system()")))
        inspected = command(["docker", "exec", storage["container_id"], "psql", "-U", "benchmark",
                             "-d", "rte_benchmark_test", "-Atc",
                             "SELECT system_identifier FROM pg_control_system()"])
        if actual != inspected:
            raise BenchmarkError("docker_storage_database_identity_mismatch")
    memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), "unknown")
    paths = sorted([*Path("benchmarks").glob("*.py"), *Path("src").rglob("*.py"),
                    *Path("migrations").rglob("*.py"), Path("benchmarks/compose.yaml"),
                    Path("pyproject.toml"), Path("uv.lock")])
    fingerprint = hashlib.sha256()
    for path in paths:
        fingerprint.update(str(path).encode() + b"\0" + path.read_bytes() + b"\0")
    return {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "git_sha": command(["git", "rev-parse", "HEAD"]),
        "git_dirty": bool(command(["git", "status", "--porcelain"])),
        "source_sha256": fingerprint.hexdigest(),
        "source_fingerprint_paths": [str(p) for p in paths],
        "os": {"system": platform.system(), "kernel": platform.release(),
               "architecture": platform.machine(), "distribution": platform.freedesktop_os_release()
               .get("PRETTY_NAME", "unknown")},
        "cpu_model": cpu, "logical_cpus": os.cpu_count(),
        "memory_total_kib": int(memory["MemTotal"].split()[0]),
        "memory_available_kib": int(memory["MemAvailable"].split()[0]),
        "python_version": platform.python_version(), "uv_version": command(["uv", "--version"]),
        "docker_version": command(["docker", "version", "--format", "{{.Server.Version}}"]),
        "compose_version": command(["docker", "compose", "version", "--short"]),
        "postgresql": settings, "storage": storage,
        "workers": args.workers, "dispatchers": {p: int(p == "job_outbox") for p in args.profiles},
        "size": args.size, "seed": args.seed, "lease_seconds": args.lease_seconds,
        "delivery_lease_seconds": args.lease_seconds, "telemetry": args.telemetry,
        "console_sink": "os.devnull; exporter formatting retained; application logging disabled",
        "warmup_size": args.warmup, "warmup_method": "separate dataset/processes once per config",
        "repetitions": args.repetitions, "timeout_seconds": args.timeout,
        "startup_timeout_seconds": 30, "shutdown_grace_seconds": 5,
        "controller_statement_timeout_seconds": 5, "controller_lock_timeout_seconds": 5,
        "poll_seconds": .02, "idle_process_poll_seconds": 1,
        "profiles": args.profiles, "smoke": args.smoke,
        "percentile_algorithm": "nearest-rank ceil(q*n), no interpolation; p99 null below n=1000",
        "latency_reference": "PostgreSQL clock_timestamp immediately before common barrier release",
        "throughput_reference": "controller perf_counter until expected terminal counts confirmed",
        "start_bridge": "DB start fetched, then monotonic start, then event.set; RPC gap not corrected",
        "competing_workload_policy": "no concurrent benchmark; existing development services untouched",
    }
