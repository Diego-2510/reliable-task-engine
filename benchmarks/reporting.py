"""Compact CSV evidence and deterministic summaries; no invalid throughput."""
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

from benchmarks.statistics import distribution

RUN_FIELDS = ["run", "phase", "profile", "workers", "dispatchers", "repetition", "size", "seed",
              "telemetry", "valid", "error", "job_elapsed_seconds", "delivery_elapsed_seconds",
              "job_throughput", "delivery_throughput", "succeeded", "failed", "events", "delivered",
              "dead_letters", "active_leases", "duplicate_ids", "start_db_utc"]
LATENCY_FIELDS = ["run", "metric", "sample_ms"]


def write_csv(path, fields, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def summarize(directory, manifest):
    with (directory / "runs.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    latencies = defaultdict(list)
    with (directory / "latencies.csv").open() as stream:
        for row in csv.DictReader(stream):
            latencies[row["run"], row["metric"]].append(float(row["sample_ms"]))
    grouped = defaultdict(list)
    for row in rows:
        if row["phase"] == "measured" and row["valid"] == "True":
            grouped[row["profile"], int(row["workers"])].append(row)
    groups = []
    missing = []
    for profile in manifest["profiles"]:
        for workers in manifest["workers"]:
            selected = grouped[profile, workers]
            present = {int(r["repetition"]) for r in selected}
            missing.extend({"profile": profile, "workers": workers, "repetition": rep}
                           for rep in range(1, manifest["repetitions"] + 1) if rep not in present)
            if not selected:
                continue
            group = {"profile": profile, "workers": workers, "valid_repetitions": len(selected),
                     "job_throughput_median": statistics.median(
                         float(r["job_throughput"]) for r in selected),
                     "succeeded": sum(int(r["succeeded"]) for r in selected),
                     "failed": sum(int(r["failed"]) for r in selected),
                     "delivered": sum(int(r["delivered"]) for r in selected),
                     "correctness": "all gates passed"}
            if profile == "job_outbox":
                group["delivery_throughput_median"] = statistics.median(
                    float(r["delivery_throughput"]) for r in selected)
            for metric in ("job", "outbox"):
                samples = [sample for r in selected for sample in latencies[r["run"], metric]]
                if samples:
                    group[metric + "_latency"] = distribution(samples)
            groups.append(group)
    for group in groups:
        baseline = next((g for g in groups if g["profile"] == group["profile"]
                         and g["workers"] == 1), None)
        for metric in ("job", "delivery"):
            key = metric + "_throughput_median"
            if key in group and baseline:
                group[metric + "_speedup"] = group[key] / baseline[key]
                group[metric + "_scaling_efficiency"] = group[key] / baseline[key] / group["workers"]
    return {"complete": not missing and not any(r["valid"] != "True" for r in rows),
            "missing_cases": missing, "invalid_runs": [r for r in rows if r["valid"] != "True"],
            "aggregation": "median per-run throughput; pooled measured latency samples per config",
            "groups": groups}


def save(directory, manifest, runs, latencies):
    write_csv(directory / "runs.csv", RUN_FIELDS, runs)
    write_csv(directory / "latencies.csv", LATENCY_FIELDS, latencies)
    summary = summarize(directory, manifest)
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (directory / "report.md").write_text(markdown(manifest, summary))
    return summary


def markdown(manifest, summary):
    def fmt(value):
        return "—" if value is None else f"{value:.2f}"

    lines = ["# Synthetic PostgreSQL benchmark", "",
             (f"Executed: {manifest['executed_at_utc']}. Commit: `{manifest['git_sha']}`; "
             f"dirty: {manifest['git_dirty']}. Smoke: {manifest['smoke']}."), "",
             (f"CPU: {manifest['cpu_model']} ({manifest['logical_cpus']} logical CPUs). "
             f"Python {manifest['python_version']}; PostgreSQL "
             f"{manifest['postgresql']['server_version']}. "
             f"N={manifest['size']}; seed={manifest['seed']}; repetitions={manifest['repetitions']}; "
             f"warm-up={manifest['warmup_size']} separately per configuration. "
             f"Telemetry: {manifest['telemetry']}. One dispatcher for job_outbox."), "",
             ("Storage and full durability/environment settings: [manifest.json](manifest.json). "
             "Durable baseline uses fsync=on, synchronous_commit=on and a dedicated Docker volume."),
             "", "## Measured results", "",
             ("Rates are median jobs/events per second. Latencies are pooled backlog-drain completion "
             "latencies in milliseconds, measured from a DB clock reference before barrier release. "
             "Outbox latency ends at delivery acknowledgement. These are not HTTP E2E latencies."), "",
             ("| Profile | Workers | Reps | Jobs/s | Ack/s | Job p50/p95/p99 ms | "
             "Outbox p50/p95/p99 ms | Job efficiency | Ack efficiency |"),
             "|---|---:|---:|---:|---:|---|---|---:|---:|"]
    for g in summary["groups"]:
        latency = lambda key, g=g: "/".join(fmt(g.get(key, {}).get(p + "_ms"))
                                       for p in ("p50", "p95", "p99"))
        lines.append(f"| {g['profile']} | {g['workers']} | {g['valid_repetitions']} | "
                     f"{fmt(g['job_throughput_median'])} | "
                     f"{fmt(g.get('delivery_throughput_median'))} | {latency('job_latency')} | "
                     f"{latency('outbox_latency')} | {fmt(g.get('job_scaling_efficiency'))} | "
                     f"{fmt(g.get('delivery_scaling_efficiency'))} |")
    lines += ["", ("Efficiency = median rate / (one-worker median rate × worker count). "
              "Nearest-rank percentiles use ceil(q×n), with no interpolation. "
              "p99 is suppressed below 1000 samples, including tiny smoke datasets. "
              "Count, min, max and mean are in summary.json. Even 1000 observations "
              "provide only about ten upper-tail observations; no population-tail guarantee."), "",
              "## Correctness and failures", "",
              (f"Complete: **{summary['complete']}**. Invalid runs: {len(summary['invalid_runs'])}. "
              f"Missing measured cases: {len(summary['missing_cases'])}."), ""]
    for g in summary["groups"]:
        lines.append(f"- {g['profile']}, {g['workers']} workers: succeeded={g['succeeded']}, "
                     f"failed={g['failed']}, delivered={g['delivered']}; {g['correctness']}.")
    if summary["invalid_runs"] or summary["missing_cases"]:
        lines += ["", ("Exact failed and missing cases are recorded in summary.json; "
                  "invalid runs have no throughput or latency results.")]
    lines += ["", "## Interpretation and limitations", "",
              ("Synthetic handlers are effectively no-op work; no HTTP ingress, network downstream "
              "effect or production workload equivalence is measured. The receiver is another "
              "transaction on the same PostgreSQL instance. DB work may dominate scaling; "
              "one dispatcher may constrain outbox throughput. Cache warmth, WAL, I/O and CPU "
              "contention affect results. Bottleneck attribution requires profiling; the rates "
              "alone cannot prove which DB resource dominates."), "",
              ("Startup/imports and data loading are excluded. Throughput uses controller "
              "perf_counter until all expected terminal counts are confirmed; polling adds up to "
              "one polling interval plus query latency. DB timestamps precede their COMMIT and "
              "are wall-clock sensitive. The DB-reference-to-release RPC gap is included in latency "
              "and is not estimated away. Idle processes retain the runtime's 1s poll interval. "
              "Post-drain row validation is outside timing, and gates all published numbers."), "",
              ("No cold-cache reset, independent machine replication, randomization of all factors, "
              "confidence interval or storage saturation diagnosis is claimed. Repetition order "
              "rotates worker counts. Existing development services remain untouched, so shared "
              "host background activity is a possible confounder. Console comparison exports to "
              "the null sink and includes formatting/export work, not terminal rendering; cumulative "
              "telemetry snapshots are never used to calculate rates."), ""]
    return "\n".join(lines)


def regenerate(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    summary = summarize(directory, manifest)
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (directory / "report.md").write_text(markdown(manifest, summary))
