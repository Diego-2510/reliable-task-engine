"""Nearest-rank percentiles (no interpolation); suppress p99 below 1000 samples."""
import math
import statistics


def percentile(samples, quantile):
    if not samples or not 0 < quantile <= 1:
        raise ValueError("invalid_percentile_input")
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def distribution(samples):
    if not samples or any(not math.isfinite(x) or x < 0 for x in samples):
        raise ValueError("invalid_latency_samples")
    return {"count": len(samples), "p50_ms": percentile(samples, .50),
            "p95_ms": percentile(samples, .95),
            "p99_ms": percentile(samples, .99) if len(samples) >= 1000 else None,
            "min_ms": min(samples), "max_ms": max(samples), "mean_ms": statistics.mean(samples)}


def throughput(count, elapsed):
    if count < 0 or not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError("invalid_throughput_input")
    return count / elapsed
