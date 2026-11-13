"""Fixed-size synthetic payloads; UUIDs deterministic for each dataset."""
import random
import uuid

PROFILES = ("job_only", "job_outbox", "permanent_failure_5pct")
NAMESPACE = uuid.UUID("be0c0000-0000-4000-8000-000000000001")


def workload(profile: str, size: int, seed: int) -> list[dict]:
    if profile not in PROFILES or size < 1:
        raise ValueError("invalid_workload")
    rng = random.Random(seed)
    failures = set(rng.sample(range(size), size // 20)) if profile == PROFILES[2] else set()
    return [{
        "id": uuid.uuid5(NAMESPACE, f"{seed}:{index}"),
        "payload": {"synthetic": "benchmark", "index": index,
                    "data": f"{rng.getrandbits(256):064x}",
                    "emit_outbox_event": profile == "job_outbox", "fail": index in failures},
        "status": "queued", "attempts": 0, "max_attempts": 3,
    } for index in range(size)]
