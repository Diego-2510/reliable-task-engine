# Phase 4B: v0.1.0 candidate review

Review date: 2026-11-15, Europe/Berlin. Working-tree candidate based on clean
`main` at `91ef58585af5f62446fa5349e7b034549a5cdb0f`; initial untracked
`LICENSE.candidate` was the only local addition and was adopted as `LICENSE`.
No commit, push, tag, release, PR, remote write or Git identity change occurred.

## Changes and scope

Exact candidate inventory:

- `LICENSE` — new root MIT license, adopting the previously untracked candidate.
- `pyproject.toml` — SPDX MIT and explicit license file; version remains 0.1.0.
- `Dockerfile` — copy LICENSE before locked installation so packaging has its input.
- `README.md` — focused overview, one CI badge, measured table, current status/links.
- `docs/case-study.md` — technical narrative, guarantees, failure boundaries and evidence.
- `docs/releases/v0.1.0.md` — proposed publication text and absolute versioned links.
- `docs/release-readiness.md` — current checklist, operational history and publication gate.
- `docs/phase-4b-validation.md` — this review and current verification record.

The candidate filename no longer exists. Historical references to that name in
Phase 4A documents describe its then-proposed status; they are not adoption advice.
Runtime source, migrations, tests, benchmark implementation/safety controls,
saved measurements, dependencies, uv.lock, Compose and CI configuration are
unchanged. No new feature, framework, service or architecture is introduced.
The local Docker build now includes the project license; this does not relicense
any dependency.

## License and distribution review

The complete candidate text was compared with the standard
[MIT text](https://opensource.org/license/mit), retaining the permission grant,
notice condition, warranty disclaimer and liability exclusion. Exact attribution:
`Copyright (c) 2026 Diego Ringleb`. No licensing ambiguity was found.

Hatchling supports the SPDX expression in the existing toolchain. `uv build`
built an sdist and a wheel from that sdist. Both contain the complete root license
and report `Name: reliable-task-engine`, `Version: 0.1.0`, `Metadata-Version: 2.5`,
`License-Expression: MIT`, `License-File: LICENSE`. The sdist excludes local
AGENTS.md, .env, .serena and the old candidate file. Dependencies retain their
own licenses. No dependency or lockfile edit was necessary.

## Current verification

All required local checks passed against this candidate. Temporary command logs
and scripts are under `/tmp/rte-phase4b-evidence`, outside publication inputs.

| Check | Executed result |
| --- | --- |
| `uv sync --locked` | Passed; no uv.lock change |
| `uv build` | Passed: sdist plus wheel built from sdist |
| Distribution metadata and license contents | Both version 0.1.0, SPDX MIT, complete LICENSE |
| `uv run ruff check .` | All checks passed |
| `uv run mypy src` | No issues in nine source files |
| `docker compose config -q` | Passed |
| `docker compose -f benchmarks/compose.yaml config -q` | Passed |
| Full authorized PostgreSQL `uv run pytest -ra` | **350 passed, 26 warnings, 83.27 s**, zero skipped |
| Clean candidate locked install / Docker build | Passed with a new empty uv cache |
| Fresh smoke database migration | Empty schema upgraded 0001→0007 |
| Real HTTP smoke | Create 201, same-ID replay 200, conflict 409, invalid key 422, GET succeeded |
| Durable smoke SQL state | One succeeded job, one delivered event, one receiver row; both attempts=1 |
| Read-only diagnostics | Succeeded/delivered=1; due work, expired leases and dead letters=0 |
| Saved benchmark report regeneration | Five sets identical, originals untouched |
| Documentation links | 67 local targets/anchors validated, including nine future versioned URLs |
| `git diff --check` | Passed |
| Tracked/untracked inventory and sensitive-data review | Eight candidate files; no introduced secrets/private material |

The full suite used a uniquely named newly created PostgreSQL 16.15 container with
three separately created disposable `_test` databases: regression, benchmark
smoke and runtime smoke. Actual regression database identity and empty public
schema were checked before migration. TEST_DATABASE=1 and BENCHMARK_DATABASE=1,
explicit URLs, identity/ownership checks and all existing benchmark controls
remained enabled. Development databases/containers/volumes were not accessed.

The clean source was a `git archive HEAD` export with exactly the eight candidate
files overlaid. It excluded ignored local instructions, configuration and venv.
It is a clean-source candidate smoke, not a claim that an uncommitted release
already has a clean Git commit. Final result/checklist prose was filled in after
execution; runtime and build inputs were unchanged. Empty-cache locked sync and Docker image build
passed; fresh API, worker and dispatcher containers exercised real local HTTP
and verified durable SQL results. Only owned disposable resources were removed
after checking their labels. tmpfs storage was used for ephemeral checks; no
new persistence, restart or performance claim is inferred from that smoke.

Warnings were the existing Starlette/httpx TestClient deprecation (one), Alembic
path_separator deprecations (25), and Docker legacy-builder deprecation during
image build. None failed a check; no dependency was changed to suppress them.

Sensitive-data review covered all 97 tracked/untracked candidate paths, full
candidate diffs/text and distribution contents. Pattern matches were public
synthetic/local credentials, UUID ownership-token code and one existing historical
repository path; no high-confidence private key/access-token match or newly
introduced private reference was found. Saved manifests retain their allowlisted
schemas. This is a source/artifact review, not a vulnerability advisory scan.
Versioned release-note URLs were mapped to included repository targets; external
resolution is a future post-tag check, since no tag exists. The main CI badge
links to the actual ci.yml workflow. No required validation was skipped: batched
checks and copied report regeneration avoided repeated reads and a matrix rerun.

## Benchmark provenance

All five saved sets were copied outside the repository before
`uv run python -m benchmarks summarize COPIED_DIRECTORY`. Regenerated report.md
and summary.json matched byte-for-byte; original artifacts were untouched.
Independent stdlib arithmetic checked measured rates, medians, sample counts and
nearest-rank percentiles/means, including 46.90 and 316.81 jobs/s, 6.76× speedup
and 84.4% scaling efficiency. One-dispatcher acknowledgement medians remain
39.94–42.20/s.

| Saved set | Measured runs | Latency samples | Source equivalence with parent HEAD |
| --- | ---: | ---: | --- |
| phase3b-baseline | 24 | 36,000 | Exact fingerprint match |
| phase3b-telemetry-disabled | 3 | 6,000 | Exact match |
| phase3b-telemetry-console | 3 | 6,000 | Exact match |
| phase3b-permanent-failure-smoke | 4 | 80 | Exact match |
| phase3b-smoke | 8 | 240 | Original mismatch retained |

Baseline fingerprint is
`192ab442c553227bc34ccba923720ee7568fbc107cce4acd93f94b91e859b81d`,
recomputed from committed parent bytes with the recorded path/NUL/bytes/NUL
algorithm. This remains dirty-run source equivalence, not clean-run provenance.
The current candidate changes pyproject.toml, which is a covered fingerprint
input, so its aggregate fingerprint is expected to differ. No changed runtime or
benchmark behavior justifies rerunning the performance matrix. Historical raw DB
rows are unavailable; saved row-level gates cannot be independently reconstructed
from latency samples. The full suite reruns functional benchmark smoke only.

Workloads are synthetic, handlers essentially no-op, effects DB rows on the same
PostgreSQL instance. No real network delivery, production equivalence, measured
HTTP end-to-end latency, population-tail guarantee or isolated bottleneck claim
is made. See [Phase 3B](phase-3b-validation.md), [method](benchmarks.md) and
[historical reliability audit](reliability-audit.md).

## Human review gate and publication decision

The license and package metadata are consistent. New documentation links and
performance claims are reviewed against repository artifacts. The Phase 4A audit
is unchanged: its earlier baseline, license blocker and lack of then-current CI
remain truthful historical observations. Current Phase 4A CI was independently
queried read-only: [run 43529511460](https://github.com/Diego-2510/reliable-task-engine/actions/runs/43529511460)
completed successfully on exact `91ef58585af5f62446fa5349e7b034549a5cdb0f`.
It is not CI evidence for Phase 4B changes.

Material limitations are prominent in the README, case study, notes and runbook:
trusted-local synthetic scope, no auth/tenancy/real HTTP/automatic retention,
no graceful worker drain or bounded DB I/O timeout, manual restart and ambiguous
final delivery outcomes. No production-ready claim is made. **GO for local candidate review; NO-GO for publication at this stage.** No confirmed
P0/P1 blocker is known within this scope; publication still requires the owner review,
release commit, exact-commit CI, tag and release gates.

There is **no exact commit to tag yet**. The existing HEAD is the parent baseline.
After separately authorized review/commit work, tag only the final commit that
contains this entire candidate and passes remote CI. Proposed commit subjects,
file groups and sequence are in [release readiness](release-readiness.md#release-checklist-and-human-review-gate).
Do not tag a license-only intermediate commit. Proposed publication text is in
[release notes](releases/v0.1.0.md); its versioned URLs map to included local paths
and become externally resolvable after the future tag exists.
