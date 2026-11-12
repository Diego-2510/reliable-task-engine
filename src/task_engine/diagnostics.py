"""Local read-only aggregate snapshot; no row contents or management actions."""

import json
import sys
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from task_engine.db import engine

# One SELECT gives every aggregate the same statement snapshot and database time.
SNAPSHOT_SQL = """
WITH at AS (SELECT statement_timestamp() AS now)
SELECT
 (SELECT jsonb_object_agg(status, n) FROM
   (SELECT status, count(*) AS n FROM jobs GROUP BY status) j) AS jobs_by_status,
 (SELECT jsonb_object_agg(delivery_state, n) FROM
   (SELECT delivery_state, count(*) AS n FROM outbox_events GROUP BY delivery_state) e)
   AS outbox_by_delivery_state,
 (SELECT count(*) FROM jobs, at WHERE status = 'queued'
   AND available_at <= at.now AND attempts < max_attempts) AS due_pending_jobs,
 (SELECT count(*) FROM outbox_events, at WHERE delivery_state = 'pending'
   AND next_attempt_at <= at.now AND delivery_attempts < max_delivery_attempts)
   AS due_pending_outbox_events,
 (SELECT count(*) FROM jobs, at WHERE status = 'running'
   AND lease_expires_at <= at.now) AS expired_job_leases,
 (SELECT count(*) FROM outbox_events, at WHERE delivery_state = 'delivering'
   AND delivery_lease_expires_at <= at.now) AS expired_delivery_leases,
 (SELECT count(*) FROM outbox_events WHERE delivery_state = 'dead_letter')
   AS dead_letter_events
"""


def snapshot() -> dict[str, Any]:
    with engine.connect().execution_options(postgresql_readonly=True) as connection:
        row = dict(connection.execute(text(SNAPSHOT_SQL)).one()._mapping)
    row["jobs_by_status"] = row["jobs_by_status"] or {}
    row["outbox_by_delivery_state"] = row["outbox_by_delivery_state"] or {}
    return row


def main() -> int:
    try:
        print(json.dumps(snapshot(), sort_keys=True))
    except SQLAlchemyError:
        print("diagnostics: database_error", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
