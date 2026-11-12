"""Local operator recovery; no HTTP endpoint and no receiver invocation."""

import argparse
import logging
import sys
import uuid
from collections.abc import Sequence
from typing import NoReturn

from sqlalchemy import func, select, update
from sqlalchemy.exc import SQLAlchemyError

from task_engine.db import Session
from task_engine.models import OutboxEvent
from task_engine.telemetry import configure_logging, initialize, record, traced

PG_INTEGER_MAX = 2_147_483_647
MAX_EXTRA_ATTEMPTS = 1000


class ReplayError(Exception):
    """Fixed diagnostic and stable process exit code."""

    def __init__(self, code: str, exit_code: int) -> None:
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


@traced("operator", "replay")
def replay_event(event_id: uuid.UUID, extra_attempts: int) -> None:
    if type(extra_attempts) is not int or not 1 <= extra_attempts <= MAX_EXTRA_ATTEMPTS:
        raise ReplayError("invalid_budget", 2)
    with Session.begin() as session:
        row = session.execute(
            select(OutboxEvent.delivery_state, OutboxEvent.max_delivery_attempts,
                   OutboxEvent.replay_count)
            .where(OutboxEvent.id == event_id).with_for_update()
        ).one_or_none()
        if row is None:
            raise ReplayError("event_not_found", 3)
        if row.delivery_state != "dead_letter":
            raise ReplayError("event_not_dead_letter", 4)
        if (row.max_delivery_attempts > PG_INTEGER_MAX - extra_attempts
                or row.replay_count >= PG_INTEGER_MAX):
            raise ReplayError("budget_or_counter_overflow", 5)
        session.execute(
            update(OutboxEvent).where(OutboxEvent.id == event_id).values(
                max_delivery_attempts=OutboxEvent.max_delivery_attempts + extra_attempts,
                replay_count=OutboxEvent.replay_count + 1,
                last_replayed_at=func.clock_timestamp(), delivery_state="pending",
                next_attempt_at=func.clock_timestamp(), dead_lettered_at=None,
                dead_letter_reason=None, delivery_token=None, delivery_lease_expires_at=None,
            )
        )
    # Successful return requires confirmed commit; attempts and receiver rows never change.
    record("operator", "replay", "replayed")


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        # argparse's usual diagnostic echoes invalid user input.
        self.exit(2, "operator: invalid_arguments\n")


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging(level=logging.WARNING)
    initialize("operator")
    parser = _Parser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay-event")
    replay.add_argument("event_id", type=uuid.UUID)
    replay.add_argument("--extra-attempts", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        replay_event(args.event_id, args.extra_attempts)
    except ReplayError as error:
        print(f"operator: {error.code}", file=sys.stderr)
        return error.exit_code
    except SQLAlchemyError:
        # A lost COMMIT response may mean replay already committed; inspect before retrying.
        print("operator: database_error", file=sys.stderr)
        return 1
    except Exception:  # noqa: BLE001 - fatal CLI boundary must not expose diagnostics
        print("operator: unexpected_error", file=sys.stderr)
        return 1
    print("operator: replay_scheduled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
