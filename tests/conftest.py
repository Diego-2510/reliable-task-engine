import os
import re
from collections.abc import Mapping

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


def disposable_database_url(environ: Mapping[str, str]) -> str:
    """Reject implicit/default databases before importing application modules."""
    raw = environ.get("TEST_DATABASE_URL", "")
    message = (
        "Destructive tests require TEST_DATABASE=1 and an explicit TEST_DATABASE_URL "
        "using postgresql+psycopg and a disposable database name ending in _test"
    )
    if environ.get("TEST_DATABASE") != "1" or not raw:
        raise pytest.UsageError(message)
    try:
        url = make_url(raw)
    except ArgumentError:
        raise pytest.UsageError(message) from None
    if (
        url.drivername != "postgresql+psycopg"
        or not url.host
        or not re.fullmatch(r"[A-Za-z0-9_]+_test", url.database or "")
        or url.query  # Disallow connection overrides such as dbname/service/options.
    ):
        raise pytest.UsageError(message)
    return raw


def pytest_configure(config: pytest.Config) -> None:
    os.environ["DATABASE_URL"] = disposable_database_url(os.environ)


@pytest.fixture(autouse=True)
def default_lease_duration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LEASE_SECONDS", "60")


@pytest.fixture
def database():
    from sqlalchemy import delete, text

    from task_engine.db import Session
    from task_engine.models import Job, OutboxEvent, SyntheticReceiverEvent

    expected = make_url(disposable_database_url(os.environ)).database
    with Session.begin() as session:
        # Verify the actual target before any destructive statement, not just the URL.
        if session.scalar(text("SELECT current_database()")) != expected:
            raise pytest.UsageError("Connected database differs from TEST_DATABASE_URL")
        session.execute(delete(SyntheticReceiverEvent))
        session.execute(delete(OutboxEvent))
        session.execute(delete(Job))
    try:
        yield
    finally:
        with Session.begin() as session:
            if session.scalar(text("SELECT current_database()")) != expected:
                raise pytest.UsageError("Connected database differs from TEST_DATABASE_URL")
            session.execute(delete(SyntheticReceiverEvent))
            session.execute(delete(OutboxEvent))
            session.execute(delete(Job))
