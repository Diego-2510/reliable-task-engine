import pytest
from conftest import disposable_database_url

from task_engine.worker import lease_seconds


@pytest.mark.parametrize("environ", [
    {},
    {"TEST_DATABASE": "1"},
    {"TEST_DATABASE": "0", "TEST_DATABASE_URL": "postgresql+psycopg://task@db/task_test"},
    {"TEST_DATABASE": "true", "TEST_DATABASE_URL": "postgresql+psycopg://task@db/task_test"},
    {"TEST_DATABASE": "1", "DATABASE_URL": "postgresql+psycopg://task@db/task_test"},
    {"TEST_DATABASE": "1", "TEST_DATABASE_URL": "postgresql+psycopg://task@db/task"},
    {"TEST_DATABASE": "1", "TEST_DATABASE_URL": "postgresql+psycopg://task@db/production"},
    {"TEST_DATABASE": "1", "TEST_DATABASE_URL": "sqlite:///task_test"},
    {"TEST_DATABASE": "1", "TEST_DATABASE_URL": "not a URL"},
    {"TEST_DATABASE": "1", "TEST_DATABASE_URL": "postgresql+psycopg:///task_test"},
    {"TEST_DATABASE": "1", "TEST_DATABASE_URL": "postgresql+psycopg://task@db/task_test?dbname=task"},
])
def test_reject_unsafe_database_configuration(environ):
    with pytest.raises(pytest.UsageError):
        disposable_database_url(environ)


def test_explicit_disposable_database():
    url = "postgresql+psycopg://task:task@localhost:5432/task_test"
    assert disposable_database_url({"TEST_DATABASE": "1", "TEST_DATABASE_URL": url}) == url


@pytest.mark.parametrize("value", ["0", "-1", "abc", "1.5", ""])
def test_invalid_lease_duration(monkeypatch, value):
    monkeypatch.setenv("LEASE_SECONDS", value)
    with pytest.raises(ValueError):
        lease_seconds()


def test_default_lease_duration(monkeypatch):
    monkeypatch.delenv("LEASE_SECONDS", raising=False)
    assert lease_seconds() == 60


@pytest.mark.parametrize("configuration", ["missing", "wrong_name", "not_authorized"])
def test_pytest_guard_aborts_before_deletion(database, configuration):
    import os
    import subprocess
    import sys

    from sqlalchemy import func, select

    from task_engine.db import Session
    from task_engine.models import Job

    with Session.begin() as session:
        session.add(Job(payload={"sentinel": True}))
    environ = dict(os.environ)
    if configuration == "missing":
        environ.pop("TEST_DATABASE_URL", None)
    elif configuration == "wrong_name":
        environ["TEST_DATABASE_URL"] = "postgresql+psycopg://task@localhost/production"
    else:
        environ.pop("TEST_DATABASE", None)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "tests/test_engine.py::test_api_and_worker"],
        check=False, env=environ, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 4
    assert "Destructive tests require" in result.stderr
    with Session() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1
