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
