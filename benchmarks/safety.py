"""Fail closed before importing runtime modules or touching data."""
import os
import re
import subprocess
import sys
from collections.abc import Mapping
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

MARKER = "reliable-task-engine-disposable-benchmark-v1"
LOCK_ID = 739_031_381


class BenchmarkError(RuntimeError):
    """A fixed, safe diagnostic suitable for persisted failure evidence."""


def database_url(environ: Mapping[str, str]) -> str:
    raw = environ.get("BENCHMARK_DATABASE_URL", "")
    if environ.get("BENCHMARK_DATABASE") != "1" or not raw:
        raise BenchmarkError("explicit_benchmark_authorization_required")
    try:
        url = make_url(raw)
    except ArgumentError:
        raise BenchmarkError("unsafe_benchmark_database") from None
    if (url.drivername != "postgresql+psycopg" or not url.host or url.query
            or not re.fullmatch(r"[A-Za-z0-9_]+_test", url.database or "")):
        raise BenchmarkError("unsafe_benchmark_database")
    return raw


def verify_identity(connection, url):
    if connection.scalar(text("SELECT current_database()")) != make_url(url).database:
        raise BenchmarkError("database_identity_mismatch")


def verify_owned(connection, url):
    verify_identity(connection, url)
    if connection.scalar(text("SELECT to_regclass('public.benchmark_ownership')")) is None:
        raise BenchmarkError("database_not_benchmark_owned_run_prepare_on_empty_database")
    markers = connection.execute(text("SELECT marker FROM public.benchmark_ownership")).scalars().all()
    if markers != [MARKER]:
        raise BenchmarkError("database_ownership_mismatch")


@contextmanager
def exclusive_database(url):
    engine = create_engine(url, hide_parameters=True, connect_args={"connect_timeout": 5})
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            verify_identity(connection, url)
            connection.execute(text("SET statement_timeout = '5s'"))
            connection.execute(text("SET lock_timeout = '5s'"))
            if not connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": LOCK_ID}):
                raise BenchmarkError("another_benchmark_owns_database")
            try:
                others = connection.scalar(text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname=current_database() AND pid<>pg_backend_pid() "
                    "AND backend_type='client backend'"
                ))
                if others:
                    raise BenchmarkError("competing_database_connections")
                yield connection
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": LOCK_ID})
    finally:
        engine.dispose()


def prepare(connection, url):
    """Adopt ONLY an empty DB; never migrate an existing application database."""
    verify_identity(connection, url)
    objects = connection.execute(text(
        "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
        "AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S','f')"
    )).all()
    if objects:
        verify_owned(connection, url)
        if connection.scalar(text("SELECT version_num FROM alembic_version")) != "0007":
            raise BenchmarkError("owned_database_schema_not_current_no_automatic_upgrade")
        return
    connection.execute(text("CREATE TABLE public.benchmark_ownership (marker text PRIMARY KEY)"))
    connection.execute(text("INSERT INTO public.benchmark_ownership VALUES (:marker)"),
                       {"marker": MARKER})
    env = dict(os.environ, DATABASE_URL=url, TASK_ENGINE_TELEMETRY="disabled")
    result = subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                            env=env, capture_output=True, timeout=60, check=False)
    if result.returncode:
        raise BenchmarkError("benchmark_schema_initialization_failed")


def clean(connection, url):
    verify_owned(connection, url)
    # One statement, no CASCADE, only this dedicated database's synthetic runtime tables.
    connection.execute(text("TRUNCATE synthetic_receiver_events, outbox_events, jobs"))
