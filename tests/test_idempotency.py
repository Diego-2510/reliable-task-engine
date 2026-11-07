import multiprocessing as mp
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select, text

from task_engine.api import app
from task_engine.db import Session, engine
from task_engine.models import Job
from task_engine.worker import claim, complete, run_once


def submit(key=None, payload=None, **kwargs):
    headers = {} if key is None else {'Idempotency-Key': key}
    with TestClient(app) as client:
        return client.post('/v1/jobs', headers=headers,
                           json={'payload': payload or {}, **kwargs})


def test_unkeyed_submissions(database):
    responses = [submit() for _ in range(2)]
    assert [r.status_code for r in responses] == [201, 201]
    assert len({r.json()['id'] for r in responses}) == 2


def test_normalized_replay_and_conflicts(database, caplog):
    key = 'private-submission-key'
    first = submit(key, {'a': {'x': 1, 'y': 2}, 'b': [1, 2]})
    assert first.status_code == 201
    replay = submit(key, {'b': [1, 2], 'a': {'y': 2, 'x': 1}}, max_attempts=3)
    assert replay.status_code == 200 and replay.json() == first.json()
    for payload, limit in [({'a': 2}, 3), ({'a': {'x': 1, 'y': 2}, 'b': [1, 2]}, 4)]:
        conflict = submit(key, payload, max_attempts=limit)
        assert conflict.status_code == 409
        assert conflict.json()['error']['code'] == 'idempotency_conflict'
        assert key not in conflict.text
    assert key not in first.text + replay.text + caplog.text
    with Session() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1


def test_json_types_and_array_order_are_distinct(database):
    assert submit('types', {'value': True}).status_code == 201
    assert submit('types', {'value': 1}).status_code == 409
    assert submit('array', {'value': [1, 2]}).status_code == 201
    assert submit('array', {'value': [2, 1]}).status_code == 409


@pytest.mark.parametrize('key', ['', ' ', 'a b', 'a\tb', 'a\nb', 'a\rb', '\x00', '\x1f', '\x7f', 'x' * 129])
def test_invalid_keys(database, key):
    response = submit(key)
    assert response.status_code == 422
    assert response.json() == {'error': {'code': 'invalid_input', 'message': 'Invalid request'}}
    with Session() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_non_ascii_and_duplicate_headers(database):
    with TestClient(app) as client:
        for headers in [[(b'idempotency-key', b'\xff')],
                        [('Idempotency-Key', 'one'), ('Idempotency-Key', 'two')]]:
            assert client.post('/v1/jobs', headers=headers, json={'payload': {}}).status_code == 422


@pytest.mark.parametrize('key', ['!', '~', 'x' * 128])
def test_valid_key_boundaries(database, key):
    assert submit(key).status_code == 201
    assert submit(key).status_code == 200


def _submit_process(barrier, results, key, payload):
    # Spawn creates a fresh API module and connection pool in every process.
    with TestClient(app) as client:
        barrier.wait(timeout=30)
        response = client.post('/v1/jobs', headers={'Idempotency-Key': key},
                               json={'payload': payload})
        results.put((response.status_code, response.json(), payload))


def parallel_submissions(keys, payloads):
    ctx = mp.get_context('spawn')
    barrier, results = ctx.Barrier(8), ctx.Queue()
    processes = [ctx.Process(target=_submit_process, args=(barrier, results, key, payload))
                 for key, payload in zip(keys, payloads, strict=True)]
    try:
        for process in processes:
            process.start()
        outcomes = [results.get(timeout=45) for _ in processes]
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
        return outcomes
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            if process.pid is not None:
                process.join(timeout=5)
        results.close()
        results.join_thread()


@pytest.mark.parametrize('different_payloads', [False, True])
def test_eight_process_duplicate_submissions(database, different_payloads):
    payloads = [{'value': i if different_payloads else 0} for i in range(8)]
    outcomes = parallel_submissions(['shared'] * 8, payloads)
    statuses = [status for status, _, _ in outcomes]
    assert statuses.count(201) == 1
    assert statuses.count(409 if different_payloads else 200) == 7
    winner = next(body for status, body, _ in outcomes if status == 201)
    for status, body, payload in outcomes:
        if status == 409:
            assert body == {'error': {'code': 'idempotency_conflict',
                                     'message': 'Job parameters differ from the original submission'}}
            assert payload != winner['payload']
        else:
            assert body['id'] == winner['id']
    with Session() as session:
        job = session.scalars(select(Job)).one()
        assert str(job.id) == winner['id'] and job.attempts == 0
        assert job.payload == winner['payload']


def test_eight_independent_keys(database):
    outcomes = parallel_submissions([f'key-{i}' for i in range(8)], [{}] * 8)
    assert all(status == 201 for status, _, _ in outcomes)
    assert len({body['id'] for _, body, _ in outcomes}) == 8


@pytest.mark.parametrize('terminal', ['succeeded', 'failed'])
def test_replay_preserves_current_state_and_attempts(database, terminal):
    first = submit('finished')
    job_id, token, _ = claim()
    running = submit('finished')
    assert running.status_code == 200 and running.json()['status'] == 'running'
    complete(job_id, token, terminal)
    with TestClient(app) as client:
        before = client.get('/v1/jobs/' + first.json()['id']).json()
    replay = submit('finished')
    assert replay.status_code == 200 and replay.json() == before
    assert replay.json()['attempts'] == 1
    assert not run_once()


def test_replay_after_api_process_restart(database):
    first = submit('durable')
    # All child processes import the app afresh; no parent application state is inherited.
    outcomes = parallel_submissions(['durable'] * 8, [{}] * 8)
    assert all(status == 200 and body['id'] == first.json()['id'] for status, body, _ in outcomes)


def test_unrelated_integrity_error_is_not_conflict(database, caplog):
    first = submit('original')
    fixed_id = uuid.UUID(first.json()['id'])
    def duplicate_id(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith('INSERT INTO jobs'):
            parameters = {**parameters, 'id': fixed_id}
        return statement, parameters

    event.listen(engine, 'before_cursor_execute', duplicate_id, retval=True)
    try:
        response = submit('private-failing-key')
    finally:
        event.remove(engine, 'before_cursor_execute', duplicate_id)
    assert response.status_code == 500
    assert response.json()['error']['code'] == 'database_error'
    assert 'private-failing-key' not in response.text + caplog.text
    with Session() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1


@pytest.mark.parametrize('stage', ['insert', 'select'])
def test_real_database_errors_not_masked(database, stage, caplog):
    key = 'private-db-error-key'
    if stage == 'select':
        assert submit(key).status_code == 201

    def fail_statement(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith(stage.upper()):
            cursor.execute('SELECT 1 / 0')

    event.listen(engine, 'before_cursor_execute', fail_statement)
    try:
        response = submit(key)
        assert response.status_code == 500
        assert response.json()['error']['code'] == 'database_error'
        assert key not in response.text + caplog.text
    finally:
        event.remove(engine, 'before_cursor_execute', fail_statement)
    with Session() as session:
        assert session.scalar(select(text('1'))) == 1
        assert session.scalar(select(func.count()).select_from(Job)) == (stage == 'select')
