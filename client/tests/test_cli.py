import pytest

from cook.cli import cli

def test_create_job():
    assert cli(['create-job', 'echo', '1'])[0] is True
    assert cli(['create-job', '-r', 'illegal-json'])[0] is False

def test_create_group():
    assert cli(['create-group'])[0] is True
    assert cli(['create-group', 'illegal-json'])[0] is False

def test_kill_job():
    _, uuids = cli(['create-job', 'echo', '1'])

    assert cli(['kill-job', uuids[0]])[0] is True

def test_retry_job():
    _, uuids = cli(['create-job', 'echo', '1'])

    assert cli(['retry-job', uuids[0]])[0] is True

def test_await_job():
    _, uuids = cli(['create-job', 'echo', '1'])

    assert cli(['await-job', uuids[0]])[0] is True

def test_await_instance():
    _, uuids = cli(['create-job', 'echo', '1'])
    _, jobs = cli(['await-job', uuids[0]])

    assert cli(['await-instance', jobs[0].get('instances')[0].get('task_id')])[0] is True

def test_query_job():
    _, uuids = cli(['create-job', 'echo', '1'])

    assert cli(['query-job', uuids[0]])[0] is True

def test_query_instance():
    _, uuids = cli(['create-job', 'echo', '1'])
    _, jobs = cli(['await-job', uuids[0]])

    assert cli(['query-instance', jobs[0].get('instances')[0].get('task_id')])[0] is True

def test_query_group():
    _, uuids = cli(['create-group'])

    assert cli(['query-group', uuids[0]])[0] is True

def test_kill_instance():
    _, uuids = cli(['create-job', 'echo', '1'])
    _, jobs = cli(['await-job', uuids[0]])

    assert cli(['kill-instance', jobs[0].get('instances')[0].get('task_id')])[0] is True
