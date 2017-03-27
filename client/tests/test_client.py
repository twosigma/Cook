import pytest

from cook.client import CookClient, Group, Job, Instance

client = CookClient(url='http://127.0.0.1:12321/swagger-docs')

def test_group():
    group0 = Group(client=client)

    assert group0.uuid is not None
    assert group0.name is not None
    assert group0.jobs is not None

    group1 = Group(client=client, uuid=group0.uuid)

    assert group0.uuid == group1.uuid
    assert group0.name == group1.name
    assert group0.jobs == group1.jobs


def test_job():
    job0 = Job(client=client, command='echo 1')

    assert job0.uuid is not None
    assert job0.name is not None

    job1 = Job(client=client, uuid=job0.uuid)

    assert job0.uuid == job1.uuid
    assert job0.name == job1.name

def test_instance():
    job = Job(client=client, command='echo 1').wait()
    instance0 = job.instances[0]
    instance1 = Instance(client=client, uuid=instance0.task_id)

    assert instance0.uuid == instance1.uuid
    assert instance0.status == instance1.status
