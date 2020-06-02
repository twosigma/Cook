import uuid

import cook.scheduler.client.constraints

from datetime import datetime, timedelta
from unittest import TestCase

from cook.scheduler.client.instance import Instance, Executor
from cook.scheduler.client.jobs import Application, Job
from cook.scheduler.client.jobs import Status as JobStatus
from cook.scheduler.client.jobs import State as JobState
from cook.scheduler.client.util import Dataset, FetchableUri

JOB_DICT_NO_OPTIONALS = {
    'command': 'ls',
    'mem': 10.0,
    'cpus': 1.0,
    # UUID sample from Wikipedia
    'uuid': '123e4567-e89b-12d3-a456-426614174000',
    'name': 'test-job',
    'max_retries': 10,
    'max_runtime': 15,
    'status': 'running',
    'state': 'waiting',
    'priority': 50,
    'framework_id': 'unsupported',
    'retries_remaining': 5,
    'submit_time': 123123123123,
    'user': 'vagrant'
}

JOB_DICT_WITH_OPTIONALS = {**JOB_DICT_NO_OPTIONALS, **{
    'executor': 'cook',
    'container': {},
    'disable_mea_culpa_retries': True,
    'expected_runtime': 1000,
    'pool': 'default',
    'instances': [
        {
            'task_id': '123e4567-e89b-12d3-a456-426614174010',
            'slave_id': 'foo',
            'executor_id': 'foo',
            'start_time': 123123123,
            'hostname': 'host.name',
            'status': 'failed',
            'preempted': True
        },
        {
            'task_id': '123e4567-e89b-12d3-a456-426614174020',
            'slave_id': 'foo',
            'executor_id': 'foo',
            'start_time': 123123321,
            'hostname': 'host.name',
            'status': 'success',
            'preempted': False,
            'end_time': 123123521,
            'progress': 100,
            'progress_message': 'foo',
            'reason_code': 0,
            'output_url': 'http://localhost/output',
            'executor': 'cook',
            'reason_mea_culpa': False
        },
    ],
    'env': {
        'KEY1': 'VALUE',
        'KEY2': 'VALUE'
    },
    'uris': [
        {
            'value': 'localhost:80/resource1'
        },
        {
            'value': 'localhost:80/resource2',
            'executable': False,
            'extract': False,
            'cache': True,
        },
    ],
    'labels': {
        'label1': 'value1',
        'label2': 'value2',
    },
    'constraints': [
        ['EQUALS', 'attr', 'value'],
        ['EQUALS', 'attr2', 'value2'],
    ],
    'group': '123e4567-e89b-12d3-a456-426614174001',
    'groups': [
        '123e4567-e89b-12d3-a456-426614174002',
        '123e4567-e89b-12d3-a456-426614174003'
    ],
    'application': {
        'name': 'pytest',
        'version': '1.0'
    },
    'progress_output_file': 'output.txt',
    'progress_regex_string': 'test',
    'datasets': [
        {
            'dataset': {
                'foo': 'bar'
            }
        },
        {
            'dataset': {
                'partition-type': 'date',
                'foo': 'bar',
                'fizz': 'buzz'
            },
            'partitions': [
                {
                    'begin': '20180201',
                    'end': '20180301'
                },
                {
                    'begin': '20190201',
                    'end': '20190301'
                }
            ]
        }
    ],
    'gpus': 1,
    'ports': 10
}}

JOB_EXAMPLE = Job(
    command='ls',
    mem=10.0,
    cpus=1.0,
    uuid=uuid.uuid4(),
    name='test-job',
    max_retries=10,
    max_runtime=timedelta(seconds=15),
    status=JobStatus.RUNNING,
    state=JobState.WAITING,
    priority=50,
    framework_id='unsupported',
    retries_remaining=5,
    submit_time=datetime.now(),
    user='vagrant',

    executor=Executor.COOK,
    container={},
    disable_mea_culpa_retries=True,
    expected_runtime=timedelta(seconds=1000),
    pool='default',
    instances=[],
    env={
        'KEY1': 'VALUE',
        'KEY2': 'VALUE'
    },
    uris=[
        FetchableUri('localhost:80/resource1'),
        FetchableUri('localhost:80/resource2',
                     executable=False,
                     extract=False,
                     cache=True)
    ],
    labels={
        'label1': 'value1',
        'label2': 'value2'
    },
    constraints=[
        cook.scheduler.client.constraints.build_equals_constraint(
            'attr', 'value')
    ],
    group=uuid.uuid4(),
    groups=[
        uuid.uuid4(),
        uuid.uuid4()
    ],
    application=Application('pytest', '1.0'),
    progress_output_file='output.txt',
    progress_regex_string='test',
    datasets=[
        Dataset(dataset={'foo': 'bar'}),
    ],
    gpus=1,
    ports=10
)


class JobTest(TestCase):
    def test_dict_parse_required(self):
        """Test parsing a job dictionary object parsed from JSON.

        This test case will only test the required fields.
        """
        jobdict = JOB_DICT_NO_OPTIONALS
        job = Job.from_dict(jobdict)
        self.assertEqual(job.command, jobdict['command'])
        self.assertAlmostEqual(job.mem, jobdict['mem'])
        self.assertAlmostEqual(job.cpus, jobdict['cpus'])
        self.assertEqual(str(job.uuid), jobdict['uuid'])
        self.assertEqual(job.name, jobdict['name'])
        self.assertEqual(job.max_retries, jobdict['max_retries'])
        self.assertEqual(int(job.max_runtime.total_seconds()),
                         jobdict['max_runtime'])
        self.assertEqual(str(job.status).lower(), jobdict['status'].lower())
        self.assertEqual(str(job.state).lower(), jobdict['state'].lower())
        self.assertEqual(job.priority, jobdict['priority'])
        self.assertEqual(job.framework_id, jobdict['framework_id'])
        self.assertEqual(job.retries_remaining, jobdict['retries_remaining'])
        # jobdict['submit_time'] is in milliseconds, while datetime.timestamp()
        # is in seconds.
        self.assertEqual(int(job.submit_time.timestamp() * 1000),
                         jobdict['submit_time'])
        self.assertEqual(job.user, jobdict['user'])

    def test_dict_parse_optional(self):
        """Test parsing a job dictionary object parsed from JSON.

        This test case will only test the optional fields.
        """
        jobdict = JOB_DICT_WITH_OPTIONALS
        job = Job.from_dict(jobdict)
        self.assertEqual(str(job.executor).lower(),
                         jobdict['executor'].lower())
        self.assertEqual(job.container, jobdict['container'])
        self.assertEqual(job.disable_mea_culpa_retries,
                         jobdict['disable_mea_culpa_retries'])
        self.assertEqual(int(job.expected_runtime.total_seconds()),
                         jobdict['expected_runtime'])
        self.assertEqual(job.pool, jobdict['pool'])
        self.assertEqual(len(job.instances), len(jobdict['instances']))
        for instance in job.instances:
            self.assertTrue(isinstance(instance, Instance))
        self.assertEqual(job.env, jobdict['env'])
        self.assertEqual(len(job.uris), len(jobdict['uris']))
        for uri in job.uris:
            self.assertTrue(isinstance(uri, FetchableUri))
        self.assertEqual(job.labels, jobdict['labels'])
        self.assertEqual(len(job.constraints), len(jobdict['constraints']))
        self.assertEqual(str(job.group), jobdict['group'])
        self.assertEqual(len(job.groups), len(jobdict['groups']))
        for i in range(len(job.groups)):
            self.assertEqual(str(job.groups[i]), jobdict['groups'][i])
        # Test application inline as it's a very simple structure
        self.assertEqual(job.application.name, jobdict['application']['name'])
        self.assertEqual(job.application.version,
                         jobdict['application']['version'])
        self.assertEqual(job.progress_output_file,
                         jobdict['progress_output_file'])
        self.assertEqual(job.progress_regex_string,
                         jobdict['progress_regex_string'])
        self.assertEqual(len(job.datasets), len(jobdict['datasets']))
        for dataset in job.datasets:
            self.assertTrue(isinstance(dataset, Dataset))
        self.assertEqual(job.gpus, jobdict['gpus'])
        self.assertEqual(job.ports, jobdict['ports'])

    def test_dict_output(self):
        job = JOB_EXAMPLE
        jobdict = job.to_dict()
        # REQUIRED KEYS
        self.assertEqual(job.command, jobdict['command'])
        self.assertAlmostEqual(job.mem, jobdict['mem'])
        self.assertAlmostEqual(job.cpus, jobdict['cpus'])
        self.assertEqual(str(job.uuid), jobdict['uuid'])
        self.assertEqual(job.name, jobdict['name'])
        self.assertEqual(job.max_retries, jobdict['max_retries'])
        self.assertEqual(int(job.max_runtime.total_seconds()),
                         jobdict['max_runtime'])
        self.assertEqual(str(job.status).lower(), jobdict['status'].lower())
        self.assertEqual(str(job.state).lower(), jobdict['state'].lower())
        self.assertEqual(job.priority, jobdict['priority'])
        self.assertEqual(job.framework_id, jobdict['framework_id'])
        self.assertEqual(job.retries_remaining, jobdict['retries_remaining'])
        # jobdict['submit_time'] is in milliseconds, while datetime.timestamp()
        # is in seconds.
        self.assertEqual(int(job.submit_time.timestamp() * 1000),
                         jobdict['submit_time'])
        self.assertEqual(job.user, jobdict['user'])
        # OPTIONAL KEYS
        self.assertEqual(str(job.executor).lower(),
                         jobdict['executor'].lower())
        self.assertEqual(job.container, jobdict['container'])
        self.assertEqual(job.disable_mea_culpa_retries,
                         jobdict['disable_mea_culpa_retries'])
        self.assertEqual(int(job.expected_runtime.total_seconds()),
                         jobdict['expected_runtime'])
        self.assertEqual(job.pool, jobdict['pool'])
        self.assertEqual(len(job.instances), len(jobdict['instances']))
        for instance in jobdict['instances']:
            self.assertTrue(isinstance(instance, dict))
        self.assertEqual(job.env, jobdict['env'])
        self.assertEqual(len(job.uris), len(jobdict['uris']))
        for uri in jobdict['uris']:
            self.assertTrue(isinstance(uri, dict))
        self.assertEqual(job.labels, jobdict['labels'])
        self.assertEqual(len(job.constraints), len(jobdict['constraints']))
        self.assertEqual(str(job.group), jobdict['group'])
        self.assertEqual(len(job.groups), len(jobdict['groups']))
        for i in range(len(job.groups)):
            self.assertEqual(str(job.groups[i]), jobdict['groups'][i])
        # Test application inline as it's a very simple structure
        self.assertEqual(job.application.name, jobdict['application']['name'])
        self.assertEqual(job.application.version,
                         jobdict['application']['version'])
        self.assertEqual(job.progress_output_file,
                         jobdict['progress_output_file'])
        self.assertEqual(job.progress_regex_string,
                         jobdict['progress_regex_string'])
        self.assertEqual(len(job.datasets), len(jobdict['datasets']))
        for dataset in jobdict['datasets']:
            self.assertTrue(isinstance(dataset, dict))
        self.assertEqual(job.gpus, jobdict['gpus'])
        self.assertEqual(job.ports, jobdict['ports'])
