# Copyright (c) Two Sigma Open Source, LLC
#
# Licensed under the Apache license, Version 2.0 (the "License");
# you may not use this file ecept in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import uuid

from datetime import datetime, timedelta
from unittest import TestCase

from cook.scheduler.client.instance import Instance, Executor
from cook.scheduler.client.jobs import Application, Job
from cook.scheduler.client.jobs import Status as JobStatus
from cook.scheduler.client.jobs import State as JobState
from cook.scheduler.client.util import datetime_to_unix_ms

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
    'groups': [
        '123e4567-e89b-12d3-a456-426614174002'
    ],
    'application': {
        'name': 'pytest',
        'version': '1.0'
    },
    'progress_output_file': 'output.txt',
    'progress_regex_string': 'test',
    'gpus': 1,
    'ports': 10
}}

JOB_DICT_GROUP_AND_GROUPS = {**JOB_DICT_NO_OPTIONALS, **{
    'group': '123e4567-e89b-12d3-a456-426614174002',
    'groups': [
        '123e4567-e89b-12d3-a456-426614174003',
        '123e4567-e89b-12d3-a456-426614174004'
    ]
}}

JOB_DICT_GROUP_NO_GROUPS = {**JOB_DICT_NO_OPTIONALS, **{
    'group': '123e4567-e89b-12d3-a456-426614174002'
}}

JOB_DICT_GROUPS_NO_GROUP = {**JOB_DICT_NO_OPTIONALS, **{
    'groups': [
        '123e4567-e89b-12d3-a456-426614174003',
        '123e4567-e89b-12d3-a456-426614174004'
    ]
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
        'localhost:80/resource1',
        'localhost:80/resource2',
    ],
    labels={
        'label1': 'value1',
        'label2': 'value2'
    },
    constraints=[
        ['EQUALS', 'attr', 'value']
    ],
    group=uuid.uuid4(),
    application=Application('pytest', '1.0'),
    progress_output_file='output.txt',
    progress_regex_string='test',
    gpus=1,
    ports=10
)


class JobTest(TestCase):
    def _check_required_fields(self, job: Job, jobdict: dict):
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
        self.assertEqual(datetime_to_unix_ms(job.submit_time),
                         jobdict['submit_time'])
        self.assertEqual(job.user, jobdict['user'])

    def _check_group(self, job: Job, jobdict: dict):
        """Check the group field of a Job.

        The Job's group attribute should be either:
        * The value of `jobdict['group']` if set,
        * Otherwise, the value of `jobdict['group'][0]` if set,
        * Otherwise, `None`.
        """
        if 'group' in jobdict:
            self.assertEqual(str(job.group), jobdict['group'])
        elif 'groups' in jobdict:
            self.assertEqual(str(job.group), jobdict['groups'][0])
        else:
            self.assertIsNone(job.group)

    def _check_optional_fields(self, job: Job, jobdict: dict):
        self._check_group(job, jobdict)

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
        for i in range(len(job.uris)):
            self.assertEqual(job.uris[i], jobdict['uris'][i]['value'])
        self.assertEqual(job.labels, jobdict['labels'])
        self.assertEqual(job.constraints, jobdict['constraints'])
        # Test application inline as it's a very simple structure
        self.assertEqual(job.application.name, jobdict['application']['name'])
        self.assertEqual(job.application.version,
                         jobdict['application']['version'])
        self.assertEqual(job.progress_output_file,
                         jobdict['progress_output_file'])
        self.assertEqual(job.progress_regex_string,
                         jobdict['progress_regex_string'])
        self.assertEqual(job.gpus, jobdict['gpus'])
        self.assertEqual(job.ports, jobdict['ports'])

    def test_dict_parse_required(self):
        """Test parsing a job dictionary object parsed from JSON.

        This test case will only test the required fields.
        """
        jobdict = JOB_DICT_NO_OPTIONALS
        job = Job.from_dict(jobdict)
        self._check_required_fields(job, jobdict)

    def test_dict_parse_optional(self):
        """Test parsing a job dictionary object parsed from JSON.

        This test case will only test the optional fields.
        """
        jobdict = JOB_DICT_WITH_OPTIONALS
        job = Job.from_dict(jobdict)
        self._check_optional_fields(job, jobdict)

    def test_dict_parse_groups(self):
        # jobdict has both 'group' and 'groups'
        jobdict = JOB_DICT_GROUP_AND_GROUPS
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

        # jobdict has only 'group'
        jobdict = JOB_DICT_GROUP_NO_GROUPS
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

        # jobdict only has 'groups'
        jobdict = JOB_DICT_GROUPS_NO_GROUP
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

        # jobdict has neither
        jobdict = JOB_DICT_NO_OPTIONALS
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

    def test_dict_output(self):
        job = JOB_EXAMPLE
        jobdict = job.to_dict()
        self._check_required_fields(job, jobdict)
        self._check_optional_fields(job, jobdict)
