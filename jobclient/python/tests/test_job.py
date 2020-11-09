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

import sys
import uuid

from datetime import datetime, timedelta
from unittest import TestCase

from cookclient.containers import (
    AbstractContainer,
    DockerContainer,
    DockerPortMapping,
    Volume
)
from cookclient.instance import Instance, Executor
from cookclient.jobs import Application, Disk, Job
from cookclient.jobs import Status as JobStatus
from cookclient.jobs import State as JobState
from cookclient.util import datetime_to_unix_ms

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
    'container': {
        'type': 'docker',
        'volumes': [
            {
                'host-path': '/home/user1/bin',
                'container-path': '/usr/local/bin',
                'mode': 'rw'
            },
            {
                'host-path': '/home/user1/include',
                'container-path': '/usr/local/include',
                'mode': 'r'
            }
        ],
        'docker': {
            'image': 'alpine:latest',
            'network': 'my-network',
            'force-pull-image': True,
            'parameters': [
                {'key': 'key1', 'value': 'value1'},
                {'key': 'key2', 'value': 'value2'}
            ],
            'port-mapping': [
                {
                    'host-port': 80,
                    'container-port': 8080,
                    'protocol': 'tcp'
                },
                {
                    'host-port': 443,
                    'container-port': 443,
                    'protocol': 'tcp'
                }
            ]
        }
    },
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
            'preempted': True,
            'backfilled': False,
            'ports': [
                443,
            ],
            'compute-cluster': {}
        },
        {
            'task_id': '123e4567-e89b-12d3-a456-426614174020',
            'slave_id': 'foo',
            'executor_id': 'foo',
            'start_time': 123123321,
            'hostname': 'host.name',
            'status': 'success',
            'preempted': False,
            'backfilled': False,
            'ports': [],
            'compute-cluster': {},
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
    'ports': 10,
}}

JOB_DICT_GROUP_AND_GROUPS = {**JOB_DICT_NO_OPTIONALS, **{
    'group': '123e4567-e89b-12d3-a456-426614174002',
    'groups': [
        '123e4567-e89b-12d3-a456-426614174003',
        '123e4567-e89b-12d3-a456-426614174004'
    ]
}}

JOB_DICT_NO_GROUPS = {**JOB_DICT_NO_OPTIONALS, **{
    'groups': []
}}

JOB_DICT_ONE_GROUP = {**JOB_DICT_NO_OPTIONALS, **{
    'groups': [
        '123e4567-e89b-12d3-a456-426614174003'
    ]
}}

JOB_DICT_MANY_GROUPS = {**JOB_DICT_NO_OPTIONALS, **{
    'groups': [
        '123e4567-e89b-12d3-a456-426614174003',
        '123e4567-e89b-12d3-a456-426614174004'
    ]
}}

JOB_DICT_DISK_ONLY_REQUEST = {**JOB_DICT_NO_OPTIONALS, **{
    'disk': {
        'request': 10.0,
     }
}}

JOB_DICT_DISK_ONLY_REQUEST_AND_LIMIT = {**JOB_DICT_NO_OPTIONALS, **{
    'disk': {
        'request': 10.0,
        'limit': 20.0
     }
}}

JOB_DICT_DISK_ALL_FIELDS = {**JOB_DICT_NO_OPTIONALS, **{
    'disk': {
        'request': 10.0,
        'limit': 20.0,
        'type': 'standard'
     }
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
    container=DockerContainer('alpine:latest', network='my-network',
                              force_pull_image=True,
                              parameters=[
                                  {'key': 'key1', 'value': 'value1'},
                                  {'key': 'key2', 'value': 'value2'}
                              ],
                              port_mapping=[
                                  DockerPortMapping(host_port=80,
                                                    container_port=8080,
                                                    protocol='tcp'),
                                  DockerPortMapping(host_port=443,
                                                    container_port=443,
                                                    protocol='tcp')
                              ],
                              volumes=[
                                  Volume(host_path='/home/user1/bin',
                                         container_path='/usr/local/bin',
                                         mode='rw'),
                                  Volume(host_path='/home/user1/include',
                                         container_path='/usr/local/include',
                                         mode='r'),
                              ]),
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
    ports=10,
    disk=Disk(request=10.0, limit=20.0, type='standard')
)


class JobTest(TestCase):
    def _check_required_fields(self, job: Job, jobdict: dict):
        self.assertEqual(job.command, jobdict['command'])
        self.assertAlmostEqual(job.mem, jobdict['mem'])
        self.assertAlmostEqual(job.cpus, jobdict['cpus'])
        self.assertEqual(str(job.uuid), jobdict['uuid'])
        self.assertEqual(job.name, jobdict['name'])
        self.assertEqual(job.max_retries, jobdict['max_retries'])
        self.assertEqual(int(job.max_runtime.total_seconds() * 1000),
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
        if 'groups' in jobdict and len(jobdict['groups']) > 0:
            self.assertEqual(str(job.group), jobdict['groups'][0])
        else:
            self.assertIsNone(job.group)

    def _check_disk(self, job: Job, jobdict: dict):
        """Check the disk field of a Job.

        The Job's disk attribute should be either:
        * The value of `jobdict['disk']` if set,
        * Otherwise, the value of `jobdict['disk'][0]` if set,
        * Otherwise, `None`.
        """
        if 'disk' in jobdict:
            self.assertEqual(job.disk.to_dict(), jobdict['disk'])
        else:
            self.assertIsNone(job.disk)

    def _check_optional_fields(self, job: Job, jobdict: dict):
        self._check_group(job, jobdict)
        self._check_disk(job, jobdict)
        self.assertEqual(str(job.executor).lower(),
                         jobdict['executor'].lower())
        self.assertTrue(isinstance(job.container, AbstractContainer))
        self.assertEqual(len(job.container.volumes),
                         len(jobdict['container']['volumes']))
        for vol, voldict in zip(job.container.volumes,
                                jobdict['container']['volumes']):
            self.assertTrue(isinstance(vol, Volume))
            self.assertEqual(vol.host_path, voldict['host-path'])
            self.assertEqual(vol.container_path,
                             voldict['container-path'])
            self.assertEqual(vol.mode, voldict['mode'])
        if jobdict['container']['type'] == 'docker':
            docdict = jobdict['container']['docker']
            self.assertTrue(isinstance(job.container, DockerContainer))
            self.assertEqual(job.container.image, docdict['image'])
            self.assertEqual(job.container.network, docdict['network'])
            self.assertEqual(job.container.force_pull_image,
                             docdict['force-pull-image'])
            self.assertEqual(job.container.parameters,
                             docdict['parameters'])
            self.assertEqual(len(job.container.port_mapping),
                             len(docdict['port-mapping']))
            for pm, pmdict in zip(job.container.port_mapping,
                                  docdict['port-mapping']):
                self.assertTrue(isinstance(pm, DockerPortMapping))
                self.assertEqual(pm.host_port, pmdict['host-port'])
                self.assertEqual(pm.container_port,
                                 pmdict['container-port'])
                self.assertEqual(pm.protocol, pmdict['protocol'])
        self.assertEqual(job.disable_mea_culpa_retries,
                         jobdict['disable_mea_culpa_retries'])
        self.assertEqual(int(job.expected_runtime.total_seconds() * 1000),
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
        # jobdict has no groups
        jobdict = JOB_DICT_NO_GROUPS
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

        # jobdict has only one group
        jobdict = JOB_DICT_ONE_GROUP
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

        # jobdict has many groups
        jobdict = JOB_DICT_MANY_GROUPS
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

        # jobdict has neither
        jobdict = JOB_DICT_NO_OPTIONALS
        job = Job.from_dict(jobdict)
        self._check_group(job, jobdict)

    def test_dict_parse_disk(self):
        # jobdict only has request
        jobdict = JOB_DICT_DISK_ONLY_REQUEST
        job = Job.from_dict(jobdict)
        self._check_disk(job, jobdict)

        # jobdict has request and limit
        jobdict = JOB_DICT_DISK_ONLY_REQUEST_AND_LIMIT
        job = Job.from_dict(jobdict)
        self._check_disk(job, jobdict)

        # jobdict has request, limit, and type
        jobdict = JOB_DICT_DISK_ALL_FIELDS
        job = Job.from_dict(jobdict)
        self._check_disk(job, jobdict)


    def test_dict_output(self):
        job = JOB_EXAMPLE
        jobdict = job.to_dict()
        self._check_required_fields(job, jobdict)
        self._check_optional_fields(job, jobdict)

    def test_timedelta_overflows(self):
        """Test parsing a job with runtime values that overflow timedelta."""
        jobdict = JOB_DICT_NO_OPTIONALS
        jobdict.update({
            'max_runtime': sys.maxsize,
            'expected_runtime': sys.maxsize
        })
        job = Job.from_dict(jobdict)

        self.assertEqual(job.max_runtime, timedelta.max)
        self.assertEqual(job.expected_runtime, timedelta.max)
