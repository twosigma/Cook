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

from datetime import datetime
from unittest import TestCase

from cookclient.instance import Executor, Instance
from cookclient.instance import Status as InstanceStatus
from cookclient.util import datetime_to_unix_ms

INSTANCE_DICT_NO_OPTIONALS = {
    'task_id': '123e4567-e89b-12d3-a456-426614174010',
    'slave_id': 'foo',
    'executor_id': 'foo',
    'start_time': 123123123,
    'hostname': 'host.name',
    'status': 'failed',
    'preempted': True,
    'backfilled': True,
    'ports': [
        22,
        80
    ],
    'compute-cluster': {}
}

INSTANCE_DICT_WITH_OPTIONALS = {**INSTANCE_DICT_NO_OPTIONALS, **{
    'end_time': 123123521,
    'progress': 100,
    'progress_message': 'foo',
    'reason_code': 0,
    'reason_string': "Segmentation fault",
    'output_url': 'http://localhost/output',
    'executor': 'cook',
    'reason_mea_culpa': False,
    'exit_code': 1
}}

INSTANCE_EXAMPLE = Instance(
    task_id=uuid.uuid4(),
    slave_id='foo',
    executor_id='foo',
    start_time=datetime.now(),
    hostname='host.name',
    status=InstanceStatus.RUNNING,
    preempted=False,
    ports=[22, 80],
    compute_cluster={},

    end_time=datetime.now(),
    progress=100,
    progress_message='foo',
    reason_code=0,
    reason_string="Segmentation fault",
    output_url='http://localhost/output',
    executor=Executor.COOK,
    reason_mea_culpa=False,
    exit_code=1
)


class InstanceTest(TestCase):
    def _check_required_fields(self, inst: Instance, instdict: dict):
        self.assertEqual(str(inst.task_id), instdict['task_id'])
        self.assertEqual(inst.slave_id, instdict['slave_id'])
        self.assertEqual(inst.executor_id, instdict['executor_id'])
        self.assertEqual(datetime_to_unix_ms(inst.start_time),
                         instdict['start_time'])
        self.assertEqual(inst.hostname, instdict['hostname'])
        self.assertEqual(str(inst.status).lower(), instdict['status'].lower())
        self.assertEqual(inst.preempted, instdict['preempted'])
        self.assertEqual(inst.ports, instdict['ports'])
        self.assertEqual(inst.compute_cluster, instdict['compute-cluster'])

    def _check_optional_fields(self, inst: Instance, instdict: dict):
        self.assertEqual(datetime_to_unix_ms(inst.end_time),
                         instdict['end_time'])
        self.assertEqual(inst.progress, instdict['progress'])
        self.assertEqual(inst.progress_message, instdict['progress_message'])
        self.assertEqual(inst.reason_code, instdict['reason_code'])
        self.assertEqual(inst.output_url, instdict['output_url'])
        self.assertEqual(str(inst.executor).lower(),
                         instdict['executor'].lower())
        self.assertEqual(inst.reason_mea_culpa, instdict['reason_mea_culpa'])
        self.assertEqual(inst.reason_string, instdict['reason_string'])
        self.assertEqual(inst.exit_code, instdict['exit_code'])

    def test_dict_parse_required(self):
        instdict = INSTANCE_DICT_NO_OPTIONALS
        inst = Instance.from_dict(instdict)
        self._check_required_fields(inst, instdict)

    def test_dict_parse_optional(self):
        instdict = INSTANCE_DICT_WITH_OPTIONALS
        inst = Instance.from_dict(instdict)
        self._check_optional_fields(inst, instdict)

    def test_dict_output(self):
        inst = INSTANCE_EXAMPLE
        instdict = inst.to_dict()
        self._check_required_fields(inst, instdict)
        self._check_optional_fields(inst, instdict)
