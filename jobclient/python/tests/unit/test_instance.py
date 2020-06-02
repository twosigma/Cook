import uuid

from datetime import datetime
from unittest import TestCase

from cook.scheduler.client.instance import Executor, Instance
from cook.scheduler.client.instance import Status as InstanceStatus

INSTANCE_DICT_NO_OPTIONALS = {
    'task_id': '123e4567-e89b-12d3-a456-426614174010',
    'slave_id': 'foo',
    'executor_id': 'foo',
    'start_time': 123123123,
    'hostname': 'host.name',
    'status': 'failed',
    'preempted': True
}

INSTANCE_DICT_WITH_OPTIONALS = {**INSTANCE_DICT_NO_OPTIONALS, **{
    'end_time': 123123521,
    'progress': 100,
    'progress_message': 'foo',
    'reason_code': 0,
    'output_url': 'http://localhost/output',
    'executor': 'cook',
    'reason_mea_culpa': False
}}

INSTANCE_EXAMPLE = Instance(
    task_id=uuid.uuid4(),
    slave_id='foo',
    executor_id='foo',
    start_time=datetime.now(),
    hostname='host.name',
    status=InstanceStatus.RUNNING,
    preempted=False,

    end_time=datetime.now(),
    progress=100,
    progress_message='foo',
    reason_code=0,
    output_url='http://localhost/output',
    executor=Executor.COOK,
    reason_mea_culpa=False
)


class InstanceTest(TestCase):
    def _check_required_fields(self, inst: Instance, instdict: dict):
        self.assertEqual(str(inst.task_id), instdict['task_id'])
        self.assertEqual(inst.slave_id, instdict['slave_id'])
        self.assertEqual(inst.executor_id, instdict['executor_id'])
        self.assertEqual(int(inst.start_time.timestamp() * 1000),
                         instdict['start_time'])
        self.assertEqual(inst.hostname, instdict['hostname'])
        self.assertEqual(str(inst.status).lower(), instdict['status'].lower())
        self.assertEqual(inst.preempted, instdict['preempted'])

    def _check_optional_fields(self, inst: Instance, instdict: dict):
        self.assertEqual(int(inst.end_time.timestamp() * 1000),
                         instdict['end_time'])
        self.assertEqual(inst.progress, instdict['progress'])
        self.assertEqual(inst.progress_message, instdict['progress_message'])
        self.assertEqual(inst.reason_code, instdict['reason_code'])
        self.assertEqual(inst.output_url, instdict['output_url'])
        self.assertEqual(str(inst.executor).lower(),
                         instdict['executor'].lower())
        self.assertEqual(inst.reason_mea_culpa, instdict['reason_mea_culpa'])

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
