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

from copy import deepcopy
from enum import Enum
from typing import Optional
from uuid import UUID


class Status(Enum):
    """A status for some instance in Cook."""
    UNKNOWN = 'UNKNOWN'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Status.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'Status':
        """Create a status from a case-insensitive string representation."""
        return _INSTANCE_STATUS_LOOKUP[name.lower()]


_INSTANCE_STATUS_LOOKUP = {
    'unknown': Status.UNKNOWN,
    'running': Status.RUNNING,
    'success': Status.SUCCESS,
    'failed': Status.FAILED
}


class Executor(Enum):
    """Indicates where an instance is running."""
    COOK = 'COOK'
    EXECUTOR = 'EXECUTOR'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Executor.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'Executor':
        """Create an executor from a case-insensitive string representation."""
        return _EXECUTOR_LOOKUP[name.lower()]


_EXECUTOR_LOOKUP = {
    'cook': Executor.COOK,
    'executor': Executor.EXECUTOR,
}


class Instance:
    """An instance of a job in Cook."""
    task_id: UUID
    slave_id: str
    executor_id: str
    start_time: int
    hostname: str
    status: Status
    preempted: bool

    end_time: Optional[int]
    progress: Optional[int]
    progress_message: Optional[str]
    reason_code: Optional[int]
    output_url: Optional[str]
    executor: Optional[Executor]
    reason_mea_culpa: Optional[bool]

    def __init__(self, *,
                 # Required arguments
                 task_id: UUID,
                 slave_id: str,
                 executor_id: str,
                 start_time: int,
                 hostname: str,
                 status: Status,
                 preempted: bool,
                 # Optional arguments
                 end_time: Optional[int] = None,
                 progress: Optional[int] = None,
                 progress_message: Optional[str] = None,
                 reason_code: Optional[int] = None,
                 output_url: Optional[str] = None,
                 executor: Optional[Executor] = None,
                 reason_mea_culpa: Optional[bool] = None):
        """Initialize an instance.

        Required Parameters
        -------------------
        :param slave_id: The ID of the slave running this instance.
        :type slave_id: str
        :param executor_id: The ID of the executor running this instance.
        :type executor_id: str
        :param start_time: The UNIX timestamp at which this instance began
            running.
        :type start_time: int
        :param hostname: The hostname of the machine running this instance.
        :type hostname: str
        :param status: The status of this instance.
        :type status: Status
        :param preempted: If true, then this instance was preempted.
        :type preempted: bool
        Optional Parameters
        -------------------
        :param progress: Progress of this instance.
        :type progress: Optional[int], optional
        :param progress_message: Progress message of this instance.
        :type progress_message: Optional[str], optional
        :param reason_code: Reason code of this instance.
        :type reason_code: Optional[int], optional
        :param output_url: Output URL for this instance.
        :type output_url: Optional[str], optional
        :param executor: Executor information for this instance.
        :type executor: Optional[Executor], optional
        :param reason_mea_culpa: If true, then reason mea culpa.
        :type reason_mea_culpa: Optional[bool], optional
        """
        self.task_id = task_id
        self.slave_id = slave_id
        self.executor_id = executor_id
        self.start_time = start_time
        self.hostname = hostname
        self.status = status
        self.preempted = preempted
        self.end_time = end_time
        self.progress = progress
        self.progress_message = progress_message
        self.reason_code = reason_code
        self.output_url = output_url
        self.executor = executor
        self.reason_mea_culpa = reason_mea_culpa

    def __hash__(self):
        PRIME = 31
        result = 1
        result = (PRIME * result +
                  hash(self.status) if self.status is not None else 0)
        result = (PRIME * result +
                  hash(self.task_id) if self.task_id is not None else 0)
        return result

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, self.__class__):
            return False
        if self.status != other.status:
            return False
        if self.task_id != other.task_id:
            return False
        return True

    def to_dict(self) -> dict:
        """Generate this instance's `dict` representation."""
        d = {
            'task_id': str(self.uuid),
            'slave_id': self.slave_id,
            'executor_id': self.executor_id,
            'start_time': self.start_time,
            'hostname': self.hostname,
            'status': str(self.status),
            'preempted': self.preempted
        }
        if self.end_time is not None:
            d['end_time'] = self.end_time
        if self.progress is not None:
            d['progress'] = self.progress
        if self.progress_message is not None:
            d['progress_message'] = self.progress_message
        if self.reason_code is not None:
            d['reason_code'] = self.reason_code
        if self.output_url is not None:
            d['output_url'] = self.output_url
        if self.executor is not None:
            d['executor'] = self.executor
        if self.reason_mea_culpa is not None:
            d['reason_mea_culpa'] = self.reason_mea_culpa
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Instance':
        """Create an instance from its `dict` representation."""
        d = deepcopy(d)
        d['uuid'] = UUID(d['uuid'])
        d['status'] = Status.from_string(d['status'])
        if 'executor' in d:
            d['executor'] = Executor.from_string(d['executor'])
        return cls(**d)
