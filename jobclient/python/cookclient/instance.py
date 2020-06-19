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

import json

from copy import deepcopy
from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

from .util import unix_ms_to_datetime


class Status(Enum):
    """A status for some instance in Cook."""
    UNKNOWN = 'UNKNOWN'
    """The instance's status is unknown."""
    RUNNING = 'RUNNING'
    """The instance is currently running."""
    SUCCESS = 'SUCCESS'
    """The instance has finished and succeeded."""
    FAILED = 'FAILED'
    """The instance has finished and failed."""

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
    """The instance is running on a Cook executor."""
    MESOS = 'MESOS'
    """The instance is running on a Mesos executor."""

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
    'mesos': Executor.MESOS,
}


class Instance:
    """An instance of a job in Cook.

    Normally, this class wouldn't be instantiated directly. It is instead
    used by ``JobClient`` when fetching a job information from ``query``.

    :param slave_id: The ID of the slave running this instance.
    :type slave_id: str
    :param executor_id: The ID of the executor running this instance.
    :type executor_id: str
    :param start_time: The UNIX timestamp at which this instance began
        running.
    :type start_time: datetime
    :param hostname: The hostname of the machine running this instance.
    :type hostname: str
    :param status: The status of this instance.
    :type status: cookclient.instance.Status
    :param preempted: If true, then this instance was preempted.
    :type preempted: bool
    :param end_time: Time at which the instance finished.
    :type end_time: datetime, optional
    :param progress: Progress of this instance.
    :type progress: int, optional
    :param progress_message: Progress message of this instance.
    :type progress_message: str, optional
    :param reason_code: Reason code of this instance.
    :type reason_code: int, optional
    :param output_url: Output URL for this instance.
    :type output_url: str, optional
    :param executor: Executor information for this instance.
    :type executor: Executor, optional
    :param reason_mea_culpa: If true, then reason mea culpa.
    :type reason_mea_culpa: bool, optional
    """

    task_id: UUID
    slave_id: str
    executor_id: str
    hostname: str
    status: Status
    preempted: bool
    ports: List[int]
    compute_cluster: dict

    start_time: Optional[datetime]
    end_time: Optional[datetime]
    progress: Optional[int]
    progress_message: Optional[str]
    reason_code: Optional[int]
    reason_string: Optional[str]
    output_url: Optional[str]
    executor: Optional[Executor]
    reason_mea_culpa: Optional[bool]
    exit_code: Optional[int]

    etc: dict

    def __init__(self, *,
                 # Required arguments
                 task_id: UUID,
                 slave_id: str,
                 executor_id: str,
                 start_time: datetime,
                 hostname: str,
                 status: Status,
                 preempted: bool,
                 ports: List[int],
                 compute_cluster: dict,
                 # Optional arguments
                 end_time: Optional[datetime] = None,
                 progress: Optional[int] = None,
                 progress_message: Optional[str] = None,
                 reason_code: Optional[int] = None,
                 reason_string: Optional[str] = None,
                 output_url: Optional[str] = None,
                 executor: Optional[Executor] = None,
                 reason_mea_culpa: Optional[bool] = None,
                 exit_code: Optional[int] = None,
                 # Extra not-officially-supported params
                 **kwargs):
        """Initialize an instance."""
        self.task_id = task_id
        self.slave_id = slave_id
        self.executor_id = executor_id
        self.start_time = start_time
        self.hostname = hostname
        self.status = status
        self.preempted = preempted
        self.ports = ports
        self.compute_cluster = compute_cluster
        self.end_time = end_time
        self.progress = progress
        self.progress_message = progress_message
        self.reason_code = reason_code
        self.reason_string = reason_string
        self.output_url = output_url
        self.executor = executor
        self.reason_mea_culpa = reason_mea_culpa
        self.exit_code = exit_code
        self.etc = kwargs

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    def __repr__(self):
        inner = ', '.join(
            f'{key}={repr(value)}'
            for key, value in self.__dict__.items()
        )
        return f'Instance({inner})'

    def to_dict(self) -> dict:
        """Generate this instance's `dict` representation."""
        d = {
            'task_id': str(self.task_id),
            'slave_id': self.slave_id,
            'executor_id': self.executor_id,
            'start_time': int(self.start_time.timestamp() * 1000),
            'hostname': self.hostname,
            'status': str(self.status),
            'preempted': self.preempted,
            'ports': self.ports,
            'compute-cluster': self.compute_cluster,
            **self.etc
        }
        if self.end_time is not None:
            d['end_time'] = int(self.end_time.timestamp() * 1000)
        if self.progress is not None:
            d['progress'] = self.progress
        if self.progress_message is not None:
            d['progress_message'] = self.progress_message
        if self.reason_code is not None:
            d['reason_code'] = self.reason_code
        if self.reason_string is not None:
            d['reason_string'] = self.reason_string
        if self.output_url is not None:
            d['output_url'] = self.output_url
        if self.executor is not None:
            d['executor'] = str(self.executor)
        if self.reason_mea_culpa is not None:
            d['reason_mea_culpa'] = self.reason_mea_culpa
        if self.exit_code is not None:
            d['exit_code'] = self.exit_code
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Instance':
        """Create an instance from its `dict` representation."""
        d = deepcopy(d)
        d['task_id'] = UUID(d['task_id'])
        d['start_time'] = unix_ms_to_datetime(d['start_time'])
        d['status'] = Status.from_string(d['status'])
        # The JSON object uses the key `compute-cluster`, which isn't a valid
        # Python identifier, so we rename it to `compute_cluster` (note how the
        # hyphen is now an underscore)
        d['compute_cluster'] = d['compute-cluster']
        del d['compute-cluster']

        # `backfilled` is deprecated, but is still in the API output, so we
        # delete it from the dict.
        del d['backfilled']

        if 'end_time' in d:
            d['end_time'] = unix_ms_to_datetime(d['end_time'])
        if 'executor' in d:
            d['executor'] = Executor.from_string(d['executor'])
        return cls(**d)
