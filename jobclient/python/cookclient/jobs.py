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
from datetime import datetime, timedelta
from enum import Enum
from uuid import UUID
from typing import Dict, List, Optional, Set

from .containers import AbstractContainer
from .instance import Executor, Instance
from .util import (
    clamped_ms_to_timedelta,
    unix_ms_to_datetime,
    datetime_to_unix_ms
)


class Status(Enum):
    """
    The curent status of a job.
    """
    WAITING = 'WAITING'
    """The job is currently waiting to begin."""
    RUNNING = 'RUNNING'
    """The job is currently running."""
    COMPLETED = 'COMPLETED'
    """The job has finished running."""

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Status.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'Status':
        """Parse a ``Status`` from a case-invariant string representation."""
        return _JOB_STATUS_LOOKUP[name.lower()]


_JOB_STATUS_LOOKUP = {
    'waiting': Status.WAITING,
    'running': Status.RUNNING,
    'completed': Status.COMPLETED
}


class State(Enum):
    """The current status of a job.

    Indicates whether a job is currently running, has finished and succeeded,
    or has finished and failed.
    """
    WAITING = 'WAITING'
    """The job has not finished yet."""
    RUNNING = 'RUNNING'
    """The job is currently running."""
    SUCCESS = 'SUCCESS'
    """The job has finished and succeeded."""
    FAILED = 'FAILED'
    """The job has finished and failed."""

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'State.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'State':
        """Parse a ``State`` from a case-invariant string representation."""
        return _JOB_STATE_LOOKUP[name.lower()]


_JOB_STATE_LOOKUP = {
    'waiting': State.WAITING,
    'success': State.SUCCESS,
    'running': State.RUNNING,
    'failed': State.FAILED
}


class Application:
    """Application information associated with a job.

    :param name: Name of the application.
    :type name: str
    :param version: Application version.
    :type version: str
    """
    name: str
    version: str
    workload_class: str
    workload_id: str
    workload_details: str

    def __init__(self, name: str, version: str, **kwargs):
        self.name = name
        self.version = version
        self.workload_class = kwargs.get('workload-class', None)
        self.workload_id = kwargs.get('workload-id', None)
        self.workload_details = kwargs.get('workload-details', None)

    def __str__(self):
        return json.dumps(self.to_dict, indent=4)

    def __repr__(self):
        return f'Application({self.name}, {self.version}, {self.workload_class}, ' \
               f'{self.workload_id}, {self.workload_details}) '

    def to_dict(self) -> dict:
        d = {'name': self.name, 'version': self.version}
        if self.workload_class:
            d['workload-class'] = self.workload_class
        if self.workload_id:
            d['workload-id'] = self.workload_id
        if self.workload_details:
            d['workload-details'] = self.workload_details
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Application':
        """Parse an ``Application`` from its dict representation."""
        return cls(**d)

class Disk:
    """Disk information associated with a job.

    :param request: Disk request - guaranteed disk space for job.
    :type name: float
    :param limit: Disk limit - max disk space job can use.
    :type limit: float
    :param type: Disk type.
    :type type: str
    """
    request: float
    limit: float
    type: str

    def __init__(self, request: float, limit: float = None, type: str = None):
        self.request = request
        self.limit = limit
        self.type = type

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    def __repr__(self):
        return f'Disk({self.request}, {self.limit}, {self.type})'

    def to_dict(self) -> dict:
        disk_dict = {}
        if self.request is not None:
            disk_dict['request'] = self.request
        if self.limit is not None:
            disk_dict['limit'] = self.limit
        if self.type is not None:
            disk_dict['type'] = self.type
        return disk_dict

    @classmethod
    def from_dict(cls, d: dict) -> 'Disk':
        """Parse a ``Disk`` from its dict representation."""
        return cls(**d)


class Job:
    """A job object returned from Cook.

    Normally, this class wouldn't be instantiated directly. It is instead
    used by ``JobClient`` when fetching a job information from ``query``.

    :param command: The command-line string that this job will execute.
    :type command: str
    :param mem: The amount of memory, in MB, to request from the scheduler.
    :type mem: float
    :param cpus: The number of CPUs to request from Cook.
    :type cpus: float
    :param uuid: The UUID of the job.
    :type uuid: UUID
    :param name: The name of the job, defaults to 'cookjob'
    :type name: str
    :param max_retries: The maximum number of times to attempt this job.
    :type max_retries: int
    :param max_runtime: The maximum time that this job may run.
    :type max_runtime: timedelta
    :param status: The current status of the job.
    :type status: cookclient.jobs.Status
    :param state: The current state of the job.
    :type state: State
    :param priority: The current priority of the job, defaults to 50
    :type priority: int
    :param framework_id: Framework ID for this job.
    :type framework_id: str
    :param retries_remaining: The number of retries remaining for this job.
    :type retries_remaining: int
    :param submit_time: The point in time when this job was submitted.
    :type submit_time: datetime
    :param user: User running this job.
    :type user: str

    :param executor: Executor information about the job.
    :type executor: Executor, optional
    :param container: Cotnainer description for this job.
    :type container: dict, optional
    :param disable_mea_culpa_retries: If true, then the job will not issue
        *mea culpa* retries.
    :type disable_mea_culpa_retries: bool, optional
    :param expected_runtime: This job's expected runtime.
    :type expected_runtime: timedelta, optional
    :param pool: The pool that this job should be deployed to.
    :type pool: str, optional
    :param instances: A list of instances for this job.
    :type instances: List[Instance], optional
    :param env: A mapping of environment variables that should be set
        before running this job.
    :type env: Dict[str, str], optional
    :param uris: A list of URIs associated with this job.
    :type uris: List[str], optional
    :param labels: Labels associated with this job.
    :type labels: Dict[str, str], optional
    :param constraints: Constraints for this job.
    :type constraints: Set[list], optional
    :param group: Group associated with this job.
    :type group: UUID, optional
    :param application: Application information for this job.
    :type application: Application, optional
    :param progress_output_file: Path to the file where this job will
        output its progress.
    :type progress_output_file: str, optional
    :param progress_regex_string: Progress regex.
    :type progress_regex_string: str, optional
    :param gpus: The number of GPUs to request from Cook.
    :type gpus: int, optional
    :param ports: Ports for this job.
    :type ports: int, optional
    :param disk: Disk information for this job.
    :type disk: Disk, optional
    """
    command: str
    mem: float
    cpus: float
    uuid: UUID
    name: str
    max_retries: int
    max_runtime: timedelta
    status: Status
    state: State
    priority: int
    framework_id: str
    retries_remaining: int
    submit_time: datetime
    user: str
    executor: Optional[Executor]
    container: Optional[AbstractContainer]
    disable_mea_culpa_retries: Optional[bool]
    expected_runtime: Optional[timedelta]
    pool: Optional[str]
    instances: Optional[List[Instance]]
    env: Optional[Dict[str, str]]
    uris: Optional[List[str]]
    labels: Optional[Dict[str, str]]
    constraints: Optional[Set[list]]
    group: Optional[UUID]
    application: Optional[Application]
    progress_output_file: Optional[str]
    progress_regex_string: Optional[str]
    gpus: Optional[int]
    ports: Optional[int]
    disk = Optional[Disk]

    etc: dict

    def __init__(self, *,
                 # Required args
                 command: str,
                 mem: float,
                 cpus: float,
                 uuid: UUID,
                 name: str,
                 max_retries: int,
                 max_runtime: timedelta,
                 status: Status,
                 state: State,
                 priority: int,
                 framework_id: str,
                 retries_remaining: int,
                 submit_time: datetime,
                 user: str,
                 # Optional keys
                 executor: Optional[Executor] = None,
                 container: Optional[AbstractContainer] = None,
                 disable_mea_culpa_retries: Optional[bool] = None,
                 expected_runtime: Optional[timedelta] = None,
                 pool: Optional[str] = None,
                 instances: Optional[List[Instance]] = None,
                 env: Optional[Dict[str, str]] = None,
                 uris: Optional[List[str]] = None,
                 labels: Optional[Dict[str, str]] = None,
                 constraints: Optional[Set[list]] = None,
                 group: Optional[UUID] = None,
                 application: Optional[Application] = None,
                 progress_output_file: Optional[str] = None,
                 progress_regex_string: Optional[str] = None,
                 gpus: Optional[int] = None,
                 ports: Optional[int] = None,
                 disk: Optional[Disk] = None,
                 # Extra not-officially-supported params
                 **kwargs):
        """Initializes a job object.

        Normally, this function wouldn't be invoked directly. It is instead
        used by ``JobClient`` when getting the data from ``query``.
        """
        self.command = command
        self.mem = mem
        self.cpus = cpus
        self.uuid = uuid
        self.name = name
        self.max_retries = max_retries
        self.max_runtime = max_runtime
        self.status = status
        self.state = state
        self.priority = priority
        self.framework_id = framework_id
        self.retries_remaining = retries_remaining
        self.submit_time = submit_time
        self.user = user
        self.executor = executor
        self.container = container
        self.disable_mea_culpa_retries = disable_mea_culpa_retries
        self.expected_runtime = expected_runtime
        self.pool = pool
        self.instances = instances
        self.env = env
        self.uris = uris
        self.labels = labels
        self.constraints = constraints
        self.group = group
        self.application = application
        self.progress_output_file = progress_output_file
        self.progress_regex_string = progress_regex_string
        self.gpus = gpus
        self.ports = ports
        self.disk = disk
        self.etc = kwargs

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    def __repr__(self):
        inner = ', '.join(
            f'{key}={repr(value)}'
            for key, value in self.__dict__.items()
        )
        return f'Job({inner})'

    def to_dict(self) -> dict:
        """Generate this job's `dict` representation."""
        d = {
            'command': self.command,
            'mem': self.mem,
            'cpus': self.cpus,
            'uuid': str(self.uuid),
            'name': self.name,
            'max_retries': self.max_retries,
            # Convert s to ms
            'max_runtime': int(self.max_runtime.total_seconds() * 1000),
            'status': str(self.status),
            'state': str(self.state),
            'priority': self.priority,
            'framework_id': self.framework_id,
            'retries_remaining': self.retries_remaining,
            'submit_time': datetime_to_unix_ms(self.submit_time),
            'user': self.user,
            **self.etc
        }
        if self.disable_mea_culpa_retries is not None:
            d['disable_mea_culpa_retries'] = self.disable_mea_culpa_retries
        if self.executor is not None:
            d['executor'] = str(self.executor)
        if self.container is not None:
            d['container'] = self.container.to_dict()
        if self.expected_runtime is not None:
            d['expected_runtime'] = int(self.expected_runtime.total_seconds() * 1000)  # noqa: E501
        if self.pool is not None:
            d['pool'] = self.pool
        if self.instances is not None:
            d['instances'] = list(map(Instance.to_dict, self.instances))
        if self.env is not None:
            d['env'] = self.env
        if self.uris is not None:
            d['uris'] = [{'value': uri} for uri in self.uris]
        if self.labels is not None:
            d['labels'] = self.labels
        if self.constraints is not None:
            d['constraints'] = self.constraints
        if self.group is not None:
            d['groups'] = [str(self.group)]
        if self.application is not None:
            d['application'] = self.application.to_dict()
        if self.progress_output_file is not None:
            d['progress_output_file'] = self.progress_output_file
        if self.progress_regex_string is not None:
            d['progress_regex_string'] = self.progress_regex_string
        if self.user is not None:
            d['user'] = self.user
        if self.gpus is not None:
            d['gpus'] = self.gpus
        if self.framework_id is not None:
            d['framework_id'] = self.framework_id
        if self.ports is not None:
            d['ports'] = self.ports
        if self.retries_remaining is not None:
            d['retries_remaining'] = self.retries_remaining
        if self.disk is not None:
            d['disk'] = self.disk.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Job':
        """Parse a Job from its `dict` representation."""
        d = deepcopy(d)
        d['uuid'] = UUID(d['uuid'])
        d['max_runtime'] = clamped_ms_to_timedelta(d['max_runtime'])
        d['status'] = Status.from_string(d['status'])
        d['state'] = State.from_string(d['state'])
        d['submit_time'] = unix_ms_to_datetime(d['submit_time'])

        if 'executor' in d:
            d['executor'] = Executor.from_string(d['executor'])
        if 'container' in d:
            d['container'] = AbstractContainer.from_dict(d['container'])
        if 'expected_runtime' in d:
            d['expected_runtime'] = clamped_ms_to_timedelta(d['expected_runtime'])  # noqa: E501
        if 'instances' in d:
            d['instances'] = list(map(Instance.from_dict, d['instances']))
        if 'uris' in d:
            d['uris'] = [uri['value'] for uri in d['uris']]
        if 'groups' in d and len(d['groups']) > 0:
            d['group'] = d['groups'][0]
        if 'application' in d:
            d['application'] = Application.from_dict(d['application'])
        if 'disk' in d:
            d['disk'] = Disk.from_dict(d['disk'])

        # There's no 'groups' field in the Job constructor, so remove that key
        # from the dict.
        if 'groups' in d:
            del d['groups']
        return cls(**d)
