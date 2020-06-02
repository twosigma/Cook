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
from datetime import datetime, timedelta
from enum import Enum
from uuid import UUID
from typing import Dict, List, Optional, Set

from . import constraints
from .constraints import Constraint
from .instance import Executor, Instance
from .util import FetchableUri


class Status(Enum):
    """
    The curent status of a job.
    """
    WAITING = 'WAITING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Status.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'Status':
        """Parse a `Status` from a case-invariant string representation."""
        return _JOB_STATUS_LOOKUP[name.lower()]


_JOB_STATUS_LOOKUP = {
    'waiting': Status.WAITING,
    'running': Status.RUNNING,
    'completed': Status.COMPLETED
}


class State(Enum):
    """The current state of a job.

    Indicates whether a job is currently running, has finished and succeeded,
    or has finished and failed.
    """
    WAITING = 'WAITING'
    PASSED = 'PASSED'
    FAILED = 'FAILED'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'State.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'State':
        """Parse a `State` from a case-invariant string representation."""
        return _JOB_STATE_LOOKUP[name.lower()]


_JOB_STATE_LOOKUP = {
    'waiting': State.WAITING,
    'passed': State.PASSED,
    'failed': State.FAILED
}


class Application:
    name: str
    version: str

    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'version': self.version
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'Application':
        return cls(**d)


class Job:
    """A job object returned from Cook."""
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
    container: Optional[dict]
    disable_mea_culpa_retries: Optional[bool]
    expected_runtime: Optional[timedelta]
    pool: Optional[str]
    instances: Optional[List[Instance]]
    env: Optional[Dict[str, str]]
    uris: Optional[List[FetchableUri]]
    labels: Optional[Dict[str, str]]
    constraints: Optional[Set[Constraint]]
    group: Optional[UUID]
    groups: Optional[List[UUID]]
    application: Optional[Application]
    progress_output_file: Optional[str]
    progress_regex_string: Optional[str]
    datasets: Optional[dict]
    gpus: Optional[int]
    ports: Optional[int]

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
                 container: Optional[dict] = None,
                 disable_mea_culpa_retries: Optional[bool] = None,
                 expected_runtime: Optional[timedelta] = None,
                 pool: Optional[str] = None,
                 instances: Optional[List[Instance]] = None,
                 env: Optional[Dict[str, str]] = None,
                 uris: Optional[List[FetchableUri]] = None,
                 labels: Optional[Dict[str, str]] = None,
                 constraints: Optional[Set[Constraint]] = None,
                 group: Optional[UUID] = None,
                 groups: Optional[List[UUID]] = None,
                 application: Optional[Application] = None,
                 progress_output_file: Optional[str] = None,
                 progress_regex_string: Optional[str] = None,
                 datasets: Optional[dict] = None,
                 gpus: Optional[int] = None,
                 ports: Optional[int] = None):
        """Initializes a job object.

        Normally, this function wouldn't be invoked directly. It is instead
        used by `JobClient` when getting the data from `query`.

        Required Parameters
        ----------
        :param command: The command-line string that this job will execute.
        :type command: str
        :param mem: The amount of memory, in MB, to request from the scheduler.
        :type mem: float
        :param cpus: The number of CPUs to request from Cook.
        :type cpus: float
        :param name: The name of the job, defaults to 'cookjob'
        :type name: str
        :param max_retries: The maximum number of times to attempt this job.
        :type max_retries: int
        :param max_runtime: The maximum time that this job may run.
        :type max_runtime: timedelta
        :param status: The current status of the job.
        :type status: Status
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

        Optional Parameters
        -------------------
        :param disable_mea_culpa_retries: If true, then the job will not issue
            *mea culpa* retries.
        :type disable_mea_culpa_retries: bool
        :param expected_runtime: This job's expected runtime.
        :type expected_runtime: timedelta
        :param pool: The pool that this job should be deployed to.
        :type pool: str
        :param instances: A list of instances for this job.
        :type instances: List[Instance]
        :param env: A mapping of environment variables that should be set
            before running this job.
        :type env: Dict[str, str]
        :param uris: A list of URIs associated with this job.
        :type uris: List[FetchableUri]
        :param container: Cotnainer description for this job.
        :type container: dict
        :param labels: Labels associated with this job.
        :type labels: Dict[str, str]
        :param constraints: Constraints for this job.
        :type constraints: Set[Constraint]
        :param groups: Group associated with this job.
        :type groups: List[UUID]
        :param groups: Groups associated with this job.
        :type groups: List[UUID]
        :param application: Application information for this job.
        :type application: Application
        :param progress_output_file: Path to the file where this job will
            output its progress.
        :type progress_output_file: str
        :param progress_regex_string: Progress regex.
        :type progress_regex_string: str
        :param datasets: Datasets associated with this job.
        :type datasets: list
        :param gpus: The number of GPUs to request from Cook.
        :type gpus: int
        :param ports: Ports for this job.
        :type ports: int
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
        self.groups = groups
        self.application = application
        self.progress_output_file = progress_output_file
        self.progress_regex_string = progress_regex_string
        self.datasets = datasets
        self.gpus = gpus
        self.ports = ports

    def to_dict(self) -> dict:
        """Generate this job's `dict` representation."""
        d = {
            'command': self.command,
            'mem': self.mem,
            'cpus': self.cpus,
            'uuid': str(self.uuid),
            'name': self.name,
            'max_retries': self.max_retries,
            'max_runtime': int(self.max_runtime.total_seconds()),
            'status': str(self.status),
            'state': str(self.state),
            'priority': self.priority,
            'framework_id': self.framework_id,
            'retries_remaining': self.retries_remaining,
            'submit_time': int(self.submit_time.timestamp()),
            'user': self.user
        }
        if self.disable_mea_culpa_retries is not None:
            d['disable_mea_culpa_retries'] = self.disable_mea_culpa_retries
        if self.executor is not None:
            d['executor'] = str(self.executor)
        if self.expected_runtime is not None:
            d['expected_runtime'] = int(self.expected_runtime.total_seconds())
        if self.pool is not None:
            d['pool'] = self.pool
        if self.instances is not None:
            d['instances'] = list(map(str, self.instances))
        if self.env is not None:
            d['env'] = self.env
        if self.uris is not None:
            d['uris'] = list(map(FetchableUri.to_dict, self.uris))
        if self.container is not None:
            d['container'] = self.container
        if self.labels is not None:
            d['labels'] = self.labels
        if self.constraints is not None:
            d['constraints'] = list(map(lambda c: c.to_list, self.constraints))
        if self.groups is not None:
            d['groups'] = list(map(str, self.groups))
        if self.application is not None:
            d['application'] = self.application.to_dict()
        if self.progress_output_file is not None:
            d['progress_output_file'] = self.progress_output_file
        if self.progress_regex_string is not None:
            d['progress_regex_string'] = self.progress_regex_string
        if self.user is not None:
            d['user'] = self.user
        if self.datasets is not None:
            d['datasets'] = self.datasets
        if self.gpus is not None:
            d['gpus'] = self.gpus
        if self.framework_id is not None:
            d['framework_id'] = self.framework_id
        if self.ports is not None:
            d['ports'] = self.ports
        if self.retries_remaining is not None:
            d['retries_remaining'] = self.retries_remaining
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Job':
        """Parse a Job from its `dict` representation."""
        d = deepcopy(d)
        d['uuid'] = UUID(d['uuid'])
        d['max_runtime'] = timedelta(seconds=d['max_runtime'])
        d['status'] = Status.from_string(d['status'])
        d['state'] = State.from_string(d['state'])
        d['submit_time'] = datetime.fromtimestamp(d['submit_time'] / 1000)

        if 'executor' in d:
            d['executor'] = Executor.from_string(d['executor'])
        if 'expected_runtime' in d:
            d['expected_runtime'] = timedelta(seconds=d['expected_runtime'])
        if 'instances' in d:
            d['instances'] = list(map(Instance.from_dict, d['instances']))
        if 'uris' in d:
            d['uris'] = list(map(FetchableUri.from_dict, d['uris']))
        if 'constraints' in d:
            d['constraints'] = set(map(constraints.parse_from, d['constraints']))  # noqa E501
        if 'group' in d:
            d['group'] = UUID(d['group'])
        if 'groups' in d:
            d['groups'] = list(map(UUID, d['groups']))
        if 'application' in d:
            d['application'] = Application.from_dict(d['application'])
        return cls(**d)
