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

import constraints
import util

from copy import deepcopy
from enum import Enum
from uuid import UUID
from typing import Dict, List, Optional, Set

from constraints import Constraint
from instance import Executor, Instance
from util import FetchableUri


class Status(Enum):
    """
    The curent status of a job.

    A job in Cook has only three statuses: WAITING, RUNNING, COMPLETED. The
    INITIALIZED status is for a `JobListener` to receive status updates.
    """
    INITIALIZED = 'INITIALIZED'
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
    'initialized': Status.INITIALIZED,
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
    max_runtime: int
    status: Status
    state: State
    priority: int
    disable_mea_culpa_retries: bool

    executor: Optional[Executor]
    expected_runtime: Optional[int]
    pool: Optional[str]
    instances: Optional[List[Instance]]
    env: Optional[Dict[str, str]]
    uris: Optional[List[FetchableUri]]
    container: Optional[dict]
    labels: Optional[Dict[str, str]]
    constraints: Optional[Set[Constraint]]
    groups: Optional[List[UUID]]
    application: Optional[Application]
    progress_output_file: Optional[str]
    progress_regex_string: Optional[str]
    user: Optional[str]
    datasets: Optional[list]
    gpus: Optional[float]
    framework_id: Optional[str]
    ports: Optional[int]
    submit_time: Optional[int]
    retries_remaining: Optional[int]

    def __init__(self, *,
                 # Required args
                 command: str,
                 mem: float,
                 cpus: float,
                 # Optional args with defaults
                 uuid: Optional[UUID] = None,
                 name: str = 'cookjob',
                 max_retries: int = 5,
                 max_runtime: int = sys.maxsize,
                 state: State = State.WAITING,
                 status: Status = Status.INITIALIZED,
                 priority: int = 50,
                 disable_mea_culpa_retries: bool = False,
                 # Optional args with null defaults
                 executor: Optional[Executor] = None,
                 expected_runtime: Optional[int] = None,
                 pool: Optional[str] = None,
                 instances: Optional[List[Instance]] = None,
                 env: Optional[Dict[str, str]] = None,
                 uris: Optional[List[FetchableUri]] = None,
                 container: Optional[dict] = None,
                 labels: Optional[Dict[str, str]] = None,
                 constraints: Optional[Set[Constraint]] = None,
                 groups: Optional[List[UUID]] = None,
                 application: Optional[Application] = None,
                 progress_output_file: Optional[str] = None,
                 progress_regex_string: Optional[str] = None,
                 user: Optional[str] = None,
                 datasets: Optional[list] = None,
                 gpus: Optional[float] = None,
                 framework_id: Optional[str] = None,
                 ports: Optional[int] = None,
                 submit_time: Optional[int] = None,
                 retries_remaining: Optional[int] = None):
        """Initializes a job object.

        Normally, this function wouldn't be invoked directly. It is instead
        used by `JobClient` when getting the data from `query`.

        Required Parameters
        -------------------
        :param command: The command-line string that this job will execute.
        :type command: str
        :param mem: The amount of memory, in GB, to request from the scheduler.
        :type mem: float
        :param cpus: The number of CPUs to request from Cook.
        :type cpus: float

        Parameters With Defaults
        ------------------------
        :param name: The name of the job, defaults to 'cookjob'
        :type name: str, optional
        :param max_retries: The maximum number of times to retry this job,
            defaults to 5
        :type max_retries: int, optional
        :param max_runtime: The maximum time, in seconds, that this job may
            run, defaults to sys.maxsize
        :type max_runtime: int, optional
        :param state: The current state of the job, defaults to State.WAITING
        :type state: State, optional
        :param status: The current status of the job, defaults to
            Status.INITIALIZED
        :type status: Status, optional
        :param priority: The current priority of the job, defaults to 50
        :type priority: int, optional
        :param disable_mea_culpa_retries: If true, then the job will not issue
            *mea culpa* retries, defaults to False
        :type disable_mea_culpa_retries: bool, optional

        Optional Parameters
        -------------------
        :param expected_runtime: This job's expected runtime, in seconds.
        :type expected_runtime: Optional[int], optional
        :param pool: The pool that this job should be deployed to.
        :type pool: Optional[str], optional
        :param instances: A list of instances for this job.
        :type instances: Optional[List[Instance]], optional
        :param env: A mapping of environment variables that should be set
            before running this job.
        :type env: Optional[Dict[str, str]], optional
        :param uris: A list of URIs associated with this job.
        :type uris: Optional[List[FetchableUri]], optional
        :param container: Cotnainer description for this job.
        :type container: Optional[dict], optional
        :param labels: Labels associated with this job.
        :type labels: Optional[Dict[str, str]], optional
        :param constraints: Constraints for this job.
        :type constraints: Optional[Set[Constraint]], optional
        :param groups: Groups associated with this job.
        :type groups: Optional[List[UUID]], optional
        :param application: Application information for this job.
        :type application: Optional[Application], optional
        :param progress_output_file: Path to the file where this job will
            output its progress.
        :type progress_output_file: Optional[str], optional
        :param progress_regex_string: Progress regex.
        :type progress_regex_string: Optional[str], optional
        :param user: User running this job.
        :type user: Optional[str], optional
        :param datasets: Datasets associated with this job.
        :type datasets: Optional[list], optional
        :param gpus: The number of GPUs to request from Cook.
        :type gpus: Optional[float], optional
        :param framework_id: Framework ID for this job.
        :type framework_id: Optional[str], optional
        :param ports: Ports for this job.
        :type ports: Optional[int], optional
        :param submit_time: The UNIX timestamp when this job was submitted.
        :type submit_time: Optional[int], optional
        :param retries_remaining: The number of retries remaining for this job.
        :type retries_remaining: Optional[int], optional
        """
        self.command = command
        self.mem = mem
        self.cpus = cpus
        self.uuid = uuid if uuid is not None else util.make_temporal_uuid()
        self.name = name
        self.max_retries = max_retries
        self.max_runtime = max_runtime
        self.state = state
        self.status = status
        self.priority = priority
        self.disable_mea_culpa_retries = disable_mea_culpa_retries
        self.executor = executor
        self.expected_runtime = expected_runtime
        self.pool = pool
        self.instances = instances
        self.env = env
        self.uris = uris
        self.container = container
        self.labels = labels
        self.constraints = constraints
        self.groups = groups
        self.application = application
        self.progress_output_file = progress_output_file
        self.progress_regex_string = progress_regex_string
        self.user = user
        self.datasets = datasets
        self.gpus = gpus
        self.framework_id = framework_id
        self.ports = ports
        self.submit_time = submit_time
        self.retries_remaining = retries_remaining

    def __hash__(self):
        PRIME = 31
        result = 1
        result = (PRIME * result
                  + hash(self.status) if self.status is not None else 0)
        result = (PRIME * result
                  + hash(self.uuid) if self.uuid is not None else 0)
        return result

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, self.__class__):
            return False
        return self.uuid == other.uuid and self.status == other.status

    def to_dict(self) -> dict:
        """Generate this job's `dict` representation."""
        d = {
            'command': self.command,
            'mem': self.mem,
            'cpus': self.cpus,
            'uuid': str(self.uuid),
            'name': self.name,
            'max_retries': self.max_retries,
            'max_runtime': self.max_runtime,
            'status': str(self.status),
            'state': str(self.state),
            'priority': self.priority,
            'disable_mea_culpa_retries': self.disable_mea_culpa_retries
        }
        if self.executor is not None:
            d['executor'] = str(self.executor)
        if self.expected_runtime is not None:
            d['expected_runtime'] = self.expected_runtime
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
        if self.submit_time is not None:
            d['submit_time'] = self.submit_time
        if self.retries_remaining is not None:
            d['retries_remaining'] = self.retries_remaining
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Job':
        """Parse a Job from its `dict` representation."""
        d = deepcopy(d)
        d['uuid'] = UUID(d['uuid'])
        d['status'] = Status.from_string(d['status'])
        if 'application' in d:
            d['application'] = Application.from_dict(d['application'])
        if 'uris' in d:
            d['uris'] = list(map(FetchableUri.from_dict, d['uris']))
        if 'constraints' in d:
            d['constraints'] = set(map(constraints.parse_from,
                                       d['constraints']))
        if 'executor' in d:
            d['executor'] = Executor.from_string(d['executor'])
        if 'instances' in d:
            d['instances'] = list(map(Instance.from_dict, d['instances']))
        return cls(**d)
