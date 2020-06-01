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
from typing import Dict, List, Set

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
    max_runtime: int
    status: Status
    state: State
    priority: int
    disable_mea_culpa_retries: bool
    executor: Executor
    expected_runtime: timedelta
    pool: str
    instances: List[Instance]
    env: Dict[str, str]
    uris: List[FetchableUri]
    container: dict
    labels: Dict[str, str]
    constraints: Set[Constraint]
    groups: List[UUID]
    application: Application
    progress_output_file: str
    progress_regex_string: str
    user: str
    datasets: list
    gpus: int
    framework_id: str
    ports: int
    submit_time: datetime
    retries_remaining: int

    def __init__(self, *,
                 # Required args
                 command: str,
                 mem: float,
                 cpus: float,
                 uuid: UUID,
                 name: str,
                 max_retries: int,
                 max_runtime: timedelta,
                 state: State,
                 status: Status,
                 priority: int,
                 disable_mea_culpa_retries: bool,
                 executor: Executor,
                 expected_runtime: int,
                 pool: str,
                 instances: List[Instance],
                 env: Dict[str, str],
                 uris: List[FetchableUri],
                 container: dict,
                 labels: Dict[str, str],
                 constraints: Set[Constraint],
                 groups: List[UUID],
                 application: Application,
                 progress_output_file: str,
                 progress_regex_string: str,
                 user: str,
                 datasets: list,
                 gpus: float,
                 framework_id: str,
                 ports: int,
                 submit_time: int,
                 retries_remaining: int):
        """Initializes a job object.

        Normally, this function wouldn't be invoked directly. It is instead
        used by `JobClient` when getting the data from `query`.

        Parameters
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
        :param state: The current state of the job.
        :type state: State
        :param status: The current status of the job.
        :type status: Status
        :param priority: The current priority of the job, defaults to 50
        :type priority: int
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
        :param groups: Groups associated with this job.
        :type groups: List[UUID]
        :param application: Application information for this job.
        :type application: Application
        :param progress_output_file: Path to the file where this job will
            output its progress.
        :type progress_output_file: str
        :param progress_regex_string: Progress regex.
        :type progress_regex_string: str
        :param user: User running this job.
        :type user: str
        :param datasets: Datasets associated with this job.
        :type datasets: list
        :param gpus: The number of GPUs to request from Cook.
        :type gpus: int
        :param framework_id: Framework ID for this job.
        :type framework_id: str
        :param ports: Ports for this job.
        :type ports: int
        :param submit_time: The point in time when this job was submitted.
        :type submit_time: datetime
        :param retries_remaining: The number of retries remaining for this job.
        :type retries_remaining: int
        """
        self.command = command
        self.mem = mem
        self.cpus = cpus
        self.uuid = uuid
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

    def to_dict(self) -> dict:
        """Generate this job's `dict` representation."""
        return {
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
            'disable_mea_culpa_retries': self.disable_mea_culpa_retries,
            'executor': str(self.executor),
            'expected_runtime': int(self.expected_runtime.total_seconds()),
            'pool': self.pool,
            'instances': list(map(str, self.instances)),
            'env': self.env,
            'uris': list(map(FetchableUri.to_dict, self.uris)),
            'container': self.container,
            'labels': self.labels,
            'constraints': list(map(lambda c: c.to_list, self.constraints)),
            'groups': list(map(str, self.groups)),
            'application': self.application.to_dict(),
            'progress_output_file': self.progress_output_file,
            'progress_regex_string': self.progress_regex_string,
            'user': self.user,
            'datasets': self.datasets,
            'gpus': self.gpus,
            'framework_id': self.framework_id,
            'ports': self.ports,
            'submit_time': datetime.fromtimestamp(self.submit_time),
            'retries_remaining': self.retries_remaining,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'Job':
        """Parse a Job from its `dict` representation."""
        d = deepcopy(d)
        d['uuid'] = UUID(d['uuid'])
        d['status'] = Status.from_string(d['status'])
        d['state'] = State.from_string(d['state'])
        d['max_runtime'] = timedelta(seconds=d['max_runtime'])
        d['application'] = Application.from_dict(d['application'])
        d['uris'] = list(map(FetchableUri.from_dict, d['uris']))
        d['constraints'] = set(map(constraints.parse_from, d['constraints']))
        d['executor'] = Executor.from_string(d['executor'])
        d['instances'] = list(map(Instance.from_dict, d['instances']))
        d['expected_runtime'] = timedelta(seconds=d['expected_runtime'])
        d['submit_time'] = datetime.fromtimestamp(d['submit_time'])
        return cls(**d)
