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
    command: str
    memory: float
    cpus: float

    uuid: UUID
    name: str
    retries: int
    max_runtime: int
    status: Status
    priority: int
    is_mea_culpa_retries_disabled: bool

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

    def __init__(self, *,
                 # Required args
                 command: str,
                 memory: float,
                 cpus: float,
                 # Optional args with defaults
                 uuid: Optional[UUID] = None,
                 name: str = 'cookjob',
                 retries: int = 5,
                 max_runtime: int = sys.maxsize,
                 status: Status = Status.INITIALIZED,
                 priority: int = 50,
                 is_mea_culpa_retries_disabled: bool = False,
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
                 datasets: Optional[list] = None):
        self.command = command
        self.memory = memory
        self.cpus = cpus
        self.uuid = uuid if uuid is not None else util.make_temporal_uuid()
        self.name = name
        self.retries = retries
        self.max_runtime = max_runtime
        self.status = status
        self.priority = priority
        self.is_mea_culpa_retries_disabled = is_mea_culpa_retries_disabled
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
        d = {
            'command': self.command,
            'memory': self.memory,
            'cpus': self.cpus,
            'uuid': self.uuid,
            'name': self.name,
            'retries': self.retries,
            'max_runtime': self.max_runtime,
            'status': self.status,
            'priority': self.priority,
            'is_mea_culpa_retries_disabled': self.is_mea_culpa_retries_disabled
        }
        if self.executor is not None:
            d['executor'] = self.executor
        if self.expected_runtime is not None:
            d['expected_runtime'] = self.expected_runtime
        if self.pool is not None:
            d['pool'] = self.pool
        if self.instances is not None:
            d['instances'] = self.instances
        if self.env is not None:
            d['env'] = self.env
        if self.uris is not None:
            d['uris'] = self.uris
        if self.container is not None:
            d['container'] = self.container
        if self.labels is not None:
            d['labels'] = self.labels
        if self.constraints is not None:
            d['constraints'] = self.constraints
        if self.groups is not None:
            d['groups'] = self.groups
        if self.application is not None:
            d['application'] = self.application
        if self.progress_output_file is not None:
            d['progress_output_file'] = self.progress_output_file
        if self.progress_regex_string is not None:
            d['progress_regex_string'] = self.progress_regex_string
        if self.user is not None:
            d['user'] = self.user
        if self.datasets is not None:
            d['datasets'] = self.datasets
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Job':
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
