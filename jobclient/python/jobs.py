import sys

import util

from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class Application:
    name: str
    version: str

    def __init__(self, name, version):
        self.__name = name
        self.__version = version

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'version': self.version
        }


@dataclass(frozen=True)
class Job:
    command: str
    memory: float
    cpus: float

    uuid: UUID = field(default_factory=util.make_temporal_uuid)
    name: str = 'cookjob'
    retries: int = 5
    max_runtime: int = sys.maxsize
    status: Status = Status.INITIALIZED
    priority: int = 50
    is_mea_culpa_retries_disabled: bool = False

    executor: Optional[Executor] = None
    expected_runtime: Optional[int] = None
    pool: Optional[str] = None
    instances: Optional[List[Instance]] = None
    env: Optional[Dict[str, str]] = None
    uris: Optional[List[FetchableUri]] = None
    container: Optional[dict] = None
    labels: Optional[Dict[str, str]] = None
    constraints: Optional[Set[Constraint]] = None
    groups: Optional[List[UUID]] = None
    application: Optional[Application] = None
    progress_output_file: Optional[str] = None
    progress_regex_string: Optional[str] = None
    user: Optional[str] = None
    datasets: Optional[list] = None

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
