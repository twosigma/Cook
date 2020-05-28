import sys

import util

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
    __name: str
    __version: str

    def __init__(self, name, version):
        self.__name = name
        self.__version = version

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'version': self.version
        }

    @property
    def name(self) -> str:
        return self.__name

    @property
    def version(self) -> str:
        return self.__version


class Job:
    __command: str
    __memory: float
    __cpus: float

    __uuid: UUID = util.make_temporal_uuid()
    __name: str = 'cookjob'
    __retries: int = 5
    __max_runtime: int = sys.maxsize
    __status: Status = Status.INITIALIZED
    __priority: int = 50
    __is_mea_culpa_retries_disabled: bool = False

    __executor: Optional[Executor]
    __expected_runtime: Optional[int]
    __pool: Optional[str]
    __instances: Optional[List[Instance]]
    __env: Optional[Dict[str, str]]
    __uris: Optional[List[FetchableUri]]
    __container: Optional[dict]
    __labels: Optional[Dict[str, str]]
    __constraints: Optional[Set[Constraint]]
    __groups: Optional[List[UUID]]
    __application: Optional[Application]
    __progress_output_file: Optional[str]
    __progress_regex_string: Optional[str]
    __user: Optional[str]
    __datasets: Optional[list]

    def __init__(self, *,
                 command: str,
                 memory: str,
                 cpus: str,
                 **kwargs):
        # Required args
        self.__command = command
        self.__memory = memory
        self.__cpus = cpus
        # Optional args with defaults
        if 'uuid' in kwargs:
            self.__uuid = kwargs['uuid']
        if 'name' in kwargs:
            self.__name = kwargs['name']
        if 'executor' in kwargs:
            self.__executor = kwargs['executor']
        if 'retries' in kwargs:
            self.__retries = kwargs['retries']
        if 'max_runtime' in kwargs:
            self.__max_runtime = kwargs['max_runtime']
        if 'status' in kwargs:
            self.__status = kwargs['status']
        if 'priority' in kwargs:
            self.__priority = kwargs['priority']
        if 'is_mea_culpa_retries_disabled' in kwargs:
            self.__is_mea_culpa_retries_disabled = kwargs['is_mea_culpa_retries_disabled'] # noqa 501
        # Optional args without defaults
        if 'expected_runtime' in kwargs:
            self.__expected_runtime = kwargs['expected_runtime']
        if 'pool' in kwargs:
            self.__pool = kwargs['pool']
        if 'instances' in kwargs:
            self.__instances = kwargs['instances']
        if 'env' in kwargs:
            self.__env = kwargs['env']
        if 'uris' in kwargs:
            self.__uris = kwargs['uris']
        if 'container' in kwargs:
            self.__container = kwargs['container']
        if 'labels' in kwargs:
            self.__labels = kwargs['labels']
        if 'constraints' in kwargs:
            self.__constraints = kwargs['constraints']
        if 'groups' in kwargs:
            self.__groups = kwargs['groups']
        if 'application' in kwargs:
            self.__application = kwargs['application']
        if 'progress_output_file' in kwargs:
            self.__progress_output_file = kwargs['progress_output_file']
        if 'progress_regex_string' in kwargs:
            self.__progress_regex_string = kwargs['progress_regex_string']
        if 'user' in kwargs:
            self.__user = kwargs['user']
        if 'datasets' in kwargs:
            self.__datasets = kwargs['datasets']

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

    @property
    def command(self) -> str:
        return self.__command

    @property
    def memory(self) -> float:
        return self.__memory

    @property
    def cpus(self) -> float:
        return self.__cpus

    @property
    def uuid(self) -> UUID:
        return self.__uuid

    @property
    def name(self) -> str:
        return self.__name

    @property
    def retries(self) -> int:
        return self.__retries

    @property
    def max_runtime(self) -> int:
        return self.__max_runtime

    @property
    def status(self) -> Status:
        return self.__status

    @property
    def priority(self) -> int:
        return self.__priority

    @property
    def is_mea_culpa_retries_disabled(self) -> bool:
        return self.__is_mea_culpa_retries_disabled

    @property
    def executor(self) -> Optional[Executor]:
        return self.__executor

    @property
    def expected_runtime(self) -> Optional[int]:
        return self.__expected_runtime

    @property
    def pool(self) -> Optional[str]:
        return self.__pool

    @property
    def instances(self) -> Optional[List[Instance]]:
        return self.__instances

    @property
    def env(self) -> Optional[Dict[str, str]]:
        return self.__env

    @property
    def uris(self) -> Optional[List[FetchableUri]]:
        return self.__uris

    @property
    def container(self) -> Optional[dict]:
        return self.__container

    @property
    def labels(self) -> Optional[Dict[str, str]]:
        return self.__labels

    @property
    def constraints(self) -> Optional[Set[Constraint]]:
        return self.__constraints

    @property
    def groups(self) -> Optional[List[UUID]]:
        return self.__groups

    @property
    def application(self) -> Optional[Application]:
        return self.__application

    @property
    def progress_output_file(self) -> Optional[str]:
        return self.__progress_output_file

    @property
    def progress_regex_string(self) -> Optional[str]:
        return self.__progress_regex_string

    @property
    def user(self) -> Optional[str]:
        return self.__user

    @property
    def datasets(self) -> Optional[list]:
        return self.__datasets
