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
        return _INSTANCE_STATUS_LOOKUP[name.lower()]


_INSTANCE_STATUS_LOOKUP = {
    'unknown': Status.UNKNOWN,
    'running': Status.RUNNING,
    'success': Status.SUCCESS,
    'failed': Status.FAILED
}


class Executor(Enum):
    COOK = 'COOK'
    EXECUTOR = 'EXECUTOR'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Executor.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'Executor':
        return _EXECUTOR_LOOKUP[name.lower()]


_EXECUTOR_LOOKUP = {
    'COOK': Executor.COOK,
    'EXECUTOR': Executor.EXECUTOR,
}


class Instance:
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

    @classmethod
    def from_dict(cls, d: dict) -> 'Instance':
        d = deepcopy(d)
        d['uuid'] = UUID(d['uuid'])
        d['status'] = Status.from_string(d['status'])
        if 'executor' in d:
            d['executor'] = Executor.from_string(d['executor'])
        return cls(**d)
