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
    __task_id: UUID
    __slave_id: str
    __executor_id: str
    __start_time: int
    __hostname: str
    __status: Status
    __preempted: bool

    __end_time: Optional[int]
    __progress: Optional[int]
    __progress_message: Optional[str]
    __reason_code: Optional[int]
    __output_url: Optional[str]
    __executor: Optional[Executor]
    __reason_mea_culpa: Optional[bool]

    def __init__(self, *,
                 task_id: str,
                 slave_id: str,
                 executor_id: str,
                 hostname: str,
                 status: str,
                 preempted: bool,
                 start_time: int,
                 **kwargs):
        # Required args
        self.__task_id = UUID(task_id)
        self.__slave_id = slave_id
        self.__executor_id = executor_id
        self.__hostname = hostname
        self.__status = Status.from_string(status)
        self.__preempted = preempted
        # Optional args
        if 'executor' in kwargs:
            self.__executor = Executor.from_string(kwargs['executor'])
        if 'progress' in kwargs:
            self.__progress = kwargs['progress']
        if 'progress_message' in kwargs:
            self.__progress_message = kwargs['progress_message']
        if 'end_time' in kwargs:
            self.__end_time = kwargs['end_time']
        if 'output_url' in kwargs:
            self.__output_url = kwargs['output_url']
        if 'reason_code' in kwargs:
            self.__reason_code = kwargs['reason_code']
        if 'reason_mea_culpa' in kwargs:
            self.__reason_mea_culpa = kwargs['reason_mea_culpa']

    @property
    def task_id(self) -> UUID:
        return self.__task_id

    @property
    def slave_id(self) -> str:
        return self.__slave_id

    @property
    def executor_id(self) -> str:
        return self.__executor_id

    @property
    def start_time(self) -> int:
        return self.__start_time

    @property
    def hostname(self) -> str:
        return self.__hostname

    @property
    def status(self) -> Status:
        return self.__status

    @property
    def preempted(self) -> bool:
        return self.__preempted

    @property
    def end_time(self) -> Optional[int]:
        return self.__end_time

    @property
    def progress(self) -> Optional[int]:
        return self.__progress

    @property
    def progress_message(self) -> Optional[str]:
        return self.__progress_message

    @property
    def reason_code(self) -> Optional[int]:
        return self.__reason_code

    @property
    def output_url(self) -> Optional[str]:
        return self.__output_url

    @property
    def executor(self) -> Optional[Executor]:
        return self.__executor

    @property
    def reason_mea_culpa(self) -> Optional[bool]:
        return self.__reason_mea_culpa
