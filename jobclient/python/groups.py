import util

from enum import Enum
from typing import Dict, List
from uuid import UUID

from straggler_handling import StragglerHandling


class Status(Enum):
    INITIALIZED = 'INITIALIZED'
    WAITING = 'WAITING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Status.{self.value}'

    def from_string(self, name: str) -> 'Status':
        return _STATUS_LOOKUP[name.lower()]


_STATUS_LOOKUP = {
    'INITIALIZED': Status.INITIALIZED,
    'WAITING': Status.WAITING,
    'RUNNING': Status.RUNNING,
    'COMPLETED': Status.COMPLETED
}


class HostPlacementType(Enum):
    UNIQUE = 'UNIQUE'
    BALANCED = 'BALANCED'
    ONE = 'ONE'
    ATTRIBUTE_EQUALS = 'ATTRIBUTE-EQUALS'
    ALL = 'ALL'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'HostPlacementType.{self.value}'

    @staticmethod
    def from_string(name: str) -> 'HostPlacementType':
        return _HOST_PLACEMENT_TYPE_LOOKUP[name.lower()]


_HOST_PLACEMENT_TYPE_LOOKUP = {
    'unique': HostPlacementType.UNIQUE,
    'balanced': HostPlacementType.BALANCED,
    'one': HostPlacementType.ONE,
    'attribute-equals': HostPlacementType.ATTRIBUTE_EQUALS,
    'all': HostPlacementType.ALL,
}


class HostPlacement:
    __placement_type: HostPlacementType
    __parameters: Dict[str, str]

    def __init__(self, placement_type: HostPlacementType,
                 parameters: Dict[str, str] = {}):
        self.__placement_type = placement_type
        self.__parameters = parameters

    def parameter(self, key: str) -> str:
        return self.parameters[key]

    def to_dict(self) -> dict:
        return {
            'type': self.placement_type,
            'parameters': self.parameters
        }

    def __index__(self, key: str) -> str:
        return self.parameter(key)

    @property
    def placement_type(self) -> HostPlacementType:
        return self.__placement_type

    @property
    def parameters(self) -> Dict[str, str]:
        return self.__parameters


class Group:
    __uuid: UUID
    __status: Status
    __name: str
    __host_placement: HostPlacement
    __straggler_handling: StragglerHandling
    __jobs: List[UUID]

    def __init__(self, *,
                 uuid: UUID = None,
                 status: Status = Status.INITIALIZED,
                 name: str = 'cookgroup',
                 host_placement: HostPlacement
                                = HostPlacement(HostPlacementType.ALL),
                 straggler_handling: StragglerHandling = StragglerHandling(),
                 jobs: List[UUID] = []):
        self.__uuid = uuid if uuid is not None else util.make_temporal_uuid()
        self.__status = status
        self.__name = name
        self.__host_placement = host_placement
        self.__straggler_handling = straggler_handling

    def to_dict(self) -> dict:
        return {
            'uuid': self.uuid,
            'status': self.status,
            'name': self.name,
            'host_placement': self.host_placement,
            'straggler_handling': self.straggler_handling,
            'jobs': self.jobs
        }

    @property
    def uuid(self) -> UUID:
        return self.__uuid

    @property
    def status(self) -> Status:
        return self.__status

    @property
    def name(self) -> str:
        return self.__name

    @property
    def host_placement(self) -> HostPlacement:
        return self.__host_placement

    @property
    def straggler_handling(self) -> StragglerHandling:
        return self.__straggler_handling

    @property
    def jobs(self) -> List[UUID]:
        return self.__jobs
