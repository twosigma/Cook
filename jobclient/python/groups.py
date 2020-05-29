import util

from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class HostPlacement:
    placement_type: HostPlacementType
    parameters: Dict[str, str] = {}

    def parameter(self, key: str) -> str:
        return self.parameters[key]

    def to_dict(self) -> dict:
        return {
            'type': self.placement_type,
            'parameters': self.parameters
        }

    def __index__(self, key: str) -> str:
        return self.parameter(key)


@dataclass(frozen=True)
class Group:
    uuid: UUID = field(default_factory=util.make_temporal_uuid)
    status: Status = Status.INITIALIZED
    name: str = 'cookgroup'
    host_placement: HostPlacement = HostPlacement(HostPlacement.ALL)
    straggler_handling: StragglerHandling = StragglerHandling()
    jobs: List[UUID] = []

    def to_dict(self) -> dict:
        return {
            'uuid': self.uuid,
            'status': self.status,
            'name': self.name,
            'host_placement': self.host_placement,
            'straggler_handling': self.straggler_handling,
            'jobs': self.jobs
        }
