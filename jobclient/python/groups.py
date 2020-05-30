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

import util

from enum import Enum
from typing import Dict, List, Optional
from uuid import UUID

from straggler_handling import StragglerHandling


class Status(Enum):
    """The status of a group.

    `INITIALIZED` is the default status of a Group when initialized.

    If the states of all jobs in a group are `WAITING`, then that group's state
    will also be `WAITING`.

    If any job in a group is running, then the state of that group will also be
    `RUNNING`.

    Finally, if all jobs in a group have completed, then the state of that
    group will be `COMPLETED`.
    """
    INITIALIZED = 'INITIALIZED'
    WAITING = 'WAITING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Status.{self.value}'

    def from_string(self, name: str) -> 'Status':
        """Parse a status from a case-insensitive string representation."""
        return _STATUS_LOOKUP[name.lower()]


_STATUS_LOOKUP = {
    'initialized': Status.INITIALIZED,
    'waiting': Status.WAITING,
    'running': Status.RUNNING,
    'completed': Status.COMPLETED
}


class HostPlacementType(Enum):
    """Host placement type identifier."""
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
        """Parse from a case-insensitive string representation."""
        return _HOST_PLACEMENT_TYPE_LOOKUP[name.lower()]


_HOST_PLACEMENT_TYPE_LOOKUP = {
    'unique': HostPlacementType.UNIQUE,
    'balanced': HostPlacementType.BALANCED,
    'one': HostPlacementType.ONE,
    'attribute-equals': HostPlacementType.ATTRIBUTE_EQUALS,
    'all': HostPlacementType.ALL,
}


class HostPlacement:
    """Dictates how jobs in a group should be organized in terms of hosts."""
    placement_type: HostPlacementType
    parameters: Dict[str, str] = {}

    def __init__(self, placement_type: HostPlacementType,
                 parameters: Dict[str, str] = {}):
        """Initialize a host placement.

        :param placement_type: What type of host placement strategy should be used.
        :type placement_type: HostPlacementType
        :param parameters: Parameters, defaults to an empty `dict`.
        :type parameters: Dict[str, str], optional
        """
        self.placement_type = HostPlacementType
        self.parameters = parameters

    def parameter(self, key: str) -> str:
        """Get the value associated with a parameter."""
        return self.parameters[key]

    def to_dict(self) -> dict:
        """Generate this object's `dict` representation."""
        return {
            'type': self.placement_type,
            'parameters': self.parameters
        }

    def __index__(self, key: str) -> str:
        """Get the value associated with a parameter."""
        return self.parameter(key)


class Group:
    """A group of linked jobs."""
    uuid: UUID
    status: Status = Status.INITIALIZED
    name: str = 'cookgroup'
    host_placement: HostPlacement = HostPlacement(HostPlacement.ALL)
    straggler_handling: StragglerHandling = StragglerHandling()
    jobs: List[UUID] = []

    def __init__(self, *,
                 uuid: Optional[UUID] = None,
                 status: Status = Status.INITIALIZED,
                 name: str = 'cookgroup',
                 host_placement: HostPlacement = HostPlacement(
                     HostPlacement.ALL),
                 straggler_handling: StragglerHandling = StragglerHandling(),
                 jobs: List[UUID] = []):
        """Initialize a group.

        :param uuid: The UUID of this group. If no UUID is specified, then one
            will be generated with the `make_temporal_uuid()`.
        :type uuid: Optional[UUID], optional
        :param status: The status of this group, defaults to Status.INITIALIZED
        :type status: Status, optional
        :param name: A description of this group, defaults to 'cookgroup'
        :type name: str, optional
        :param host_placement: A host placement strategy for this group,
            defaults to `HostPlacement(HostPlacement.ALL)`
        :type host_placement: HostPlacement, optional
        :param straggler_handling: A straggling job handling strategy,
            defaults to the default straggler handling strategy.
        :type straggler_handling: StragglerHandling, optional
        :param jobs: UUIDs of the jobs this group includes, defaults to an
            empty list.
        :type jobs: List[UUID], optional
        """
        self.uuid = uuid if uuid is not None else util.make_temporal_uuid()
        self.status = status
        self.name = name
        self.host_placement = host_placement
        self.straggler_handling = straggler_handling
        self.jobs = jobs

    def to_dict(self) -> dict:
        return {
            'uuid': self.uuid,
            'status': self.status,
            'name': self.name,
            'host_placement': self.host_placement,
            'straggler_handling': self.straggler_handling,
            'jobs': self.jobs
        }
