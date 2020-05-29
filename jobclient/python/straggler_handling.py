from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict


class StragglerHandlingType(Enum):
    NONE = 'NONE'
    QUANTILE_DEVIATION = 'QUANTILE-DEVIATION'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'StragglerHandlingType.{self.value}'

    @staticmethod
    def from_string(self, name: str) -> 'StragglerHandlingType':
        return _STRAGGLER_HANDLING_TYPE_LOOKUP[name.lower()]


_STRAGGLER_HANDLING_TYPE_LOOKUP = {
    'none': StragglerHandlingType.NONE,
    'quantile-deviation': StragglerHandlingType.QUANTILE_DEVIATION
}


@dataclass(frozen=True)
class StragglerHandling:
    handling_type: StragglerHandlingType = StragglerHandlingType.NONE
    parameters: Dict[str, Any] = {}

    def __post_init__(self):
        if self.handling_type == StragglerHandlingType.QUANTILE_DEVIATION:
            if 'quantile' not in self.parameters \
                    or self.parameters['quantile'] is None:
                raise Exception("Straggler handling type was 'quantile-deviation' but 'parameters.quantile' was not set") # noqa
            if 'multiplier' not in self.parameters \
                    or self.parameters['multiplier'] is None:
                raise Exception("Straggler handling type was 'quantile-deviation' but 'parameters.multiplier' was not set") # noqa

    def parameter(self, key: str) -> Any:
        return self.parameters[key]

    def to_dict(self) -> dict:
        return {
            'type': self.handling_type,
            'parameters': self.parameters
        }

    def __index__(self, key: str) -> Any:
        return self.parameter(key)
