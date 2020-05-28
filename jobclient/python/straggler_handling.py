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


class StragglerHandling:
    __handling_type: StragglerHandlingType
    __parameters: Dict[str, Any]

    def __init__(self,
                 handling_type: StragglerHandlingType = StragglerHandlingType.NONE, # noqa
                 parameters: Dict[str, Any] = {}):
        self.__handling_type = handling_type
        self.__parameters = parameters

        if handling_type == StragglerHandlingType.QUANTILE_DEVIATION:
            if 'quantile' not in parameters or parameters['quantile'] is None:
                raise Exception("Straggler handling type was 'quantile-deviation' but 'parameters.quantile' was not set") # noqa
            if 'multiplier' not in parameters or parameters['multiplier'] is None: # noqa
                raise Exception("Straggler handling type was 'quantile-deviation' but 'parameters.multiplier' was not set") # noqa

    def parameter(self, key: str) -> Any:
        return self.__parameters[key]

    def to_dict(self) -> dict:
        return {
            'type': self.handling_type,
            'parameters': self.parameters
        }

    def __index__(self, key: str) -> Any:
        return self.parameter(key)

    @property
    def handling_type(self) -> StragglerHandlingType:
        return self.__handling_type

    @property
    def parameters(self) -> Dict[str, Any]:
        return self.__parameters
