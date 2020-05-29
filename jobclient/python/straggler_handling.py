from enum import Enum
from typing import Any, Dict


class StragglerHandlingType(Enum):
    """Identifies how stragglers should be handled."""
    NONE = 'NONE'
    QUANTILE_DEVIATION = 'QUANTILE-DEVIATION'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'StragglerHandlingType.{self.value}'

    @staticmethod
    def from_string(self, name: str) -> 'StragglerHandlingType':
        """Generate this object from a case-insensitive representation."""
        return _STRAGGLER_HANDLING_TYPE_LOOKUP[name.lower()]


_STRAGGLER_HANDLING_TYPE_LOOKUP = {
    'none': StragglerHandlingType.NONE,
    'quantile-deviation': StragglerHandlingType.QUANTILE_DEVIATION
}


class StragglerHandling:
    """A straggler handling strategy."""
    handling_type: StragglerHandlingType = StragglerHandlingType.NONE
    parameters: Dict[str, Any] = {}

    def __init__(self,
                 handling_type: StragglerHandlingType
                 = StragglerHandlingType.NONE,
                 parameters: Dict[str, Any] = {}):
        """Initialize a straggler handling strategy.

        :param handling_type: Type
        :type handling_type: StragglerHandlingType
        :param parameters: Parameters for the strategy, defaults to the empty
            `dict`. If the handling type is `NONE`, then this parameter is
            ignored. Otherwise, this parameter is expected to have a `quantile`
            field and a `multiplier` field.
        :type parameters: Dict[str, Any], optional
        :raises Exception: If the handling type is `QUANTILE_DEVIATION` and
            `parameters` does not contain a `quantile` or `multiplier` key.
        """
        self.handling_type = handling_type
        self.parameters = parameters
        if self.handling_type == StragglerHandlingType.QUANTILE_DEVIATION:
            if 'quantile' not in self.parameters \
                    or self.parameters['quantile'] is None:
                raise Exception("Straggler handling type was 'quantile-deviation' but 'parameters.quantile' was not set") # noqa
            if 'multiplier' not in self.parameters \
                    or self.parameters['multiplier'] is None:
                raise Exception("Straggler handling type was 'quantile-deviation' but 'parameters.multiplier' was not set") # noqa

    def parameter(self, key: str) -> Any:
        """Get the value associated with a parameter."""
        return self.parameters[key]

    def to_dict(self) -> dict:
        """Get the `dict` representation of this object."""
        return {
            'type': self.handling_type,
            'parameters': self.parameters
        }

    def __index__(self, key: str) -> Any:
        """Get the value associated with a parameter."""
        return self.parameter(key)
