import time
import uuid

from uuid import UUID
from typing import Any, Dict


class FetchableUri:
    value: str

    cache: bool
    extract: bool
    executable: bool

    def __init__(self, value: str, *,
                 cache: bool = False,
                 extract: bool = True,
                 executable: bool = False):
        self.value = value
        self.cache = cache
        self.extract = extract
        self.executable = executable

    def __len__(self):
        return len(self.value)

    def __hash__(self):
        PRIME = 37
        result = 1
        result = PRIME * result + hash(self.value)
        result = PRIME * result + (0 if self.is_extract else 1)
        result = PRIME * result + (0 if self.is_executable else 3)
        result = PRIME * result + (0 if self.is_cache else 5)
        return result

    def __eq__(self, other):
        if self is other:
            return True
        if other is None:
            return False
        if not isinstance(other, self.__class__):
            return False
        return (self.value == other.value and
                self.is_cache == other.is_cache and
                self.is_executable == other.is_executable and
                self.is_extract == other.is_extract)

    def __repr__(self):
        return f'FetchableUri({self.value}, extract={self.is_extract}, executable={self.is_executable}, cache={self.is_cache})' # noqa 501

    def as_dict(self):
        return {
            'value': self.value,
            'executable': self.is_executable,
            'cache': self.is_cache,
            'extract': self.is_extract
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'FetchableUri':
        return cls(**d)


def make_temporal_uuid() -> UUID:
    millis = int(time.time() * 1000)
    millis_masked = millis & ((1 << 40) - 1)
    base_uuid = uuid.uuid4()

    base_high_masked = int.from_bytes(base_uuid.bytes[:4], byteorder='big')
    base_high_masked &= (1 << 24) - 1
    base_high_masked |= millis_masked << 24
    base_high_masked <<= 64

    base_low = int.from_bytes(base_uuid.bytes[4:], byteorder='big')

    return UUID(int=base_high_masked | base_low)


def kebab_to_snake(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a `dict`'s keys from `kebab-case` to `snake_case`.

    The provided dict is expected to be the result of a call to `json.load` or
    `json.loads`. As such, it is expected that the keys are all string values.

    This function will scan the topmost level of the `dict` and replace all
    keys in `kebab-case`, as is used in Cook for its JSON objects, to
    `snake_case`, as is used in Python for variable names.

    Usage
    -----
    Use this function in as a value for the `object_hook` keyword parameter in
    `json.load` or `json.loads`.

    Parameters
    ----------
    :param obj: The object to replace keys for.
    :type obj: dict
    :return: A shallow copy of the provided dict with all `kebab-case` keys
        converted to `snake-case`.
    :rtype: dict
    """
    return {
        key.replace('-', '_'): value for key, value in obj.items()
    }
