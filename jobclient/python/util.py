import time
import uuid

from uuid import UUID


class FetchableUri:
    __value: str

    __cache: bool = False
    __extract: bool = True
    __executable: bool = False

    def __init__(self, value, **kwargs):
        self.__value = value

        if 'cache' in kwargs:
            self.__cache = kwargs['cache']
        if 'extract' in kwargs:
            self.__extract = kwargs['extract']
        if 'executable' in kwargs:
            self.__executable = kwargs['executable']

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

    @property
    def value(self) -> str:
        return self.__value

    @property
    def is_cache(self) -> bool:
        return self.__cache

    @property
    def is_extract(self) -> bool:
        return self.__extract

    @property
    def is_executable(self) -> bool:
        return self.__executable


def make_temporal_uuid() -> UUID:
    millis = int(time.time() * 1000)
    millis_masked = millis & ((1 << 40) - 1)
    base_uuid = uuid.uuid4()

    base_high_masked = int.from_bytes(base_uuid.bytes[:4], byteorder='big')
    base_high_masked &= (1 << 24) - 1
    base_high_masked |= millis_masked << 24
    base_high_masked <<= 64

    base_low = int.from_bytes(base_uuid.bytes[4:], byteorder='big')

    return UUID(base_high_masked | base_low)
