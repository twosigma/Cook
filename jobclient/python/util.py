import time
import uuid

from uuid import UUID


class FetchableUri:
    """An immutable fetchable Mesos URI.

    Represents a resource that Mesos should fetch before launching a job, with
    extra parameters indicating how it should be handled.
    """
    value: str

    cache: bool
    extract: bool
    executable: bool

    def __init__(self, value: str, *,
                 cache: bool = False,
                 extract: bool = True,
                 executable: bool = False):
        """Initialize a fetchable URI object.

        :param value: The base URI.
        :type value: str
        :param cache: If true, then the resource should be cached, defaults to
            False.
        :type cache: bool, optional
        :param extract: If true, then the resource is an archive that should
            be, defaults to True.
        :type extract: bool, optional
        :param executable: If true, then the resources should be marked as
            executable, defaults to False.
        :type executable: bool, optional
        """
        self.value = value
        self.cache = cache
        self.extract = extract
        self.executable = executable

    def __len__(self):
        """Get the length of the inner URI."""
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
        """Generate this object's `dict` representation."""
        return {
            'value': self.value,
            'executable': self.is_executable,
            'cache': self.is_cache,
            'extract': self.is_extract
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'FetchableUri':
        """Parse a `FetchableURI` from a `dict` representation."""
        return cls(**d)


def make_temporal_uuid() -> UUID:
    """Create a temporally-clustered UUID.

    UUIDs generated with this function will be temporally clustered so that
    UUIDs generated closer in time will have a longer prefix than those
    generated further apart.
    """
    millis = int(time.time() * 1000)
    millis_masked = millis & ((1 << 40) - 1)
    base_uuid = uuid.uuid4()

    base_high_masked = int.from_bytes(base_uuid.bytes[:4], byteorder='big')
    base_high_masked &= (1 << 24) - 1
    base_high_masked |= millis_masked << 24
    base_high_masked <<= 64

    base_low = int.from_bytes(base_uuid.bytes[4:], byteorder='big')

    return UUID(int=base_high_masked | base_low)
