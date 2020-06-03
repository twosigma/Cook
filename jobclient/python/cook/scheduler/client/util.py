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

import copy
import time
import uuid

from datetime import datetime
from uuid import UUID
from typing import Dict, List, Optional


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

    def __repr__(self):
        return f'FetchableUri({self.value}, extract={self.is_extract}, executable={self.is_executable}, cache={self.is_cache})' # noqa 501

    def to_dict(self):
        """Generate this object's `dict` representation."""
        return {
            'value': self.value,
            'executable': self.executable,
            'cache': self.cache,
            'extract': self.extract
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'FetchableUri':
        """Parse a `FetchableURI` from a `dict` representation."""
        return cls(**d)


class Dataset:
    """A data dependency for a job."""
    dataset: Dict[str, str]
    partitions: Optional[List[Dict[str, str]]]

    def __init__(self, *,
                 dataset: Dict[str, str],
                 partitions: Optional[List[Dict[str, str]]] = None):
        """Create a new Dataset.

        :param dataset: Describes the dataset.
        :type dataset: Dict[str, str]
        :param partitions: Describes a subset of the dataset required.
        :type partitions: Optional[Dict[str, str]], optional
        """
        self.dataset = dataset
        self.partitions = partitions

    def to_dict(self) -> dict:
        """Convert a dataset into its dict representation."""
        d = {'dataset': self.dataset}
        if self.partitions is not None:
            d['partitions'] = self.partitions
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Dataset':
        """Parse a Dataset from its dict representation."""
        d = copy.deepcopy(d)
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


def datetime_to_unix_ms(dt: datetime) -> int:
    """Convert a Python `datetime` object to a Unix millisecond timestamp.

    This is necessary as the timestamps Cook returns in its API are in
    milliseconds, while the Python `datetime` API uses seconds for Unix
    timestamps.
    """
    return int(dt.timestamp() * 1000)


def unix_ms_to_datetime(timestamp: int) -> datetime:
    """Convert a Unix millisecond timestamp to a Python `datetime` object.

    This is necessary as the timestamps Cook returns in its API are in
    milliseconds, while the Python `datetime` API uses seconds for Unix
    timestamps.
    """
    return datetime.fromtimestamp(timestamp / 1000)
