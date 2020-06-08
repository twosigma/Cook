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

import time
import uuid

from datetime import datetime
from uuid import UUID


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
    """Convert a Python ``datetime`` object to a Unix millisecond timestamp.

    This is necessary as the timestamps Cook returns in its API are in
    milliseconds, while the Python ``datetime`` API uses seconds for Unix
    timestamps.
    """
    return int(dt.timestamp() * 1000)


def unix_ms_to_datetime(timestamp: int) -> datetime:
    """Convert a Unix millisecond timestamp to a Python ``datetime`` object.

    This is necessary as the timestamps Cook returns in its API are in
    milliseconds, while the Python ``datetime`` API uses seconds for Unix
    timestamps.
    """
    return datetime.fromtimestamp(timestamp / 1000)
