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

import getpass
import time
import uuid

from datetime import datetime, timedelta
from uuid import UUID

from . import _CLIENT_APP


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


def clamped_ms_to_timedelta(ms: int) -> timedelta:
    """Convert a number of milliseconds into a Python ``timedelta`` object.

    This function will handle overflows if the millisecond count is too large,
    as is the case with Cook's default max job runtime (which holds the
    ``Long.MAX_VALUE`` value from Java). If an overflow condition is hit, then
    this function will clamp the value to either the max timedelta value or the
    min timedelta value, depending on the sign of the parameter.
    """
    try:
        return timedelta(milliseconds=ms)
    except OverflowError:
        return timedelta.max if ms > 0 else timedelta.min


def apply_jobspec_defaults(jobspec: dict):
    """Apply default values to a jobspec.

    This function will add default values to a jobspec if no value is
    provided. The provided jobspec will be modified in-place.
    """
    if 'uuid' not in jobspec:
        jobspec['uuid'] = str(make_temporal_uuid())
    if 'cpus' not in jobspec:
        jobspec['cpus'] = 1.0
    if 'mem' not in jobspec:
        jobspec['mem'] = 128.0
    if 'max-retries' not in jobspec:
        jobspec['max-retries'] = 1
    if 'max-runtime' not in jobspec:
        jobspec['max-runtime'] = timedelta(days=1)
    if 'name' not in jobspec:
        jobspec['name'] = f'{getpass.getuser()}-job'
    if 'application' not in jobspec:
        jobspec['application'] = _CLIENT_APP


def convert_jobpec(jobspec: dict):
    """Convert a Python jobspec into a JSON jobspec.

    This function will convert the higher-level Python types used in job
    submissions into their JSON primitive counterparts (e.g., timedelta is
    converted into the number of milliseconds).

    The provided jobspec is modified in-place.
    """
    if 'max-runtime' in jobspec:
        jobspec['max-runtime'] = jobspec['max-runtime'].total_seconds() * 1000
    if 'application' in jobspec:
        jobspec['application'] = jobspec['application'].to_dict()
    if 'container' in jobspec:
        jobspec['container'] = jobspec['container'].to_dict()
