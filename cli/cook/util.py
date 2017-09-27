"""Module containing utility functions that don't fit nicely anywhere else."""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta

import arrow
import humanfriendly


def deep_merge(a, b):
    """Merges a and b, letting b win if there is a conflict"""
    merged = a.copy()
    for key in b:
        b_value = b[key]
        merged[key] = b_value
        if key in a:
            a_value = a[key]
            if isinstance(a_value, dict) and isinstance(b_value, dict):
                merged[key] = deep_merge(a_value, b_value)
    return merged


def read_lines():
    """Read lines from stdin."""
    return sys.stdin.read().splitlines()


def load_json_file(path):
    """Decode a JSON formatted file."""
    content = None

    if os.path.isfile(path):
        with open(path) as json_file:
            try:
                logging.debug('attempting to load json configuration from %s' % path)
                content = json.load(json_file)
            except Exception:
                pass

    return content


def load_first_json_file(paths=None):
    """Returns the contents of the first parseable JSON file in a list of paths."""
    if paths is None:
        paths = []
    contents = (load_json_file(p) for p in paths if p)
    return next((c for c in contents if c), None)


def wait_until(pred, timeout=30, interval=5):
    """
    Wait, retrying a predicate until it is True, or the 
    timeout value has been exceeded.
    """
    if timeout:
        finish = datetime.now() + timedelta(seconds=timeout)
    else:
        finish = None

    while True:
        result = pred()

        if result:
            break

        if finish and datetime.now() >= finish:
            break

        time.sleep(interval)

    return result


def is_valid_uuid(uuid_to_test, version=4):
    """
    Check if uuid_to_test is a valid UUID.

    Parameters
    ----------
    uuid_to_test : str
    version : {1, 2, 3, 4}

    Returns
    -------
    `True` if uuid_to_test is a valid UUID, otherwise `False`.

    Examples
    --------
    >>> is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a')
    True
    >>> is_valid_uuid('c9bf9e58')
    False
    """
    try:
        uuid_obj = uuid.UUID(uuid_to_test, version=version)
    except:
        return False

    return str(uuid_obj) == uuid_to_test


silent = False


def print_info(text, silent_mode_text=None):
    """Prints text, unless in silent mode (-s / --silent)"""
    if not silent:
        print(text, flush=True)
    elif silent_mode_text:
        print(silent_mode_text, flush=True)


def strip_all(strs):
    """Strips whitespace from each string in strs"""
    return [s.strip() for s in strs]


def seconds_to_timedelta(s):
    """Converts seconds to a timedelta for display on screen"""
    return humanfriendly.format_timespan(s)


def millis_to_timedelta(ms):
    """Converts milliseconds to a timedelta for display on screen"""
    return seconds_to_timedelta(round(ms / 1000))


def millis_to_date_string(ms):
    """Converts milliseconds to a date string for display on screen"""
    s, _ = divmod(ms, 1000)
    utc = time.gmtime(s)
    return arrow.get(utc).humanize()


def current_user():
    """Returns the value of the USER environment variable"""
    return os.environ['USER']
