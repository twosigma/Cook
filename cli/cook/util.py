"""Module containing utility functions that don't fit nicely anywhere else."""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta


def merge_dicts(*ds):
    """Merge a variable number of dicts, from right to left."""
    to_d = ds[0].copy()

    for from_d in ds[1:]:
        to_d.update(from_d)

    return to_d


def flatten_list(l):
    """Flattens a list of lists down into a single list."""
    return [item for sublist in l for item in sublist]


def read_lines():
    """Read lines from stdin."""
    return sys.stdin.read().splitlines()


def load_json_file(path):
    """Decode a JSON formatted file."""
    content = None

    if os.path.isfile(path):
        with open(path) as json_file:
            try:
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
    finish = datetime.now() + timedelta(seconds=timeout)

    while True:
        result = pred()

        if result:
            break

        if datetime.now() >= finish:
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
