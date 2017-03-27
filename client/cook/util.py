"""Module containing utility functions that don't fit nicely anywhere else."""

import os
import sys
import json
import time
import importlib

from datetime import datetime, timedelta

class hashabledict(dict):
    """A hashable dict. Useful when you want to pass a dict to a memoized function."""
    def __hash__(self):
        return hash(frozenset(self))

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
            except json.decoder.JSONDecodeError:
                pass

    return content

def load_first_json_file(paths = []):
    """Returns the contents of the first parseable JSON file in a list of paths."""
    contents = (load_json_file(p) for p in paths if p)

    return next((c for c in contents if c), None)

def indirect_call(name, params, env):
    """Indirectly call a function.

    Parameters
    ---------
    name : str
        Name of function to call. Should either be a string key, found in the `env` dict,
        or a string of 'module:function', for which the module will be loaded and the
        function called.
    params : dict
        dict of params to be passed to the indirectly called function.
    env : dict
        dict of possible functions to call when a name without a module is provided.

    """

    if ':' in name:
        module, function = name.split(':')

        return getattr(importlib.import_module(module), function)(**params)
    else:
        return env[name](**params)

def await_until(pred, timeout=30, interval=5):
    """Wait, retrying a predicate until it is True, or the timeout value has been exceeded."""
    finish = datetime.now() + timedelta(seconds=timeout)
    result = None

    while True:
        result = pred()

        if result:
            break

        if datetime.now() >= finish:
            break

        time.sleep(interval)

    return result
