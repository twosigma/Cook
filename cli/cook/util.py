"""Module containing utility functions that don't fit nicely anywhere else."""

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from urllib.parse import urlparse

import arrow
import humanfriendly

from cook import terminal

quit_running = False

TRUE_STRINGS = {'yes', 'true', 'y'}
FALSE_STRINGS = {'no', 'false', 'n'}


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

        if result or quit_running:
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
        uuid.UUID(uuid_to_test, version=version)
        return True
    except:
        return False


silent = False


def print_info(text, silent_mode_text=None, end='\n'):
    """Prints text, unless in silent mode (-s / --silent)"""
    if not silent:
        print(text, flush=True, end=end)
    elif silent_mode_text:
        print(silent_mode_text, flush=True, end=end)


def print_error(text):
    """Prints text to stderr, colored as a failure"""
    print(terminal.failed(text), file=sys.stderr)


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


def check_positive(value):
    """Checks that the given value is a positive integer"""
    try:
        integer = int(value)
    except:
        raise argparse.ArgumentTypeError(f'{value} is not an integer')
    if integer <= 0:
        raise argparse.ArgumentTypeError(f'{value} is not a positive integer')
    return integer


def guard_no_cluster(clusters):
    """Throws if no clusters have been specified, either via configuration or via the command line"""
    if not clusters:
        raise Exception('You must specify at least one cluster.')


def distinct(seq):
    """Remove duplicate entries from a sequence. Maintains original order."""
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def partition(l, n):
    """Yield successive n-sized chunks from l"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def load_target_clusters(config_map, url=None, cluster=None):
    """Given the config and (optional) url and cluster flags, returns the list of clusters to target"""
    if cluster and url:
        raise Exception('You cannot specify both a cluster name and a cluster url at the same time')

    clusters = None
    config_clusters = config_map.get('clusters')
    if url:
        if urlparse(url).scheme == '':
            url = f'http://{url}'
        clusters = [{'name': url, 'url': url}]
    elif config_clusters:
        if cluster:
            clusters = [c for c in config_clusters if c.get('name').lower() == cluster.lower()]
            if len(clusters) == 0 and len(config_clusters) > 0:
                config_cluster_names = ', '.join([c.get('name') for c in config_clusters])
                raise Exception(f'You specified cluster {cluster}, which was not present in your config.' +
                                f' You have the following clusters configured: {config_cluster_names}.')
        else:
            clusters = [c for c in config_clusters if 'disabled' not in c or not c['disabled']]

    return clusters


def str2bool(v):
    """Converts the given string to a boolean, or returns None"""
    if v.lower() in TRUE_STRINGS:
        return True
    elif v.lower() in FALSE_STRINGS:
        return False
    else:
        return None


def date_time_string_to_ms_since_epoch(date_time_string):
    """Converts the given date_time_string (e.g. '5 minutes ago') to milliseconds since epoch"""
    import tzlocal
    from cook import dateparser
    local_tz = tzlocal.get_localzone()
    dt = dateparser.parse(date_time_string, local_tz)
    if dt:
        import pytz
        logging.debug(f'parsed "{date_time_string}" as {dt}')
        epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
        ms_since_epoch = int((dt - epoch).total_seconds() * 1000)
        logging.debug(f'converted "{date_time_string}" to ms {ms_since_epoch}')
        return ms_since_epoch
    else:
        raise Exception(f'"{date_time_string}" is not a valid date / time string.')
