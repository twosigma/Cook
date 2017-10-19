import logging
import os
from datetime import datetime
from functools import partial

from tabulate import tabulate

from cook import http, mesos, colors
from cook.querying import query_unique_and_run
from cook.util import strip_all


def format_path(entry):
    """Formats the directory entry path for display on screen"""
    executable_by_user = 3
    basename = os.path.basename(os.path.normpath(entry['path']))
    if entry['nlink'] > 1:
        basename = colors.directory(basename)
    elif entry['mode'][executable_by_user] == 'x':
        basename = colors.executable(basename)
    return basename


def format_modified_time(entry):
    """Formats the modified time (seconds since epoch) for display on screen"""
    return datetime.fromtimestamp(entry['mtime']).strftime('%b %-d %H:%M')


def directory_entry_to_row(entry):
    """
    Converts the given entry into a row for use in the long listing format, e.g.:

        -rwxr-xr-x  1  root  root  9157224  Oct 18 21:46  cook-executor
        -rw-r--r--  1  root  root     2870  Oct 18 21:46  executor.log
        drwxr-xr-x  2  root  root     4096  Oct 18 21:46  foo
        -rw-r--r--  1  root  root     1792  Oct 18 21:46  stderr
        -rw-r--r--  1  root  root        0  Oct 18 21:46  stdout
    """
    return (entry['mode'],
            entry['nlink'],
            entry['uid'],
            entry['gid'],
            entry['size'],
            format_modified_time(entry),
            format_path(entry))


def ls_for_instance(instance, job, path, long_format, json):
    """Lists contents of the Mesos sandbox path for the given instance"""
    agent_url = mesos.instance_to_agent_url(instance)
    sandbox_dir = mesos.retrieve_instance_sandbox_directory(instance, job)
    resp = http.__get(f'{agent_url}/files/browse', params={'path': os.path.join(sandbox_dir, path or '')})
    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for its files.')

    if json:
        print(resp.text)
    else:
        directory_entries = resp.json()
        if len(directory_entries) > 0:
            if long_format:
                rows = [directory_entry_to_row(e) for e in directory_entries]
                table = tabulate(rows, tablefmt='plain')
                print(table)
            else:
                print('  '.join([format_path(e) for e in directory_entries]))


def ls(clusters, args):
    """Lists contents of the corresponding Mesos sandbox path by job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    path = args.get('path')
    long_format = args.get('long_format')
    json = args.get('json')

    if len(uuids) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    query_unique_and_run(clusters, uuids[0], partial(ls_for_instance, path=path, long_format=long_format, json=json))


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ls', help='list contents of the Mesos sandbox by job or instance uuid')
    parser.add_argument('-l', help='use a long listing format', dest='long_format', action='store_true')
    parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    parser.add_argument('uuid', nargs=1)
    parser.add_argument('path', nargs='?')
    return ls
