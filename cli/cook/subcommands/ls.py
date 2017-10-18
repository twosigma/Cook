import logging
import os
from functools import partial

from cook import http, mesos, colors
from cook.querying import query_unique_and_run
from cook.util import strip_all


def format_path(entry):
    """Formats the directory entry path for display on screen"""
    executable_by_user = 3
    basename = os.path.basename(os.path.normpath(entry['path']))
    if entry['nlink'] != 1:
        basename = colors.directory(basename)
    elif entry['mode'][executable_by_user] == 'x':
        basename = colors.executable(basename)
    return basename


def ls_for_instance(instance, job, path):
    """Lists contents of the Mesos sandbox path for the given instance"""
    agent_url = mesos.instance_to_agent_url(instance)
    sandbox_dir = mesos.retrieve_instance_sandbox_directory(instance, job)
    resp = http.__get(f'{agent_url}/files/browse', params={'path': os.path.join(sandbox_dir, path or '')})
    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for its files.')

    directory_entries = resp.json()
    print('  '.join([format_path(e) for e in directory_entries]))


def ls(clusters, args):
    """Lists contents of the corresponding Mesos sandbox path by job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    path = args.get('path')

    if len(uuids) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    query_unique_and_run(clusters, uuids[0], partial(ls_for_instance, path=path))


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('ls', help='list contents of the corresponding Mesos sandbox by job or instance uuid')
    show_parser.add_argument('uuid', nargs=1)
    show_parser.add_argument('path', nargs='?')
    return ls
