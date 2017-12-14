import json
import logging
import os
from datetime import datetime
from functools import partial

from tabulate import tabulate

from cook import http, mesos, colors
from cook.querying import query_unique_and_run, parse_entity_refs
from cook.util import guard_no_cluster


def basename(path):
    """Returns the "last" part of the provided path"""
    return os.path.basename(os.path.normpath(path))


def is_directory(entry):
    """Returns true if the given entry is a directory"""
    return entry['nlink'] > 1


def format_path(entry):
    """Formats the directory entry path for display on screen"""
    executable_by_user = 3
    name = basename(entry['path'])
    if is_directory(entry):
        name = colors.directory(name)
    elif entry['mode'][executable_by_user] == 'x':
        name = colors.executable(name)
    return name


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


def browse_files(instance, sandbox_dir, path):
    """
    Calls the files/browse endpoint on the Mesos agent corresponding to
    the given instance, and for the provided sandbox directory and path
    """
    agent_url = mesos.instance_to_agent_url(instance)
    resp = http.__get(f'{agent_url}/files/browse', params={'path': os.path.join(sandbox_dir, path or '')})
    if resp.status_code == 404:
        raise Exception(f"Cannot access '{path}' (no such file or directory).")

    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for its files.')

    return resp.json()


def ls_for_instance(instance, sandbox_dir, path, long_format, as_json):
    """Lists contents of the Mesos sandbox path for the given instance"""
    entries = browse_files(instance, sandbox_dir, path)
    if len(entries) == 0 and path:
        # Mesos will return 200 with an empty list in two cases:
        # - the provided path is a file (this is odd)
        # - the provided path is an empty directory (this one makes sense)
        # In the former case, we want to return the entry for that file to the user
        parent_entries = browse_files(instance, sandbox_dir, os.path.dirname(path))
        child_entries = [e for e in parent_entries if basename(e['path']) == basename(path)]
        if len(child_entries) > 0 and not is_directory(child_entries[0]):
            entries = child_entries

    if as_json:
        print(json.dumps(entries))
    else:
        if len(entries) > 0:
            if long_format:
                rows = [directory_entry_to_row(e) for e in entries]
                table = tabulate(rows, tablefmt='plain')
                print(table)
            else:
                print('\n'.join(colors.wrap('  '.join([format_path(e) for e in entries]))))
        else:
            logging.info('the directory is empty')


def ls(clusters, args, _):
    """Lists contents of the corresponding Mesos sandbox path by job or instance uuid."""
    guard_no_cluster(clusters)
    entity_refs, clusters_of_interest = parse_entity_refs(clusters, args.get('uuid'))
    path = args.get('path')
    long_format = args.get('long_format')
    as_json = args.get('json')
    literal = args.get('literal')

    if len(entity_refs) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    if path and not literal and any(c in path for c in '*?[]{}'):
        message = 'It looks like you are trying to glob, but ls does not support globbing. ' \
                  f'You can use the {colors.bold("ssh")} command instead:\n' \
                  '\n' \
                  f'  cs ssh {entity_refs[0]}\n' \
                  '\n' \
                  f'Or, if you want the literal path {colors.bold(path)}, add {colors.bold("--literal")}:\n' \
                  '\n' \
                  f'  cs ls {colors.bold("--literal")} {entity_refs[0]} {path}'
        print(message)
        return 1

    command_fn = partial(ls_for_instance, path=path, long_format=long_format, as_json=as_json)
    query_unique_and_run(clusters_of_interest, entity_refs[0], command_fn)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ls', help='list contents of Mesos sandbox by job or instance uuid')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-l', help='use a long listing format', dest='long_format', action='store_true')
    group.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    parser.add_argument('--literal', help='treat globbing characters literally', dest='literal', action='store_true')
    parser.add_argument('uuid', nargs=1)
    parser.add_argument('path', nargs='?')
    return ls
