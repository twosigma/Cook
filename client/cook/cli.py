"""Module implementing a CLI for the Cook scheduler API. """

import os
import sys
import json
import uuid
import logging
import argparse
import importlib

from cook.util import merge_dicts, load_first_json_file, indirect_call, read_lines, flatten_list
from cook.client import CookMultiClient, basic_auth_http_client, key_auth_http_client

###################################################################################################

# Default locations to check for configuration files if one isn't given on the command line
DEFAULT_CONFIG_PATHS = [
    '.cook.json',
    os.path.expanduser('~/.cook.json')
]

# Default http clients, called indirectly and specified in the configuration file
DEFAULT_HTTP_CLIENTS = {
    'key_auth': key_auth_http_client,
    'basic_auth': basic_auth_http_client
}
###################################################################################################

def read_uuids(uuids):
    """Process UUIDs from the command line, potentially reading them, one per line, from stdin."""
    return [uuid.UUID(u.strip()) for u in (read_lines() if uuids[0] == '-' else uuids)]

###################################################################################################

parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
parser.add_argument('--cluster', '-c', help='cluster to use')
parser.add_argument('--config', '-C', help='config file to use')
parser.add_argument('--dry-run', '-d', help='print action and arguments without submitting',
                    dest='dry_run', action='store_true')
parser.add_argument('--no-dry-run',
                    dest='dry_run', action='store_false')

subparsers = parser.add_subparsers(dest='action')

actions = {}

def create_client_spec(c):
    """Create a client spec from cluster configuration.

    An HTTP client is potentially created as part of spec creation.

    For example, given a cluster configuration like this:

    {
      "name": "fake-dev0",
      "url": "http://127.0.0.1:12322/swagger-docs",
      "http_client": {
        "type": "basic_auth",
        "params": {
          "host": "127.0.0.1:12322",
          "username": "u",
          "password": "p"
        }
      }
    }

    An HTTP client will be created by calling the function associated with `basic_auth`
    in the DEFAULT_HTTP_CLIENTS constant. If the "type" value contains a colon, it is
    treated as a module:function. In this way, arbitrary authentication mechanisms can
    be added without changing the Cook client code.

    """
    http_client = None

    if c.get('http_client'):
        http_client = indirect_call(
            c.get('http_client').get('type'),
            c.get('http_client').get('params'),
            DEFAULT_HTTP_CLIENTS
        )

    return {'name': c.get('name'), 'url': c.get('url'), 'http_client': http_client}

def cli(args):
    """Main entrypoint to the Cook CLI.

    Loads configuration files, processes global command line arguments, and calls
    other command line sub-commands (actions) if necessary.

    Accepts a list of `args`, e.g. `['create-job', '-r', 'echo', '1']`.

    Expects each subcommand to return a tuple of `(okay, result)`. `okay` is a boolean,
    and if True, indicates success, and if False, indicates an error.

    """
    args = vars(parser.parse_args(args))
    action = args.pop('action')
    config = args.pop('config')
    cluster = args.pop('cluster')
    dry_run = args.pop('dry_run')

    if action is None:
        parser.print_help()
    else:
        config = load_first_json_file([config] + DEFAULT_CONFIG_PATHS)

        if config:
            defaults = config.get('defaults')
            clusters = config.get('clusters')
            cluster = cluster or defaults.get('cluster')

            if cluster:
                clusters = [c for c in clusters if c.get('name') == cluster]

            if clusters:
                try:
                    return actions[action](
                        CookMultiClient([create_client_spec(c) for c in clusters]),
                        merge_dicts(defaults.get(action) or {}, args),
                        dry_run
                    )
                except ValueError:
                    logging.exception('exception when running "%s"' % action)
            else:
                logging.error('must specify at least one cluster')
        else:
            logging.error('must specify config')

    return (False, None)

###################################################################################################

create_job_parser = subparsers.add_parser('create-job', help='create job for command')
create_job_parser.add_argument('command', nargs='+')
create_job_parser.add_argument('--uuid', '-u', help='uuid of job')
create_job_parser.add_argument('--name', '-n', help='name of job')
create_job_parser.add_argument('--priority', '-p', default=0, help='priority of job')
create_job_parser.add_argument('--max-retries', default=1, help='maximum retries for job')
create_job_parser.add_argument('--max-runtime', default=200, help='maximum runtime for job')
create_job_parser.add_argument('--cpus', default=1, help='cpus to reserve for job')
create_job_parser.add_argument('--mem', default=512, help='memory to reserve for job')
create_job_parser.add_argument('--raw', '-r', help='raw job spec in json format',
                               dest='raw', action='store_true')
create_job_parser.add_argument('--no-raw', dest='raw', action='store_false')

def parse_raw_job_spec(job, r):
    """Parse a JSON string containing raw job data and merge with job template.

    Job data can either be a dict of job attributes (indicating a single job),
    or a list of dicts (indicating multiple jobs). In either case, the job attributes
    are merged with (and override) the `job` template attributes.

    Throws a ValueError if there is a problem parsing the data.

    """
    try:
        content = json.loads(r)

        if type(content) is dict:
            return [merge_dicts(job, content)]
        elif type(content) is list:
            return [merge_dicts(job, c) for c in content]
        else:
            raise ValueError('invalid format for raw job')
    except json.decoder.JSONDecodeError:
        raise ValueError('malformed JSON for raw job')

def create_job(client, args, dry_run=False):
    """Schedules a job (or jobs) to run a command.

    Assembles a list of jobs, potentially getting data from configuration, the command
    line, and stdin.

    """
    job = args
    raw = job.pop('raw')
    command = " ".join(job.pop('command'))

    if command == '-':
        commands = read_lines()
    else:
        commands = command.splitlines()

    if raw:
        jobs = flatten_list([parse_raw_job_spec(job, c) for c in commands])
    else:
        jobs = [merge_dicts(job, {'command': c}) for c in commands]

    for j in jobs:
        if not j['uuid']:
            j['uuid'] = uuid.uuid4()

        if not job['name']:
            j['name'] = "{0}-{1}".format(os.getlogin(), uuid.uuid4())

    if dry_run:
        return (True, {'jobs': jobs})
    else:
        return client.create_jobs(jobs)

actions.update({'create-job': create_job})

###################################################################################################

create_group_parser = subparsers.add_parser('create-group', help='create group')
create_group_parser.add_argument('spec', nargs='*')
create_group_parser.add_argument('--uuid', '-u', help='uuid of job group')
create_group_parser.add_argument('--name', '-n', help='name of job group')
create_group_parser.add_argument('--placement-type', '-pt', help='host placement type for job group')
create_group_parser.add_argument('--placement-param', '-pp', help='host placement type for job group',
                                 dest='placement_params', default=[], action='append')
create_group_parser.add_argument('--straggler-type', '-st', help='straggler handling type for job group')
create_group_parser.add_argument('--straggler-param', '-sp', help='straggler handling type for job group',
                                 dest='straggler_params', default=[], action='append')

def parse_raw_group_spec(group, r):
    """Parse a JSON string containing raw group data and merge with group template.

    Group data can either be a dict of group attributes (indicating a single group),
    or a list of dicts (indicating multiple groups). In either case, the group attributes
    are merged with (and override) the `group` template attributes.

    Throws a ValueError if there is a problem parsing the data.

    """
    try:
        content = json.loads(r)

        if type(content) is dict:
            return [merge_dicts(group, content)]
        elif type(content) is list:
            return [merge_dicts(group, c) for c in content]
        else:
            raise ValueError('Unexpected format for raw job group.')
    except json.decoder.JSONDecodeError:
        raise ValueError('Malformed JSON for raw job group.')

def parse_param_value(v):
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return v

def parse_name_value_params(ps):
    """Turns an array of 'name=value's into a dict."""
    return {k: parse_param_value(v) for (k, v) in [p.split('=') for p in ps]}

def create_group(client, args, dry_run=False):
    """Creates a job group.

    Assembles a list of groups, potentially getting data from configuration, the command
    line, and stdin.

    """
    group = args
    specs = group.pop('spec')

    if specs and specs[0] == '-':
        specs = read_lines()

    # the existence of any specs implies raw parsing mode
    if specs:
        groups = flatten_list([parse_raw_group_spec(group, s) for s in specs])
    else:
        groups = [group]

    for g in groups:
        if not g['uuid']:
            g['uuid'] = uuid.uuid4()

        if not g['name']:
            g.pop('name')

        if g['placement_type']:
            g['host_placement'] = {
                'type': g['placement_type'],
                'parameters': parse_name_value_params(g.get('placement_params', {}))
            }

        g.pop('placement_type')
        g.pop('placement_params')

        if g['straggler_type']:
            g['straggler_handling'] = {
                'type': g['straggler_type'],
                'parameters': parse_name_value_params(g.get('straggler_params', {}))
            }

        g.pop('straggler_type')
        g.pop('straggler_params')

    if dry_run:
        (True, {'groups': groups})
    else:
        return client.create_groups(groups)

actions.update({'create-group': create_group})

###################################################################################################

kill_job_parser = subparsers.add_parser('kill-job', help='kill job related to uuid')
kill_job_parser.add_argument('uuid', nargs='+')

def kill_job(client, args, dry_run=False):
    """Kills job related to UUID."""
    uuids = read_uuids(args.get('uuid'))

    if dry_run:
        return (True, {'uuids': uuids})
    else:
        return client.delete_jobs(uuids)

actions.update({'kill-job': kill_job})

###################################################################################################

kill_instance_parser = subparsers.add_parser('kill-instance', help='kill instance related to uuid')
kill_instance_parser.add_argument('uuid', nargs='+')

def kill_instance(client, args, dry_run=False):
    """Kills instance related to UUID."""
    uuids = read_uuids(args.get('uuid'))

    if dry_run:
        return (True, {'uuids': uuids})
    else:
        return client.delete_instances(uuids)

actions.update({'kill-instance': kill_instance})

###################################################################################################

retry_job_parser = subparsers.add_parser('retry-job', help='retry job related to uuid')
retry_job_parser.add_argument('uuid', nargs='+')
retry_job_parser.add_argument('--max-retries', default=1, help='maximum retries for job')

def retry_job(client, args, dry_run=False):
    """Retries job related to UUID."""
    uuids = read_uuids(args.get('uuid'))
    max_retries = int(args.get('max_retries'))

    if dry_run:
        return (True, {'uuids': uuids, 'max_retries': max_retries})
    else:
        return client.retry_jobs(uuids, max_retries)

actions.update({'retry-job': retry_job})

###################################################################################################

await_job_parser = subparsers.add_parser('await-job', help='await job related to uuid')
await_job_parser.add_argument('uuid', nargs='+')
await_job_parser.add_argument('--timeout', '-t', default=30, help='maximum time to wait for a job to complete')
await_job_parser.add_argument('--interval', '-i', default=5, help='time to wait between polling for a job')

def await_job(client, args, dry_run=False):
    """Waits for job related to UUID."""
    uuids = read_uuids(args.get('uuid'))
    timeout = args.get('timeout')
    interval = args.get('interval')

    if dry_run:
        return (True, {'uuids': uuids, 'timeout': timeout, 'interval': interval})
    else:
        return client.await_jobs(uuids, timeout, interval)

actions.update({'await-job': await_job})

###################################################################################################

await_instance_parser = subparsers.add_parser('await-instance', help='await instance related to uuid')
await_instance_parser.add_argument('uuid', nargs='+')
await_instance_parser.add_argument('--timeout', '-t', default=30, help='maximum time to wait for an instance to complete')
await_instance_parser.add_argument('--interval', '-i', default=5, help='time to wait between polling for an instance')

def await_instance(client, args, dry_run=False):
    """Waits for job related to UUID."""
    uuids = read_uuids(args.get('uuid'))
    timeout = args.get('timeout')
    interval = args.get('interval')

    if dry_run:
        return (True, {'uuids': uuids, 'timeout': timeout, 'interval': interval})
    else:
        return client.await_instances(uuids, timeout, interval)

actions.update({'await-instance': await_instance})

###################################################################################################

query_job_parser = subparsers.add_parser('query-job', help='query job related to uuid')
query_job_parser.add_argument('uuid', nargs='+')

def query_job(client, args, dry_run=False):
    """Prints info for job related to UUID."""
    uuids = read_uuids(args.get('uuid'))

    if dry_run:
        return (True, {'uuids': uuids})
    else:
        return client.poll_jobs(uuids)

actions.update({'query-job': query_job})

###################################################################################################

query_instance_parser = subparsers.add_parser('query-instance', help='query instance related to uuid')
query_instance_parser.add_argument('uuid', nargs='+')

def query_instance(client, args, dry_run=False):
    """Prints info for job related to UUID."""
    uuids = read_uuids(args.get('uuid'))

    if dry_run:
        return (True, {'uuids': uuids})
    else:
        return client.poll_instances(uuids)

actions.update({'query-instance': query_instance})

###################################################################################################

query_group_parser = subparsers.add_parser('query-group', help='query group related to uuid')
query_group_parser.add_argument('uuid', nargs='+')
query_group_parser.add_argument('--detailed', '-r', help='show additional group statistics',
                                dest='detailed', action='store_true')
query_group_parser.add_argument('--no-detailed',
                                dest='detailed', action='store_false')

def query_group(client, args, dry_run=False):
    """Prints info for group related to UUID."""
    uuids = read_uuids(args.get('uuid'))
    detailed = args.get('detailed')

    if dry_run:
        return (True, {'uuids': uuids, 'detailed': detailed})
    else:
        return client.poll_groups(uuids, detailed)

actions.update({'query-group': query_group})
