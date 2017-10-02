import argparse
import json
import logging
import shlex
import sys
import uuid

import pkg_resources

from cook import colors, http, metrics
from cook.util import deep_merge, is_valid_uuid, read_lines, print_info, current_user


def parse_raw_job_spec(job, r):
    """
    Parse a JSON string containing raw job data and merge with job template.
    Job data can either be a dict of job attributes (indicating a single job),
    or a list of dicts (indicating multiple jobs). In either case, the job attributes
    are merged with (and override) the `job` template attributes.
    Throws a ValueError if there is a problem parsing the data.
    """
    try:
        content = json.loads(r)

        if type(content) is dict:
            return [deep_merge(job, content)]
        elif type(content) is list:
            return [deep_merge(job, c) for c in content]
        else:
            raise ValueError('invalid format for raw job')
    except Exception:
        raise ValueError('malformed JSON for raw job')


def submit_succeeded_message(cluster_name, uuids):
    """Generates a successful submission message with the given cluster and uuid(s)"""
    if len(uuids) == 1:
        return 'Job submission %s on %s. Your job UUID is:\n%s' % \
               (colors.success('succeeded'), cluster_name, uuids[0])
    else:
        return 'Job submission %s on %s. Your job UUIDs are:\n%s' % \
               (colors.success('succeeded'), cluster_name, '\n'.join(uuids))


def submit_failed_message(cluster_name, reason):
    """Generates a failed submission message with the given cluster name and reason"""
    return 'Job submission %s on %s:\n%s' % (colors.failed('failed'), cluster_name, colors.reason(reason))


def print_submit_result(cluster, response):
    """
    Parses a submission response from cluster and returns a corresponding message. Note that
    Cook Scheduler returns text when the submission was successful, and JSON when the submission
    failed. Also, in the case of failure, there are different possible shapes for the failure payload.
    """
    cluster_name = cluster['name']
    if response.status_code == 201:
        text = response.text.strip('"')
        if ' submitted groups' in text:
            group_index = text.index(' submitted groups')
            text = text[:group_index]
        uuids = [p for p in text.split() if is_valid_uuid(p)]
        print_info(submit_succeeded_message(cluster_name, uuids), '\n'.join(uuids))
    else:
        try:
            data = response.json()
            if 'errors' in data:
                reason = json.dumps(data['errors'])
            elif 'error' in data:
                reason = data['error']
            else:
                reason = json.dumps(data)
        except json.decoder.JSONDecodeError:
            reason = '%s\n' % response.text
        print_info(submit_failed_message(cluster_name, reason))


def submit_federated(clusters, jobs):
    """
    Attempts to submit the provided jobs to each cluster in clusters, until a cluster
    returns a "created" status code. If no cluster returns "created" status, throws.
    """
    for cluster in clusters:
        cluster_name = cluster['name']
        try:
            print_info('Attempting to submit on %s cluster...' % colors.bold(cluster_name))
            resp = http.post(cluster, 'rawscheduler', {'jobs': jobs})
            print_submit_result(cluster, resp)
            if resp.status_code == 201:
                metrics.inc('command.submit.jobs', len(jobs))
                return 0
        except IOError as ioe:
            logging.info(ioe)
            reason = 'Cannot connect to %s (%s)' % (cluster_name, cluster['url'])
            print_info('%s\n' % submit_failed_message(cluster_name, reason))
    raise Exception(colors.failed('Job submission failed on all of your configured clusters.'))


def read_commands_from_stdin():
    """Prompts for and then reads subcommands, one per line, from stdin"""
    print_info('Enter the subcommands, one per line (press Ctrl+D on a blank line to submit)')
    commands = read_lines()
    if len(commands) < 1:
        raise Exception('You must specify at least one command.')
    return commands


def read_jobs_from_stdin():
    """Prompts for and then reads job(s) JSON from stdin"""
    print('Enter the raw job(s) JSON (press Ctrl+D on a blank line to submit)', file=sys.stderr)
    jobs_json = sys.stdin.read()
    return jobs_json


def acquire_commands(command_args):
    """
    Given the command_args list passed from the command line, returns a
    list of commands to use for job submission. If command_args is None,
    the user will be prompted to supply commands from stdin. Otherwise,
    the returned commands list will only contain 1 element, corresponding
    to the command specified at the command line.
    """
    if command_args:
        if len(command_args) == 1:
            commands = command_args
        else:
            if command_args[0] == '--':
                command_args = command_args[1:]
            commands = [' '.join([shlex.quote(s) for s in command_args])]
    else:
        commands = read_commands_from_stdin()

    logging.info('commands: %s' % commands)
    return commands


def submit(clusters, args):
    """
    Submits a job (or multiple jobs) to cook scheduler. Assembles a list of jobs,
    potentially getting data from configuration, the command line, and stdin.
    """
    logging.debug('submit args: %s' % args)
    job = args
    raw = job.pop('raw', None)
    command_from_command_line = job.pop('command', None)
    application_name = job.pop('application_name', 'cook-scheduler-cli')
    application_version = job.pop('application_version', pkg_resources.require('twosigma.cook-cli')[0].version)
    job['application'] = {'name': application_name, 'version': application_version}

    if raw:
        if command_from_command_line:
            raise Exception('You cannot specify a command at the command line when using --raw/-r.')

        jobs_json = read_jobs_from_stdin()
        jobs = parse_raw_job_spec(job, jobs_json)
    else:
        commands = acquire_commands(command_from_command_line)

        if job.get('uuid') and len(commands) > 1:
            raise Exception('You cannot specify multiple subcommands with a single UUID.')

        if job.get('env'):
            job['env'] = dict([e.split('=', maxsplit=1) for e in job['env']])

        jobs = [deep_merge(job, {'command': c}) for c in commands]

    for j in jobs:
        if not j.get('uuid'):
            j['uuid'] = str(uuid.uuid4())

        if not j.get('name'):
            j['name'] = '%s_job' % current_user()

    logging.debug('jobs: %s' % jobs)
    return submit_federated(clusters, jobs)


def valid_uuid(s):
    """Allows argparse to flag user-provided job uuids as valid or not"""
    if is_valid_uuid(s):
        return str(uuid.UUID(s))
    else:
        raise argparse.ArgumentTypeError('%s is not a valid UUID' % s)


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    submit_parser = add_parser('submit', help='create job for command')
    submit_parser.add_argument('--uuid', '-u', help='uuid of job', type=valid_uuid)
    submit_parser.add_argument('--name', '-n', help='name of job')
    submit_parser.add_argument('--priority', '-p', help='priority of job, between 0 and 100 (inclusive), with 100 '
                                                        'being highest priority (default = 50)',
                               type=int, choices=range(0, 101), metavar='')
    submit_parser.add_argument('--max-retries', help='maximum retries for job',
                               dest='max-retries', type=int, metavar='COUNT')
    submit_parser.add_argument('--max-runtime', help='maximum runtime for job',
                               dest='max-runtime', type=int, metavar='MILLIS')
    submit_parser.add_argument('--cpus', '-c', help='cpus to reserve for job', type=float)
    submit_parser.add_argument('--mem', '-m', help='memory to reserve for job', type=int)
    submit_parser.add_argument('--group', '-g', help='group uuid for job', type=str, metavar='UUID')
    submit_parser.add_argument('--env', '-e', help='environment variable for job (can be repeated)',
                               metavar='KEY=VALUE', action='append')
    submit_parser.add_argument('--ports', help='number of ports to reserve for job', type=int)
    submit_parser.add_argument('--application-name', '-a', help='name of application submitting the job')
    submit_parser.add_argument('--application-version', '-v', help='version of application submitting the job')
    submit_parser.add_argument('--raw', '-r', help='raw job spec in json format', dest='raw', action='store_true')
    submit_parser.add_argument('command', nargs=argparse.REMAINDER)

    add_defaults('submit', {'cpus': 1, 'max-retries': 1, 'mem': 128})

    return submit
