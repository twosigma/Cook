import argparse
import datetime
import json
import logging
import shlex
import sys
import uuid

import requests

from cook import terminal, http, metrics, version
from cook.util import deep_merge, is_valid_uuid, read_lines, print_info, print_error, current_user, guard_no_cluster, check_positive


def make_temporal_uuid():
    """Make a UUID object that has a datestamp as its prefix. The datestamp being yymmddhh. This will cluster
    UUID's in a temporal manner, so jobs submitted on the same day or week will be clustered together in the
    datomic storage"""
    base_uuid = uuid.uuid4()
    now = datetime.datetime.now()
    date_prefix = now.strftime("%y%m%d%H")
    suffix = str(base_uuid)[8:]
    temporal_uuid = uuid.UUID(date_prefix+suffix)
    return temporal_uuid

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
               (terminal.success('succeeded'), cluster_name, uuids[0])
    else:
        return 'Job submission %s on %s. Your job UUIDs are:\n%s' % \
               (terminal.success('succeeded'), cluster_name, '\n'.join(uuids))


def submit_failed_message(cluster_name, reason):
    """Generates a failed submission message with the given cluster name and reason"""
    return 'Job submission %s on %s:\n%s' % (terminal.failed('failed'), cluster_name, terminal.reason(reason))


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
        print_error(submit_failed_message(cluster_name, reason))


def submit_federated(clusters, jobs, group, pool):
    """
    Attempts to submit the provided jobs to each cluster in clusters, until a cluster
    returns a "created" status code. If no cluster returns "created" status, throws.
    """
    messages = ""
    for cluster in clusters:
        cluster_name = cluster['name']
        cluster_url = cluster['url']
        try:
            print_info('Attempting to submit on %s cluster...' % terminal.bold(cluster_name))

            json_body = {'jobs': jobs}
            if group:
                json_body['groups'] = [group]
            if pool:
                json_body['pool'] = pool

            resp = http.post(cluster, 'jobs', json_body)
            print_submit_result(cluster, resp)
            if resp.status_code == 201:
                metrics.inc('command.submit.jobs', len(jobs))
                return 0
        except requests.exceptions.ReadTimeout as rt:
            logging.exception(rt)
            print_info(terminal.failed(
                f'Encountered read timeout with {cluster_name} ({cluster_url}). Your submission may have completed.'))
            return 1
        except IOError as ioe:
            logging.exception(ioe)
            reason = f'Cannot connect to {cluster_name} ({cluster_url})'
            message = submit_failed_message(cluster_name, reason)
            messages += message
    print_error(messages)
    raise Exception(terminal.failed('Job submission failed on all of your configured clusters.'))


def read_commands_from_stdin():
    """Prompts for and then reads commands, one per line, from stdin"""
    print_info('Enter the commands, one per line (press Ctrl+D on a blank line to submit)')
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


def submit(clusters, args, _):
    """
    Submits a job (or multiple jobs) to cook scheduler. Assembles a list of jobs,
    potentially getting data from configuration, the command line, and stdin.
    """
    guard_no_cluster(clusters)
    logging.debug('submit args: %s' % args)
    job_template = args
    raw = job_template.pop('raw', None)
    command_from_command_line = job_template.pop('command', None)
    command_prefix = job_template.pop('command-prefix')
    application_name = job_template.pop('application-name', 'cook-scheduler-cli')
    application_version = job_template.pop('application-version', version.VERSION)
    job_template['application'] = {'name': application_name, 'version': application_version}
    pool = job_template.pop('pool-name', None)
    checkpoint = job_template.pop('checkpoint', False)
    checkpoint_mode = job_template.pop('checkpoint-mode', None)
    checkpoint_preserve_paths = job_template.pop('checkpoint-preserve-paths', None)
    checkpoint_period_sec = job_template.pop('checkpoint-period-sec', None)
    disk_request = job_template.pop('disk-request', None)
    disk_limit = job_template.pop('disk-limit', None)
    disk_type = job_template.pop('disk-type', None)

    docker_image = job_template.pop('docker-image', None)
    if docker_image:
        job_template['container'] = {
            'type': 'docker',
            'docker': {
                'image': docker_image,
                'network': 'HOST',
                'force-pull-image': False
            }
        }

    group = None
    if 'group-name' in job_template:
        # If the user did not also specify a group uuid, generate
        # one for them, and place the job(s) into the group
        if 'group' not in job_template:
            job_template['group'] = str(make_temporal_uuid())

        # The group name is specified on the group object
        group = {'name': job_template.pop('group-name'), 'uuid': job_template['group']}

    if raw:
        if command_from_command_line:
            raise Exception('You cannot specify a command at the command line when using --raw/-r.')

        jobs_json = read_jobs_from_stdin()
        jobs = parse_raw_job_spec(job_template, jobs_json)
    else:
        commands = acquire_commands(command_from_command_line)

        if job_template.get('uuid') and len(commands) > 1:
            raise Exception('You cannot specify multiple subcommands with a single UUID.')

        if job_template.get('env'):
            job_template['env'] = dict([e.split('=', maxsplit=1) for e in job_template['env']])

        if job_template.get('label'):
            labels = dict([l.split('=', maxsplit=1) for l in job_template['label']])
            job_template.pop('label')
            if 'labels' not in job_template:
                job_template['labels'] = {}
            job_template['labels'].update(labels)

        jobs = [deep_merge(job_template, {'command': c}) for c in commands]

    for job in jobs:
        if not job.get('uuid'):
            job['uuid'] = str(make_temporal_uuid())

        if not job.get('name'):
            job['name'] = '%s_job' % current_user()

        if command_prefix:
            job['command'] = f'{command_prefix}{job["command"]}'

        if checkpoint or checkpoint_mode:
            checkpoint = {'mode': checkpoint_mode if checkpoint_mode else 'auto'}
            if checkpoint_preserve_paths:
                checkpoint['options'] = {'preserve-paths': checkpoint_preserve_paths}
            if checkpoint_period_sec:
                checkpoint['periodic-options'] = {'period-sec': checkpoint_period_sec}
            job['checkpoint'] = checkpoint

        if disk_request or disk_limit or disk_type:
            disk = {}
            if disk_request:
                disk['request'] = disk_request
            if disk_limit:
                disk['limit'] = disk_limit
            if disk_type:
                disk['type'] = disk_type
            job['disk'] = disk
    logging.debug('jobs: %s' % jobs)
    return submit_federated(clusters, jobs, group, pool)


def valid_uuid(s):
    """Allows argparse to flag user-provided job uuids as valid or not"""
    if is_valid_uuid(s):
        return str(uuid.UUID(s))
    else:
        raise argparse.ArgumentTypeError('%s is not a valid UUID' % s)

def valid_priority(value):
    """Checks that the given value is a valid priority"""
    try:
        integer = int(value)
    except:
        raise argparse.ArgumentTypeError(f'{value} is not an integer')
    if integer < 0 or integer > 16000000:
        raise argparse.ArgumentTypeError(f'Job priority must be between 0 and 16,000,000 (inclusive)')
    return integer

def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    submit_parser = add_parser('submit', help='create job for command')
    submit_parser.add_argument('--uuid', '-u', help='uuid of job', type=valid_uuid)
    submit_parser.add_argument('--name', '-n', help='name of job')
    submit_parser.add_argument('--priority', '-p', help='Per-user job priority. Allows values between 0 and 16,000,000 '
                                                        '(inclusive), with higher values having higher priority. '
                                                        'Defaults to 50 and typically set between 0 and 100.',
                               type=valid_priority, metavar='')
    submit_parser.add_argument('--max-retries', help='maximum retries for job',
                               dest='max-retries', type=int, metavar='COUNT')
    submit_parser.add_argument('--max-runtime', help='maximum runtime for job',
                               dest='max-runtime', type=int, metavar='MILLIS')
    submit_parser.add_argument('--cpus', '-c', help='cpus to reserve for job', type=float)
    submit_parser.add_argument('--mem', '-m', help='memory to reserve for job', type=int)
    submit_parser.add_argument('--gpus', help='gpus to reserve for job', type=check_positive)
    submit_parser.add_argument('--disk-request', help='disk request for job', type=float, dest='disk-request')
    submit_parser.add_argument('--disk-limit', help='disk limit for job', type=float, dest='disk-limit')
    submit_parser.add_argument('--disk-type', help='disk type for job', type=str, dest='disk-type')
    submit_parser.add_argument('--group', '-g', help='group uuid for job', type=str, metavar='UUID')
    submit_parser.add_argument('--group-name', '-G', help='group name for job',
                               type=str, metavar='NAME', dest='group-name')
    submit_parser.add_argument('--env', '-e', help='environment variable for job (can be repeated)',
                               metavar='KEY=VALUE', action='append')
    submit_parser.add_argument('--ports', help='number of ports to reserve for job', type=int)
    submit_parser.add_argument('--application-name', '-a', help='name of application submitting the job',
                               dest='application-name')
    submit_parser.add_argument('--application-version', '-v', help='version of application submitting the job',
                               dest='application-version')
    submit_parser.add_argument('--executor', '-E', help='executor to use to run the job on the Mesos agent',
                               choices=('cook', 'mesos'))
    submit_parser.add_argument('--raw', '-r', help='raw job spec in json format', dest='raw', action='store_true')
    submit_parser.add_argument('--command-prefix', help='prefix to use for all commands', dest='command-prefix')
    submit_parser.add_argument('--pool', '-P', help='pool name for job', type=str, metavar='NAME', dest='pool-name')
    submit_parser.add_argument('--docker-image', '-i', help='docker image for job',
                               type=str, metavar='IMAGE', dest='docker-image')
    submit_parser.add_argument('--label', '-l', help='label for job (can be repeated)',
                               metavar='KEY=VALUE', action='append')
    submit_parser.add_argument('--checkpoint', help='enable automatically checkpointing and restoring the application',
                               dest='checkpoint', action='store_true')
    submit_parser.add_argument('--checkpoint-mode', help='checkpointing mode, defaults to auto',
                               dest='checkpoint-mode',  choices=['auto', 'periodic', 'preemption'])
    submit_parser.add_argument('--checkpoint-preserve-path',help='path to preserve when making a checkpoint '
                                                                 '(can be repeated to specify multiple paths)',
                               dest='checkpoint-preserve-paths', action='append', metavar='PATH')
    submit_parser.add_argument('--checkpoint-period-sec', help='checkpointing period in seconds',
                               dest='checkpoint-period-sec', type=int)
    submit_parser.add_argument('command', nargs=argparse.REMAINDER)

    add_defaults('submit', {'cpus': 1, 'max-retries': 1, 'mem': 128, 'command-prefix': ''})

    return submit
