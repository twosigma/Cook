import argparse
import json
import logging
import os
import sys
import uuid
from urllib.parse import urljoin, urlparse

import time

import datetime
import requests
from tabulate import tabulate

from cook.util import merge_dicts, load_first_json_file, read_lines, flatten_list, wait_until, is_valid_uuid

# Default locations to check for configuration files if one isn't given on the command line
DEFAULT_CONFIG_PATHS = [
    '.cook.json',
    os.path.expanduser('~/.cook.json')
]


def process_uuids(uuids):
    """Processes UUIDs from the command line."""
    return [uuid.UUID(u.strip()) for u in uuids]


###################################################################################################

parser = argparse.ArgumentParser(description='cs is the Cook Scheduler CLI')
parser.add_argument('--cluster', '-c', help='the name of the Cook scheduler cluster to use')
parser.add_argument('--url', '-u', help='the url of the Cook scheduler cluster to use')
parser.add_argument('--config', '-C', help='the configuration file to use')
parser.add_argument('--retries', '-r', help='the number of retries to use for HTTP connections', type=int, default=2)
parser.add_argument('--verbose', '-v', help='be more verbose/talkative (useful for debugging)',
                    dest='verbose', action='store_true')

subparsers = parser.add_subparsers(dest='action')

actions = {}

session = requests.Session()


def default_config():
    return {'defaults': {'submit': {'cpus': 1,
                                    'max-retries': 1,
                                    'mem': 128}}}


def cli(args):
    """
    Main entrypoint to the Cook CLI. Loads configuration files, 
    processes global command line arguments, and calls other command 
    line sub-commands (actions) if necessary.
    """
    args = vars(parser.parse_args(args))
    verbose = args.pop('verbose')

    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    if verbose:
        logging.basicConfig(format=log_format, level=logging.DEBUG)
    else:
        logging.basicConfig(format=log_format, level=logging.FATAL)

    logging.debug('args: %s' % args)

    action = args.pop('action')
    config_path = args.pop('config')
    cluster = args.pop('cluster')
    url = args.pop('url')
    retries = args.pop('retries')

    if action is None:
        parser.print_help()
    else:
        if cluster and url:
            raise Exception('You cannot specify both a cluster name and a cluster url at the same time')

        if config_path:
            if os.path.isfile(config_path):
                with open(config_path) as json_file:
                    config = json.load(json_file)
            else:
                raise Exception('The configuration path specified (%s) is not valid' % config_path)
        else:
            config = load_first_json_file(DEFAULT_CONFIG_PATHS) or default_config()
        logging.debug('using configuration: %s' % config)

        defaults = config.get('defaults')
        clusters = config.get('clusters')

        if url:
            if urlparse(url).scheme == '':
                url = 'http://%s' % url
            clusters = [{'name': url, 'url': url}]
        else:
            cluster = cluster or (defaults.get('cluster') if defaults else None)
            if cluster:
                clusters = [c for c in clusters if c.get('name') == cluster]

        if clusters:
            http_adapter = requests.adapters.HTTPAdapter(max_retries=retries)
            session.mount('http://', http_adapter)
            args = {k: v for k, v in args.items() if v is not None}
            action_defaults = (defaults.get(action) if defaults else None) or {}
            result = actions[action](clusters, merge_dicts(action_defaults, args))
            logging.debug('result: %s' % result)
            return result
        else:
            raise Exception('You must specify at least one cluster. Current configuration:\n%s' % config)

    return None


###################################################################################################

submit_parser = subparsers.add_parser('submit', help='create job for command')
submit_parser.add_argument('--uuid', '-u', help='uuid of job')
submit_parser.add_argument('--name', '-n', help='name of job')
submit_parser.add_argument('--priority', '-p', help='priority of job, between 0 and 100 (inclusive)',
                           type=int, choices=range(0, 101), metavar='')
submit_parser.add_argument('--max-retries', help='maximum retries for job', dest='max-retries', type=int)
submit_parser.add_argument('--max-runtime', help='maximum runtime for job', dest='max-runtime', type=int)
submit_parser.add_argument('--cpus', help='cpus to reserve for job', type=float)
submit_parser.add_argument('--mem', help='memory to reserve for job', type=int)
submit_parser.add_argument('--raw', '-r', help='raw job spec in json format', dest='raw', action='store_true')
submit_parser.add_argument('command', nargs='?')
submit_parser.add_argument('args', nargs=argparse.REMAINDER)


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
            return [merge_dicts(job, content)]
        elif type(content) is list:
            return [merge_dicts(job, c) for c in content]
        else:
            raise ValueError('invalid format for raw job')
    except Exception:
        raise ValueError('malformed JSON for raw job')


def make_url(cluster, endpoint):
    """Given a cluster and an endpoint, returns the corresponding full URL"""
    return urljoin(cluster['url'], endpoint)


def make_federated_request(clusters, make_request_fn, success_status, parse_response_fn,
                           description, endpoint='rawscheduler'):
    """
    Attempts to make a request (via make_request_fn) to each cluster in clusters, until a cluster
    returns a status code equal to success_status. If no cluster returns success status, throws.
    """
    for cluster in clusters:
        try:
            url = make_url(cluster, endpoint)
            resp = make_request_fn(url)
            logging.info('response from cook: %s' % resp.text)
            if resp.status_code == success_status:
                return parse_response_fn(resp, cluster)
        except requests.exceptions.ConnectionError as ce:
            logging.info(ce)
    raise Exception('Unable to %s on any of the following clusters: %s' % (description, clusters))


def safe_pop(d, key):
    """If key is present in d, pops and returns the value. Otherwise, returns None."""
    value = d.pop(key) if key in d else None
    return value


def submit(clusters, args):
    """
    Submits a job (or multiple jobs) to cook scheduler. Assembles a list of jobs, potentially getting data 
    from configuration, the command line, and stdin.
    """
    logging.debug('submit args: %s' % args)

    job = args
    raw = safe_pop(job, 'raw')
    command_from_command_line = safe_pop(job, 'command')
    command_args = safe_pop(job, 'args')

    if command_from_command_line:
        commands = ['%s%s' % (command_from_command_line, (' ' + ' '.join(command_args)) if command_args else '')]
    else:
        print('Enter the commands, one per line (press Ctrl+D on a blank line to submit)', file=sys.stderr)
        commands = read_lines()
        if len(commands) < 1:
            raise Exception('You must specify at least one command')
    logging.debug('commands: %s' % commands)

    if job.get('uuid') and len(commands) > 1:
        raise Exception('You cannot specify multiple commands with a single UUID')

    if raw:
        jobs = flatten_list([parse_raw_job_spec(job, c) for c in commands])
    else:
        jobs = [merge_dicts(job, {'command': c}) for c in commands]

    for j in jobs:
        if not j.get('uuid'):
            j['uuid'] = str(uuid.uuid4())

        if not j.get('name'):
            j['name'] = "{0}_{1}".format(os.environ['USER'], uuid.uuid4())

    request_body = {'jobs': jobs}
    return make_federated_request(clusters, lambda u: session.post(u, json=request_body), 201,
                                  lambda r, _: [p for p in r.text.strip('"').split() if is_valid_uuid(p)],
                                  'create job(s)')


actions.update({'submit': submit})

###################################################################################################

wait_parser = subparsers.add_parser('wait', help='wait for job(s) to complete by uuid')
wait_parser.add_argument('uuid', nargs='+')
wait_parser.add_argument('--timeout', '-t', default=30, help='maximum time (in seconds) to wait', type=int)
wait_parser.add_argument('--interval', '-i', default=5, help='time (in seconds) to wait between polling', type=int)


def fetch_jobs_on_cluster(cluster, uuids):
    """Attempts to fetch jobs corresponding to the given uuids from cluster."""
    url = make_url(cluster, 'rawscheduler')
    resp = session.get(url, params={'job': uuids, 'partial': 'true'})
    logging.info('response from cook: %s' % resp.text)
    return resp


def fetch_jobs(clusters, args, pred=None, timeout=None, interval=None):
    """
    Attempts to fetch jobs from the given clusters. The job uuids are provided in args. Optionally
    accepts a predicate, pred, which must be satisfied within the timeout.
    """
    uuids = process_uuids(args.get('uuid'))

    def jobs_satisfy_pred(cluster_, uuids_):
        resp_ = fetch_jobs_on_cluster(cluster_, uuids_)
        return pred(resp_.json())

    all_jobs = []
    for cluster in clusters:
        try:
            resp = fetch_jobs_on_cluster(cluster, uuids)
            if resp.status_code == 200:
                jobs = resp.json()
                if pred and not pred(jobs):
                    jobs = wait_until(lambda: jobs_satisfy_pred(cluster, uuids), timeout, interval)
                    if not jobs:
                        raise Exception('Timeout waiting for jobs')
                all_jobs.extend(jobs)
                for job in jobs:
                    uuids.remove(uuid.UUID(job['uuid']))
                if len(uuids) == 0:
                    return all_jobs
        except requests.exceptions.ConnectionError as ce:
            logging.info(ce)
    raise Exception('Unable to fetch job(s) on the following cluster(s): %s' % clusters)


def all_jobs_completed(jobs):
    """Returns jobs if they are all completed, otherwise False."""
    if not [job for job in jobs if job.get('status') != 'completed']:
        return jobs
    else:
        return False


def wait(clusters, args):
    """Waits for job(s) with the given UUID(s) to complete."""
    timeout = args.get('timeout')
    interval = args.get('interval')
    fetch_jobs(clusters, args, all_jobs_completed, timeout, interval)


actions.update({'wait': wait})

###################################################################################################

show_parser = subparsers.add_parser('show', help='show job(s) by uuid')
show_parser.add_argument('uuid', nargs='+')
show_parser.add_argument('--json', help='show the job(s) in JSON format', dest='json', action='store_true')


def millis_to_timedelta(ms):
    """Converts milliseconds to a timedelta for display on screen"""
    return 'MAX_LONG' if ms == sys.maxsize else datetime.timedelta(milliseconds=ms)


def millis_to_date_string(ms):
    """Converts milliseconds to a date string for display on screen"""
    s, ms = divmod(ms, 1000)
    string = '%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s)), ms)
    return string


def format_timestamps(instance):
    """Given an instance, formats the timestamps"""
    instance['end_time'] = millis_to_date_string(instance['end_time'])
    instance['start_time'] = millis_to_date_string(instance['start_time'])
    instance['mesos_start_time'] = millis_to_date_string(instance['mesos_start_time'])
    return instance


def tabulate_job(job):
    """Given a job, returns a string containing tables for the job and instance fields"""
    headers = ['Field', 'Value']
    instances = job.pop('instances')
    job['max_runtime'] = millis_to_timedelta(job['max_runtime'])
    job['submit_time'] = millis_to_date_string(job['submit_time'])
    job_table = tabulate(sorted(job.items()), headers=headers)
    instance_table = tabulate([format_timestamps(i) for i in instances], headers='keys')
    return 'Job:\n\n%s\n\nInstances:\n\n%s' % (job_table, instance_table)


def show(clusters, args):
    """Prints info for the job(s) with the given UUID(s)."""
    as_json = args.get('json')
    jobs = fetch_jobs(clusters, args)
    if as_json:
        return json.dumps(jobs)
    else:
        return '\n\n==========\n'.join([tabulate_job(job) for job in jobs])


actions.update({'show': show})
