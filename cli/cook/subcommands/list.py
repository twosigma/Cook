import argparse
import collections
import datetime
import json
import logging
import time

from tabulate import tabulate

from cook import colors, http
from cook.subcommands.show import query_across_clusters, format_job_status, format_job_memory, format_job_attempts
from cook.util import current_user, print_info, millis_to_date_string

MILLIS_PER_HOUR = 60 * 60 * 1000
DEFAULT_LOOKBACK_HOURS = 6
DEFAULT_LIMIT = 150


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    print(colors.failed('No jobs found in %s.' % clusters_text))


def list_jobs_on_cluster(cluster, state, user, start_ms, end_ms, name, limit):
    """Queries cluster for jobs with the given state / user / time / name"""
    if 'all' in state:
        state_string = 'waiting+running+completed'
    else:
        state_string = '+'.join(state)
    params = {'state': state_string, 'user': user, 'start-ms': start_ms, 'end-ms': end_ms, 'name': name, 'limit': limit}
    jobs = http.make_data_request(cluster, lambda: http.get(cluster, 'list', params=params))
    entities = {'jobs': jobs, 'count': len(jobs)}
    return entities


def query(clusters, state, user, start_ms, end_ms, name, limit):
    """
    Uses query_across_clusters to make the /list
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(list_jobs_on_cluster, cluster, state, user, start_ms, end_ms, name, limit)

    return query_across_clusters(clusters, submit)


def format_job_command(job):
    """Truncates the job command to 47 characters + '...', if necessary"""
    return job['command'] if len(job['command']) <= 50 else ('%s...' % job['command'][:47])


def show_data(cluster_job_pairs):
    """
    Given a collection of (cluster, job) pairs,
    formats a table showing the most relevant job fields
    """
    rows = [collections.OrderedDict([("Cluster", cluster),
                                     ("UUID", job['uuid']),
                                     ("Name", job['name']),
                                     ("Memory", format_job_memory(job)),
                                     ("CPUs", job['cpus']),
                                     ("Priority", job['priority']),
                                     ("Attempts", format_job_attempts(job)),
                                     ("Submitted", millis_to_date_string(job['submit_time'])),
                                     ("Command", format_job_command(job)),
                                     ("Job Status", format_job_status(job))])
            for (cluster, job) in cluster_job_pairs]
    job_table = tabulate(rows, headers='keys', tablefmt='plain')
    print_info(job_table)


def date_time_string_to_ms_since_epoch(date_time_string):
    """Converts the given date_time_string (e.g. '5 minutes ago') to milliseconds since epoch"""
    import tzlocal
    from cook import dateparser
    local_tz = tzlocal.get_localzone()
    dt = dateparser.parse(date_time_string, local_tz)
    if dt:
        import pytz
        logging.debug(f'parsed "{date_time_string}" as {dt}')
        epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
        ms_since_epoch = int((dt - epoch).total_seconds() * 1000)
        logging.debug(f'converted "{date_time_string}" to ms {ms_since_epoch}')
        return ms_since_epoch
    else:
        raise Exception(f'"{date_time_string}" is not a valid date / time string.')


def lookback_hours_to_range(lookback_hours):
    """Converts the given number of hours to a start and end range of milliseconds since epoch"""
    end_ms = int(round(time.time() * 1000))
    lookback_ms = int(lookback_hours * MILLIS_PER_HOUR)
    start_ms = end_ms - lookback_ms
    logging.debug(f'converted lookback {lookback_hours} to start ms {start_ms} and end ms {end_ms}')
    return start_ms, end_ms


def list_jobs(clusters, args):
    """Prints info for the jobs with the given list criteria"""
    as_json = args.get('json')
    states = args.get('states')
    user = args.get('user')
    lookback_hours = args.get('lookback')
    submitted_after = args.get('submitted_after')
    submitted_before = args.get('submitted_before')
    name = args.get('name')
    limit = args.get('limit')

    if lookback_hours and (submitted_after or submitted_before):
        raise Exception('You cannot specify both lookback hours and submitted after / before times.')

    if submitted_after or submitted_before:
        start_ms = date_time_string_to_ms_since_epoch(submitted_after or f'{DEFAULT_LOOKBACK_HOURS} hours ago')
        end_ms = date_time_string_to_ms_since_epoch(submitted_before or 'now')
    else:
        start_ms, end_ms = lookback_hours_to_range(lookback_hours or DEFAULT_LOOKBACK_HOURS)

    query_result = query(clusters, states, user, start_ms, end_ms, name, limit)
    if as_json:
        print(json.dumps(query_result))
    elif query_result['count'] > 0:
        cluster_job_pairs = [(c, j) for c, e in query_result['clusters'].items() for j in e['jobs']]
        show_data(cluster_job_pairs)
    else:
        print_no_data(clusters)
    return 0


def check_positive(value):
    """Checks that the given limit value is a positive integer"""
    try:
        integer = int(value)
    except:
        raise argparse.ArgumentTypeError("%s is not an integer" % value)
    if integer <= 0:
        raise argparse.ArgumentTypeError("%s is not a positive integer" % value)
    return integer


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    list_parser = add_parser('list', help='list jobs by state / user / time / name')
    list_parser.add_argument('--waiting', '-w', help='include waiting jobs', dest='states',
                             action='append_const', const='waiting')
    list_parser.add_argument('--running', '-r', help='include running jobs', dest='states',
                             action='append_const', const='running')
    list_parser.add_argument('--completed', '-c', help='include completed jobs', dest='states',
                             action='append_const', const='completed')
    list_parser.add_argument('--failed', '-f', help='include failed jobs', dest='states',
                             action='append_const', const='failed')
    list_parser.add_argument('--success', '-s', help='include successful jobs', dest='states',
                             action='append_const', const='success')
    list_parser.add_argument('--all', '-a', help='include all jobs, regardless of status', dest='states',
                             action='append_const', const='all')
    list_parser.add_argument('--user', '-u', help='list jobs for a user')
    list_parser.add_argument('--lookback', '-t',
                             help=f'list jobs submitted in the last HOURS hours (default = {DEFAULT_LOOKBACK_HOURS})',
                             type=float, metavar='HOURS')
    list_parser.add_argument('--submitted-after', '-A',
                             help=f'list jobs submitted after the given time')
    list_parser.add_argument('--submitted-before', '-B',
                             help=f'list jobs submitted before the given time')
    list_parser.add_argument('--name', '-n', help="list jobs with a particular name pattern (name filters can contain "
                                                  "alphanumeric characters, '.', '-', '_', and '*' as a wildcard)")
    list_parser.add_argument('--limit', '-l', help=f'limit the number of results (default = {DEFAULT_LIMIT})',
                             type=check_positive)
    list_parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')

    add_defaults('list', {'states': ['running', 'waiting'],
                          'user': current_user(),
                          'limit': DEFAULT_LIMIT})

    return list_jobs
