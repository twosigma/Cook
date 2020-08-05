import collections
import json
import logging
import time
from urllib.parse import urljoin

from tabulate import tabulate

from cook import terminal, http
from cook.format import format_job_memory, format_job_attempts, format_job_status
from cook.querying import query_across_clusters
from cook.util import check_positive, current_user, date_time_string_to_ms_since_epoch, guard_no_cluster, \
    millis_to_date_string, print_info

MILLIS_PER_HOUR = 60 * 60 * 1000
DEFAULT_LOOKBACK_HOURS = 6
DEFAULT_LIMIT = 150


def print_no_data(clusters, states, user):
    """Prints a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    if 'all' in states:
        states = ['waiting', 'running', 'completed']
    elif 'success' in states:
        states.remove('success')
        states.append('successful')
    states_text = ' / '.join(states)
    print(terminal.failed(f'No matching {states_text} jobs for {user} found in {clusters_text}.'))


def list_jobs_on_cluster(cluster, state, user, start_ms, end_ms, name, limit, include_custom_executor, pool):
    """Queries cluster for jobs with the given state / user / time / name"""
    if 'all' in state:
        state = ['waiting', 'running', 'completed']
    params = {'user': user, 'name': name, 'limit': limit}
    if include_custom_executor:
        params['state'] = state
        params['start'] = start_ms
        params['end'] = end_ms
        params['pool'] = pool
        jobs = http.make_data_request(cluster, lambda: http.get(cluster, 'jobs', params=params))
    else:
        params['state'] = '+'.join(state)
        params['start-ms'] = start_ms
        params['end-ms'] = end_ms
        jobs = http.make_data_request(cluster, lambda: http.get(cluster, 'list', params=params))
    entities = {'jobs': jobs, 'count': len(jobs)}
    return entities


def query(clusters, state, user, start_ms, end_ms, name, limit, include_custom_executor, pool):
    """
    Uses query_across_clusters to make the /list
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(list_jobs_on_cluster, cluster, state, user, start_ms,
                               end_ms, name, limit, include_custom_executor, pool)

    return query_across_clusters(clusters, submit)


def format_job_command(job):
    """Truncates the job command to 47 characters + '...', if necessary"""
    return job['command'] if len(job['command']) <= 50 else ('%s...' % job['command'][:47])


def query_result_to_cluster_job_pairs(query_result):
    """Given a query result structure, returns a sequence of (cluster, job) pairs from the result"""
    cluster_job_pairs = ((c, j) for c, e in query_result['clusters'].items() for j in e['jobs'])
    return cluster_job_pairs


def print_as_one_per_line(query_result, clusters):
    """Prints one entity ref URL per line from the given query result"""
    cluster_job_pairs = query_result_to_cluster_job_pairs(query_result)
    lines = []
    for cluster_name, job in cluster_job_pairs:
        cluster_url = next(c['url'] for c in clusters if c['name'] == cluster_name)
        jobs_endpoint = urljoin(cluster_url, 'jobs/')
        job_url = urljoin(jobs_endpoint, job['uuid'])
        lines.append(job_url)

    if lines:
        print('\n'.join(lines))


def print_as_table(query_result):
    """Given a collection of (cluster, job) pairs, formats a table showing the most relevant job fields"""
    cluster_job_pairs = query_result_to_cluster_job_pairs(query_result)
    rows = [collections.OrderedDict([("Cluster", cluster),
                                     ("Pool", job.get('pool', '-')),
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


def print_as_json(query_result):
    """Prints the query result as raw JSON"""
    print(json.dumps(query_result))


def lookback_hours_to_range(lookback_hours):
    """Converts the given number of hours to a start and end range of milliseconds since epoch"""
    end_ms = int(round(time.time() * 1000))
    lookback_ms = int(lookback_hours * MILLIS_PER_HOUR)
    start_ms = end_ms - lookback_ms
    logging.debug(f'converted lookback {lookback_hours} to start ms {start_ms} and end ms {end_ms}')
    return start_ms, end_ms


def jobs(clusters, args, _):
    """Prints info for the jobs with the given list criteria"""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    one_per_line = args.get('one-per-line')
    states = args.get('states')
    user = args.get('user')
    lookback_hours = args.get('lookback')
    submitted_after = args.get('submitted_after')
    submitted_before = args.get('submitted_before')
    name = args.get('name')
    limit = args.get('limit')
    include_custom_executor = not args.get('exclude_custom_executor')
    pool = args.get('pool')

    if lookback_hours and (submitted_after or submitted_before):
        raise Exception('You cannot specify both lookback hours and submitted after / before times.')

    if submitted_after is not None or submitted_before is not None:
        start_ms = date_time_string_to_ms_since_epoch(
            submitted_after if submitted_after is not None else f'{DEFAULT_LOOKBACK_HOURS} hours ago')
        end_ms = date_time_string_to_ms_since_epoch(
            submitted_before if submitted_before is not None else 'now')
    else:
        if states == ['running']:
            default_lookback_hours = 24 * 7
        else:
            default_lookback_hours = DEFAULT_LOOKBACK_HOURS
        start_ms, end_ms = lookback_hours_to_range(lookback_hours or default_lookback_hours)

    query_result = query(clusters, states, user, start_ms, end_ms, name, limit, include_custom_executor, pool)
    found_jobs = query_result['count'] > 0
    if as_json:
        print_as_json(query_result)
    elif one_per_line:
        print_as_one_per_line(query_result, clusters)
    elif found_jobs:
        print_as_table(query_result)
    else:
        print_no_data(clusters, states, user)
    return 0


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('jobs', help='list jobs by state / user / time / name')
    parser.add_argument('--waiting', '-w', help='include waiting jobs', dest='states',
                        action='append_const', const='waiting')
    parser.add_argument('--running', '-r', help='include running jobs', dest='states',
                        action='append_const', const='running')
    parser.add_argument('--completed', '-c', help='include completed jobs', dest='states',
                        action='append_const', const='completed')
    parser.add_argument('--failed', '-f', help='include failed jobs', dest='states',
                        action='append_const', const='failed')
    parser.add_argument('--success', '-s', help='include successful jobs', dest='states',
                        action='append_const', const='success')
    parser.add_argument('--all', '-a', help='include all jobs, regardless of status', dest='states',
                        action='append_const', const='all')
    parser.add_argument('--user', '-u', help='list jobs for a user')
    parser.add_argument('--pool', '-P', help='list jobs for a pool')
    parser.add_argument('--lookback', '-t',
                        help=f'list jobs submitted in the last HOURS hours (default = {DEFAULT_LOOKBACK_HOURS})',
                        type=float, metavar='HOURS')
    parser.add_argument('--submitted-after', '-A',
                        help=f'list jobs submitted after the given time')
    parser.add_argument('--submitted-before', '-B',
                        help=f'list jobs submitted before the given time')
    parser.add_argument('--name', '-n', help="list jobs with a particular name pattern (name filters can contain "
                                             "alphanumeric characters, '.', '-', '_', and '*' as a wildcard)")
    parser.add_argument('--limit', '-l', help=f'limit the number of results (default = {DEFAULT_LIMIT})',
                        type=check_positive)
    parser.add_argument('--exclude-custom-executor', help=f'exclude jobs with a custom executor', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    group.add_argument('--urls', '-1', help='list one job URL per line, without table formatting',
                       dest='one-per-line', action='store_true')

    add_defaults('jobs', {'states': ['running'],
                          'user': current_user(),
                          'limit': DEFAULT_LIMIT})

    return jobs
