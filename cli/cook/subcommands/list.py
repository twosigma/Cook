import argparse
import collections
import json
import time

from tabulate import tabulate

from cook import colors, http
from cook.subcommands.show import query_across_clusters, format_job_status, format_job_memory, format_job_attempts
from cook.util import current_user, print_info, millis_to_date_string

MILLIS_PER_HOUR = 60 * 60 * 1000


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    print(colors.failed('No jobs found in %s.' % clusters_text))


def list_jobs_on_cluster(cluster, state, user, lookback_hours, name, limit):
    """Queries cluster for jobs with the given state / user / time / name"""
    now_ms = int(round(time.time() * 1000))
    lookback_ms = int(lookback_hours * MILLIS_PER_HOUR)
    start_ms = now_ms - lookback_ms
    if 'all' in state:
        state_string = 'waiting+running+completed'
    else:
        state_string = '+'.join(state)
    params = {'state': state_string, 'user': user, 'start-ms': start_ms, 'name': name, 'limit': limit}
    jobs = http.make_data_request(lambda: http.get(cluster, 'list', params=params))
    entities = {'jobs': jobs, 'count': len(jobs)}
    return entities


def query(clusters, state, user, lookback_hours, name, limit):
    """
    Uses query_across_clusters to make the /list
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(list_jobs_on_cluster, cluster, state, user, lookback_hours, name, limit)

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


def list_jobs(clusters, args):
    """Prints info for the jobs with the given list criteria"""
    as_json = args.get('json')
    state = args.get('state')
    user = args.get('user')
    lookback_hours = args.get('lookback')
    name = args.get('name')
    limit = args.get('limit')

    query_result = query(clusters, state, user, lookback_hours, name, limit)
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
    list_parser.add_argument('--state', '-s', help='list jobs by status (can be repeated)', action='append',
                             choices=('waiting', 'running', 'completed', 'failed', 'success', 'all'))
    list_parser.add_argument('--user', '-u', help='list jobs for a user')
    list_parser.add_argument('--lookback', '-t', help='list jobs for the last X hours', type=float)
    list_parser.add_argument('--name', '-n', help="list jobs with a particular name pattern (name filters can contain "
                                                  "alphanumeric characters, '.', '-', '_', and '*' as a wildcard)")
    list_parser.add_argument('--limit', '-l', help='limit the number of results', type=check_positive)
    list_parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')

    add_defaults('list', {'state': ['running'], 'user': current_user(), 'lookback': 6, 'limit': 150})

    return list_jobs
