import json
import sys

from tabulate import tabulate

from cook import http, terminal
from cook.format import format_job_memory, format_memory_amount
from cook.querying import query_across_clusters, make_job_request
from cook.util import guard_no_cluster, current_user, print_info, print_error

def get_job_data(cluster, usage_map):
    """Gets data for jobs in usage map if it has any"""
    ungrouped_running_job_uuids = usage_map['ungrouped']['running_jobs']
    job_uuids_to_retrieve = ungrouped_running_job_uuids[:]
    grouped = usage_map['grouped']

    group_uuid_to_name = {}
    for group_usage in grouped:
        group = group_usage['group']
        job_uuids_to_retrieve.extend(group['running_jobs'])
        group_uuid_to_name[group['uuid']] = group['name']

    applications = {}
    num_running_jobs = len(job_uuids_to_retrieve)

    if num_running_jobs > 0:
        jobs = http.make_data_request(cluster, lambda: make_job_request(cluster, job_uuids_to_retrieve))
        for job in jobs:
            application = job['application']['name'] if 'application' in job else None
            if 'groups' in job:
                group_uuids = job['groups']
                group = f'{group_uuid_to_name[group_uuids[0]]} ({group_uuids[0]})' if group_uuids else None
            else:
                group = None

            if application not in applications:
                applications[application] = {'usage': {'cpus': 0, 'mem': 0, 'gpus': 0}, 'groups': {}}

            applications[application]['usage']['cpus'] += job['cpus']
            applications[application]['usage']['mem'] += job['mem']
            applications[application]['usage']['gpus'] += job['gpus']

            if group not in applications[application]['groups']:
                applications[application]['groups'][group] = {'usage': {'cpus': 0, 'mem': 0, 'gpus': 0}, 'jobs': []}

            applications[application]['groups'][group]['usage']['cpus'] += job['cpus']
            applications[application]['groups'][group]['usage']['mem'] += job['mem']
            applications[application]['groups'][group]['usage']['gpus'] += job['gpus']
            applications[application]['groups'][group]['jobs'].append(job['uuid'])

    return {'count': num_running_jobs,
            'applications': applications}

def get_usage_on_cluster(cluster, user):
    """Queries cluster for usage information for the given user"""
    params = {'user': user, 'group_breakdown': 'true'}
    usage_map = http.make_data_request(cluster, lambda: http.get(cluster, 'usage', params=params))
    if not usage_map:
        print_error(f'Unable to retrieve usage information on {cluster["name"]} ({cluster["url"]}).')
        return {'count': 0}

    using_pools = 'pools' in usage_map
    pool_names = usage_map['pools'].keys() if using_pools else []

    share_map = http.make_data_request(cluster, lambda: http.get(cluster, 'share', params={'user': user}))
    if not share_map:
        print_error(f'Unable to retrieve share information on {cluster["name"]} ({cluster["url"]}).')
        return {'count': 0}

    if using_pools != ('pools' in share_map):
        print_error(f'Share information on {cluster["name"]} ({cluster["url"]}) is invalid. '
                    f'Usage information is{"" if using_pools else " not"} per pool, but share '
                    f'is{"" if not using_pools else " not"}')
        return {'count': 0}
    if pool_names != (share_map['pools'].keys() if using_pools else []):
        print_error(f'Share information on {cluster["name"]} ({cluster["url"]}) is invalid. '
                    f'Usage information has pools: {pool_names}, but share '
                    f'has pools: {share_map["pools"].keys()}')
        return {'count': 0}

    quota_map = http.make_data_request(cluster, lambda: http.get(cluster, 'quota', params={'user': user}))
    if not quota_map:
        print_error(f'Unable to retrieve quota information on {cluster["name"]} ({cluster["url"]}).')
        return {'count': 0}

    if using_pools != ('pools' in quota_map):
        print_error(f'Quota information on {cluster["name"]} ({cluster["url"]}) is invalid. '
                    f'Usage information is{"" if using_pools else " not"} per pool, but quota '
                    f'is{"" if not using_pools else " not"}')
        return {'count': 0}
    if pool_names != (quota_map['pools'].keys() if using_pools else []):
        print_error(f'Quota information on {cluster["name"]} ({cluster["url"]}) is invalid. '
                    f'Usage information has pools: {pool_names}, but quota '
                    f'has pools: {quota_map["pools"].keys()}')
        return {'count': 0}

    def make_query_result(using_pools, usage_map, share_map, quota_map, pool_data=None):
        query_result = {'using_pools': using_pools,
                        'usage': usage_map['total_usage'],
                        'share': share_map,
                        'quota': quota_map}
        query_result.update(get_job_data(cluster, usage_map))
        if pool_data:
            query_result.update(pool_data)
        return query_result

    if using_pools:
        pools = http.make_data_request(cluster, lambda: http.get(cluster, 'pools', params={}))
        pools_dict = {pool['name']: pool for pool in pools}
        for pool_name in pool_names:
            if pool_name not in pools_dict or 'state' not in pools_dict[pool_name]:
                print_error(f'Pool information on {cluster["name"]} ({cluster["url"]}) is invalid. '
                            f'Can\'t determine the state of pool {pool_name}')
                return {'count': 0}
        query_result = {'using_pools': using_pools,
                        'pools': {pool_name: make_query_result(using_pools,
                                                               usage_map['pools'][pool_name],
                                                               share_map['pools'][pool_name],
                                                               quota_map['pools'][pool_name],
                                                               {'state': pools_dict[pool_name]['state']})
                                  for pool_name in pool_names}}
        return query_result
    else:
        return make_query_result(using_pools, usage_map, share_map, quota_map)

def query(clusters, user):
    """
    Uses query_across_clusters to make the /usage
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(get_usage_on_cluster, cluster, user)

    return query_across_clusters(clusters, submit)

def print_as_json(query_result):
    """Prints the query result as raw JSON"""
    print(json.dumps(query_result))


def format_cpus(n):
    """Formats n as a number of CPUs"""
    return '{:.1f}'.format(n)


def format_usage(usage_map):
    """Given a "usage map" with cpus, mem, and gpus, returns a formatted usage string"""
    cpus = usage_map['cpus']
    gpus = usage_map['gpus']
    s = f'Usage: {format_cpus(cpus)} CPU{"s" if cpus > 1 else ""}, {format_job_memory(usage_map)} Memory'
    if gpus > 0:
        s += f', {gpus} GPU{"s" if gpus > 1 else ""}'
    return s

def print_formatted_cluster_or_pool_usage(cluster_or_pool, cluster_or_pool_usage):
    """Prints the query result for a cluster or pool in a cluster as a hierarchical set of bullets"""
    usage_map = cluster_or_pool_usage['usage']
    share_map = cluster_or_pool_usage['share']
    quota_map = cluster_or_pool_usage['quota']
    print_info(terminal.bold(cluster_or_pool))

    format_limit = lambda limit, formatter=(lambda x: x): \
        'Unlimited' if limit == sys.float_info.max else formatter(limit)

    rows = [
        ['Max Quota',
         format_limit(quota_map['cpus']),
         format_limit(quota_map['mem'], format_memory_amount),
         format_limit(quota_map['gpus']),
         'Unlimited' if quota_map['count'] == (2**31 - 1) else quota_map['count']],
        ['Non-preemptible Share',
         format_limit(share_map['cpus']),
         format_limit(share_map['mem'], format_memory_amount),
         format_limit(share_map['gpus']),
         'N/A'],
        ['Current Usage',
         usage_map['cpus'],
         format_job_memory(usage_map),
         usage_map['gpus'],
         usage_map['jobs']]
    ]
    print_info(tabulate(rows, headers=['', 'CPUs', 'Memory', 'GPUs', 'Jobs'], tablefmt='plain'))

    applications = cluster_or_pool_usage['applications']
    if applications:
        print_info('Applications:')
    for application, application_usage in applications.items():
        usage_map = application_usage['usage']
        print_info(f'- {terminal.running(application if application else "[no application defined]")}')
        print_info(f'  {format_usage(usage_map)}')
        print_info('  Job Groups:')
        for group, group_usage in application_usage['groups'].items():
            usage_map = group_usage['usage']
            jobs = group_usage['jobs']
            print_info(f'\t- {terminal.bold(group if group else "[ungrouped]")}')
            print_info(f'\t  {format_usage(usage_map)}')
            print_info(f'\t  Jobs: {len(jobs)}')
            print_info('')
    print_info('')

def print_formatted(query_result):
    """Prints the query result as a hierarchical set of bullets"""
    for cluster, cluster_usage in query_result['clusters'].items():
        if 'using_pools' in cluster_usage:
            if cluster_usage['using_pools']:
                for pool, pool_usage in cluster_usage['pools'].items():
                    state = ' (inactive)' if pool_usage['state'] == 'inactive' else ''
                    print_formatted_cluster_or_pool_usage(f'{cluster} - {pool}{state}', pool_usage)
            else:
                print_formatted_cluster_or_pool_usage(cluster, cluster_usage)

def filter_query_result_by_pools(query_result, pools):
    """Filter query result if pools are provided. Return warning message if some of the pools not found in any cluster"""

    clusters = []
    known_pools = []

    pools_set = set(pools)
    filtered_clusters = {}
    for cluster, cluster_usage in query_result['clusters'].items():
        clusters.append(cluster)
        if cluster_usage['using_pools']:
            filtered_pools = {}
            for pool, pool_usage in cluster_usage['pools'].items():
                known_pools.append(pool)
                if pool in pools_set:
                    filtered_pools[pool] = pool_usage
            if filtered_pools:
                filtered_clusters[cluster] = cluster_usage
                cluster_usage['pools'] = filtered_pools
    query_result['clusters'] = filtered_clusters

    missing_pools = pools_set.difference(set(known_pools))
    if missing_pools:
        print_error((f"{list(missing_pools)[0]} is not a valid pool in "
                     if len(missing_pools) == 1 else
                     f"{' / '.join(missing_pools)} are not valid pools in ") +
                    (clusters[0]
                     if len(clusters) == 1 else
                     ' / '.join(clusters)) +
                    '.')
        if query_result['clusters'].items():
            print_error('')

    return query_result

def usage(clusters, args, _):
    """Prints cluster usage info for the given user"""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    user = args.get('user')
    pools = args.get('pool')

    query_result = query(clusters, user)

    if pools:
        query_result = filter_query_result_by_pools(query_result, pools)

    if as_json:
        print_as_json(query_result)
    else:
        print_formatted(query_result)

    return 0


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('usage', help='show breakdown of usage by application and group')
    parser.add_argument('--user', '-u', help='show usage for a user')
    parser.add_argument('--pool', '-p', action='append', help='filter by pool (can be repeated)')
    parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')

    add_defaults('usage', {'user': current_user()})

    return usage
