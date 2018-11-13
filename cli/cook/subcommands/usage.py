import json
import sys

from cook import http, colors
from cook.format import format_job_memory
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

    def make_query_result(using_pools, usage_map, share_map):
        query_result = {'using_pools': using_pools,
                        'usage': usage_map['total_usage'],
                        'share': share_map}
        query_result.update(get_job_data(cluster, usage_map))
        return query_result

    if using_pools:
        query_result = {'using_pools': using_pools,
                        'pools': {pool_name: make_query_result(using_pools,
                                                               usage_map['pools'][pool_name],
                                                               share_map['pools'][pool_name])
                                  for pool_name in pool_names}}
        return query_result
    else:
        return make_query_result(using_pools, usage_map, share_map)

def query(clusters, user, pools):
    """
    Uses query_across_clusters to make the /usage
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(get_usage_on_cluster, cluster, user)

    query_result = query_across_clusters(clusters, submit)
    if pools:
        query_result.update(
            {'clusters':
                 {cluster: cluster_usage
                  for cluster, cluster_usage in query_result['clusters'].items()
                  if cluster_usage['using_pools'] and set(cluster_usage['pools']) & set(pools)}})
        for cluster, cluster_usage in query_result['clusters'].items():
            cluster_usage.update(
                {'pools':
                     {pool: pool_usage
                      for pool, pool_usage in cluster_usage['pools'].items()
                      if pool in pools}})
    return query_result


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


def format_share(share_map):
    """Given a "share map" with cpus, mem, and gpus, returns a formatted share string"""
    cpus = share_map['cpus']
    mem = share_map['mem']
    gpus = share_map['gpus']

    if cpus == sys.float_info.max:
        cpu_share = 'No CPU Limit'
    else:
        cpu_share = f'{cpus} CPU{"s" if cpus > 1 else ""}'

    if mem == sys.float_info.max:
        mem_share = 'No Memory Limit'
    else:
        mem_share = f'{format_job_memory(share_map)} Memory'

    if gpus == sys.float_info.max:
        gpu_share = 'No GPU Limit'
    else:
        gpu_share = f'{gpus} GPU{"s" if gpus > 1 else ""}'

    s = f'Share: {cpu_share}, {mem_share}, {gpu_share}'
    return s


def format_percent(n):
    """Formats n as a percentage"""
    return '{:.1%}'.format(n)


def print_formatted_cluster_or_pool_usage(cluster, cluster_usage):
    """Prints the query result for a cluster or pool in a cluster as a hierarchical set of bullets"""
    usage_map = cluster_usage['usage']
    share_map = cluster_usage['share']
    print_info(colors.bold(cluster))
    print_info(format_share(share_map))
    print_info(format_usage(usage_map))
    applications = cluster_usage['applications']
    if applications:
        print_info('Applications:')
    else:
        print_info(colors.waiting('Nothing Running'))
    for application, application_usage in applications.items():
        usage_map = application_usage['usage']
        print_info(f'- {colors.running(application if application else "[no application defined]")}')
        print_info(f'  {format_usage(usage_map)}')
        print_info('  Job Groups:')
        for group, group_usage in application_usage['groups'].items():
            usage_map = group_usage['usage']
            jobs = group_usage['jobs']
            print_info(f'\t- {colors.bold(group if group else "[ungrouped]")}')
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
                    print_formatted_cluster_or_pool_usage(f'{cluster} ({pool} pool)', pool_usage)
            else:
                print_formatted_cluster_or_pool_usage(cluster, cluster_usage)

def usage(clusters, args, _):
    """Prints cluster usage info for the given user"""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    user = args.get('user')
    pools = args.get('pools')

    query_result = query(clusters, user, pools)
    if as_json:
        print_as_json(query_result)
    else:
        if pools and not query_result['clusters'].items():
            print(f"No usage found for pools {pools}")
            return 0
        print_formatted(query_result)
    return 0


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('usage', help='show breakdown of usage by application and group')
    parser.add_argument('--user', '-u', help='show usage for a user')
    parser.add_argument('--pools', '-p', nargs='*', help='limit results to one or more pools')
    parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')

    add_defaults('usage', {'user': current_user()})

    return usage
