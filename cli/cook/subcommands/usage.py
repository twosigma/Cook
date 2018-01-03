import json

import sys

from cook import http, colors
from cook.format import format_job_memory
from cook.querying import query_across_clusters, make_job_request
from cook.util import guard_no_cluster, current_user, print_info


def get_usage_on_cluster(cluster, user):
    """Queries cluster for jobs with the given state / user / time / name"""
    params = {'user': user, 'group_breakdown': 'true'}
    usage_map = http.make_data_request(cluster, lambda: http.get(cluster, 'usage', params=params))
    if not usage_map:
        raise Exception(f'Unable to retrieve usage information on {cluster["name"]} ({cluster["url"]}).')

    share_map = http.make_data_request(cluster, lambda: http.get(cluster, 'share', params={'user': user}))
    if not share_map:
        raise Exception(f'Unable to retrieve share information on {cluster["name"]} ({cluster["url"]}).')

    ungrouped_running_job_uuids = usage_map['ungrouped']['running_jobs']
    job_uuids_to_retrieve = ungrouped_running_job_uuids[:]
    grouped = usage_map['grouped']

    group_uuid_to_name = {}
    for group_usage in grouped:
        group = group_usage['group']
        job_uuids_to_retrieve.extend(group['running_jobs'])
        group_uuid_to_name[group['uuid']] = group['name']

    jobs = http.make_data_request(cluster, lambda: make_job_request(cluster, job_uuids_to_retrieve))
    applications = {}
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
        job_map = {'uuid': job['uuid'],
                   'name': job['name'],
                   'cpus': job['cpus'],
                   'mem': job['mem'],
                   'gpus': job['gpus']}
        applications[application]['groups'][group]['jobs'].append(job_map)

    query_result = {'usage': usage_map['total_usage'],
                    'applications': applications,
                    'count': len(jobs),
                    'share': share_map}
    return query_result


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


def format_usage(usage_map):
    """Given a "usage map" with cpus, mem, and gpus, returns a formatted usage string"""
    cpus = usage_map['cpus']
    s = f'Usage: {cpus} CPU{"s" if cpus > 1 else ""}, {format_job_memory(usage_map)} Mem'
    gpus = usage_map['gpus']
    if gpus > 0:
        s += f', {gpus} GPUs'
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
        mem_share = f'{format_job_memory(share_map)} Mem'

    if gpus == sys.float_info.max:
        gpu_share = 'No GPU Limit'
    else:
        gpu_share = f'{gpus} GPU{"s" if gpus > 1 else ""}'

    s = f'Share: {cpu_share}, {mem_share}, {gpu_share}'
    return s


def print_formatted(query_result):
    """Prints the query result as a hierarchical set of bullets"""
    for cluster, cluster_usage in query_result['clusters'].items():
        usage_map = cluster_usage['usage']
        share_map = cluster_usage['share']
        print_info(colors.bold(cluster))
        print_info(format_share(share_map))
        print_info(format_usage(usage_map))
        applications = cluster_usage['applications']
        if applications:
            print_info('Applications:')
        else:
            print_info('Nothing Running')
        for application, application_usage in applications.items():
            usage_map = application_usage['usage']
            print_info(f'- {colors.running(application if application else "[no application defined]")}')
            print_info(f'  {format_usage(usage_map)}')
            print_info('  Job Groups:')
            for group, group_usage in application_usage['groups'].items():
                usage_map = group_usage['usage']
                print_info(f'\t- {colors.bold(group if group else "[ungrouped]")}')
                print_info(f'\t  {format_usage(usage_map)}')
                print_info('\t  Jobs:')
                for job in group_usage['jobs']:
                    print_info(f'\t\t- {job["name"]} ({job["uuid"]}) {format_usage(job)}')
                print_info('')
        print_info('')


def usage(clusters, args, _):
    """Prints cluster usage info for the given user"""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    user = args.get('user')

    query_result = query(clusters, user)
    if as_json:
        print_as_json(query_result)
    else:
        print_formatted(query_result)
    return 0


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('usage', help='show breakdown of usage by application and group')
    parser.add_argument('--user', '-u', help='show usage for a user')
    parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')

    add_defaults('usage', {'user': current_user()})

    return usage
