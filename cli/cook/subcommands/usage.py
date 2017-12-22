import json

from cook import http
from cook.querying import query_across_clusters, make_job_request
from cook.util import guard_no_cluster, current_user


def get_usage_on_cluster(cluster, user):
    """Queries cluster for jobs with the given state / user / time / name"""
    params = {'user': user, 'group_breakdown': 'true'}
    usage_map = http.make_data_request(cluster, lambda: http.get(cluster, 'usage', params=params))
    if not usage_map:
        raise Exception(f'Unable to retrieve usage information on {cluster}.')

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
            group = f'{group_uuid_to_name[group_uuids[0]]} ({group_uuids[0]})'if group_uuids else None
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

    query_result = {'usage': usage_map['total_usage'], 'applications': applications, 'count': len(jobs)}
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


def print_as_bullets():
    """Prints the query result as a hierarchical set of bullets"""
    raise Exception('NOT YET IMPLEMENTED')


def usage(clusters, args, _):
    """Prints cluster usage info for the given user"""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    user = args.get('user')

    query_result = query(clusters, user)
    if as_json:
        print_as_json(query_result)
    else:
        print_as_bullets()
    return 0


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('usage', help='show breakdown of usage by application and group')
    parser.add_argument('--user', '-u', help='show usage for a user')
    parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')

    add_defaults('usage', {'user': current_user()})

    return usage
