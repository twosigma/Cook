from collections import defaultdict

from cook import http, colors
from cook.querying import query, print_no_data, parse_entity_refs
from cook.util import print_info, guard_no_cluster


def guard_against_duplicates(query_result):
    """
    Checks for UUIDs that are duplicated and throws if any are found (unlikely to happen in the wild, but it could)
    """
    if query_result['count'] == 1:
        return

    uuid_to_entries = defaultdict(list)
    duplicate_uuids = set()

    def add(uuid, entity_type, cluster):
        if uuid in uuid_to_entries:
            duplicate_uuids.add(uuid)
        entry_map = {'type': entity_type, 'cluster_name': cluster}
        uuid_to_entries[uuid].append(entry_map)

    for cluster_name, entities in query_result['clusters'].items():
        if 'jobs' in entities:
            for job in entities['jobs']:
                add(job['uuid'], 'job', cluster_name)

        if 'instances' in entities:
            for instance, _ in entities['instances']:
                add(instance['task_id'], 'job instance', cluster_name)

        if 'groups' in entities:
            for group in entities['groups']:
                add(group['uuid'], 'job group', cluster_name)

    if len(duplicate_uuids) > 0:
        messages = []
        for duplicate_uuid in duplicate_uuids:
            bullets = []
            for entry in uuid_to_entries[duplicate_uuid]:
                bullets.append(f'- as a {entry["type"]} on {entry["cluster_name"]}')
            message = f'{duplicate_uuid} is duplicated:\n' + '\n'.join(bullets)
            messages.append(message)
        details = '\n\n'.join(messages)
        message = f'Refusing to kill due to duplicate UUIDs.\n\n{details}\n\nYou might need to explicitly set the ' \
                  f'cluster where you want to kill by using the --cluster flag.'
        raise Exception(message)


def kill_entities(query_result, clusters):
    """Attempts to kill the jobs / instances / groups with the given UUIDs"""
    num_failures = 0
    success_status_code = 204
    clusters_by_name = {c['name']: c for c in clusters}
    for cluster_name, entities in query_result['clusters'].items():
        cluster = clusters_by_name[cluster_name]

        job_uuids = [j['uuid'] for j in entities['jobs']] if 'jobs' in entities else []
        instance_uuids = [i['task_id'] for i, _ in entities['instances']] if 'instances' in entities else []
        num_jobs = len(job_uuids)
        num_instances = len(instance_uuids)
        if num_jobs > 0 or num_instances > 0:
            resp = http.delete(cluster, 'rawscheduler', params={'job': job_uuids, 'instance': instance_uuids})
            if resp.status_code == success_status_code:
                for job_uuid in job_uuids:
                    print_info(f'Killed job {colors.bold(job_uuid)} on {colors.bold(cluster_name)}.')
                for instance_uuid in instance_uuids:
                    print_info(f'Killed job instance {colors.bold(instance_uuid)} on {colors.bold(cluster_name)}.')
            else:
                num_failures += (num_jobs + num_instances)
                for job_uuid in job_uuids:
                    print(colors.failed(f'Failed to kill job {job_uuid} on {cluster_name}.'))
                for instance_uuid in instance_uuids:
                    print(colors.failed(f'Failed to kill job instance {instance_uuid} on {cluster_name}.'))

        if 'groups' in entities:
            group_uuids = [g['uuid'] for g in entities['groups']]
            num_groups = len(group_uuids)
            if num_groups > 0:
                resp = http.delete(cluster, 'group', params={'uuid': group_uuids})
                if resp.status_code == success_status_code:
                    for group_uuid in group_uuids:
                        print_info(f'Killed job group {colors.bold(group_uuid)} on {colors.bold(cluster_name)}.')
                else:
                    num_failures += num_groups
                    for group_uuid in group_uuids:
                        print(colors.failed(f'Failed to kill job group {group_uuid} on {cluster_name}.'))

    if num_failures > 0:
        print_info(f'There were {colors.failed(str(num_failures))} kill failures.')

    return num_failures


def kill(clusters, args, _):
    """Attempts to kill the jobs / instances / groups with the given UUIDs."""
    guard_no_cluster(clusters)
    uuids = parse_entity_refs(clusters, args.get('uuid'))
    query_result = query(clusters, uuids)
    if query_result['count'] == 0:
        print_no_data(clusters)
        return 1

    # If the user provides UUIDs that map to more than one entity,
    # we will raise an Exception that contains the details
    guard_against_duplicates(query_result)

    return kill_entities(query_result, clusters)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='kill jobs / instances / groups by uuid')
    parser.add_argument('uuid', nargs='*')
    return kill
