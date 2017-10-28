from cook import http, colors
from cook.querying import query, print_no_data
from cook.util import strip_all, print_info


def guard_against_duplicates(uuids, query_result):
    """
    Checks for UUIDs that are duplicated and throws if any are found (unlikely to happen in the wild, but it could)
    """
    if query_result['count'] == 1:
        return

    uuid_to_entries = {}
    duplicate_uuids = []

    def add(uuid, entity_type, cluster):
        entry_map = {'type': entity_type, 'cluster_name': cluster}
        if uuid in uuid_to_entries:
            uuid_to_entries[uuid].append(entry_map)
            duplicate_uuids.append(uuid)
        else:
            uuid_to_entries[uuid] = [entry_map]

    for cluster_name, entities in query_result['clusters'].items():
        jobs = entities['jobs']
        for job in jobs:
            add(job['uuid'], 'job', cluster_name)

        instances = [i for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids]
        for instance in instances:
            add(instance['task_id'], 'job instance', cluster_name)

        groups = entities['groups']
        for group in groups:
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
        message = f'Unable to kill due to duplicate UUIDs.\n\n{details}'
        raise Exception(message)


def kill_entities(uuids, query_result, clusters):
    """Attempts to kill the jobs / instances / groups with the given UUIDs"""
    exit_code = 0
    for cluster_name, entities in query_result['clusters'].items():
        cluster = next(c for c in clusters if c['name'] == cluster_name)

        jobs = entities['jobs']
        instances = [i for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids]
        if len(jobs) > 0 or len(instances) > 0:
            job_uuids = [j['uuid'] for j in jobs]
            instance_uuids = [i['task_id'] for i in instances]
            resp = http.delete(cluster, 'rawscheduler', params={'job': job_uuids, 'instance': instance_uuids})
            if resp.status_code == 204:
                for job in jobs:
                    print_info(f'Killed job {colors.bold(job["uuid"])} on {colors.bold(cluster_name)}.')
                for instance in instances:
                    instance_uuid = instance['task_id']
                    print_info(f'Killed job instance {colors.bold(instance_uuid)} on {colors.bold(cluster_name)}.')
            else:
                exit_code = 1
                for job in jobs:
                    print(colors.failed(f'Failed to kill job {job["uuid"]} on {cluster_name}.'))
                for instance in instances:
                    print(colors.failed(f'Failed to kill job instance {instance["task_id"]} on {cluster_name}.'))

        groups = entities['groups']
        if len(groups) > 0:
            group_uuids = [g['uuid'] for g in groups]
            resp = http.delete(cluster, 'group', params={'uuid': group_uuids})
            if resp.status_code == 204:
                for group in groups:
                    print_info(f'Killed job group {colors.bold(group["uuid"])} on {colors.bold(cluster_name)}.')
            else:
                exit_code = 1
                for group in groups:
                    print(colors.failed(f'Failed to kill job group {group["uuid"]} on {cluster_name}.'))

    return exit_code


def kill(clusters, args):
    """Attempts to kill the jobs / instances / groups with the given UUIDs."""
    uuids = strip_all(args.get('uuid'))
    query_result = query(clusters, uuids)
    if query_result['count'] == 0:
        print_no_data(clusters)
        return 1

    # If the user provides UUIDs that map to more than one entity,
    # we will raise an Exception that contains the details
    guard_against_duplicates(uuids, query_result)

    return kill_entities(uuids, query_result, clusters)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='kill jobs / instances / groups by uuid')
    parser.add_argument('uuid', nargs='+')
    return kill
