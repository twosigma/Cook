from collections import defaultdict

from cook import http, terminal
from cook.querying import print_no_data, parse_entity_refs, query_with_stdin_support
from cook.util import print_info, guard_no_cluster, partition


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


def __kill_entities(cluster, uuids, endpoint, param):
    """Attempts to kill the jobs / instances / groups with the given UUIDs on the given cluster"""
    resp = http.delete(cluster, endpoint, params={param: uuids})
    return resp.status_code == 204


def kill_jobs(cluster, uuids):
    """Attempts to kill the jobs with the given UUIDs"""
    return __kill_entities(cluster, uuids, 'rawscheduler', 'job')


def kill_instances(cluster, uuids):
    """Attempts to kill the job instsances with the given UUIDs"""
    return __kill_entities(cluster, uuids, 'rawscheduler', 'instance')


def kill_groups(cluster, uuids):
    """Attempts to kill the job groups with the given UUIDs"""
    return __kill_entities(cluster, uuids, 'group', 'uuid')


def kill_entities(query_result, clusters):
    """Attempts to kill the jobs / instances / groups with the given UUIDs"""
    kill_batch_size = 100
    failed = []
    succeeded = []
    clusters_by_name = {c['name']: c for c in clusters}

    def __kill(cluster, uuids, kill_fn, entity_type):
        if len(uuids) > 0:
            for uuid_batch in partition(uuids, kill_batch_size):
                success = kill_fn(cluster, uuid_batch)
                batch = [{'cluster': cluster, 'type': entity_type, 'uuid': u} for u in uuid_batch]
                (succeeded if success else failed).extend(batch)

    for cluster_name, entities in query_result['clusters'].items():
        cluster = clusters_by_name[cluster_name]
        job_uuids = [j['uuid'] for j in entities['jobs']] if 'jobs' in entities else []
        instance_uuids = [i['task_id'] for i, _ in entities['instances']] if 'instances' in entities else []
        group_uuids = [g['uuid'] for g in entities['groups']] if 'groups' in entities else []
        __kill(cluster, job_uuids, kill_jobs, 'job')
        __kill(cluster, instance_uuids, kill_instances, 'job instance')
        __kill(cluster, group_uuids, kill_groups, 'job group')

    for item in succeeded:
        print_info(f'Killed {item["type"]} {terminal.bold(item["uuid"])} on {terminal.bold(item["cluster"]["name"])}.')
    for item in failed:
        print(terminal.failed(f'Failed to kill {item["type"]} {item["uuid"]} on {item["cluster"]["name"]}.'))
    num_succeeded = len(succeeded)
    num_failed = len(failed)
    print_info(f'Successful: {num_succeeded}, Failed: {num_failed}')
    return num_failed


def kill(clusters, args, _):
    """Attempts to kill the jobs / instances / groups with the given UUIDs."""
    guard_no_cluster(clusters)
    entity_refs, _ = parse_entity_refs(clusters, args.get('uuid'))
    query_result, clusters_of_interest = query_with_stdin_support(clusters, entity_refs)
    if query_result['count'] == 0:
        print_no_data(clusters_of_interest)
        return 1

    # If the user provides UUIDs that map to more than one entity,
    # we will raise an Exception that contains the details
    guard_against_duplicates(query_result)

    return kill_entities(query_result, clusters_of_interest)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='kill jobs / instances / groups by uuid')
    parser.add_argument('uuid', nargs='*')
    return kill
