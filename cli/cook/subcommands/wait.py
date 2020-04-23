from cook.querying import print_no_data, parse_entity_refs, query_with_stdin_support
from cook.util import print_info, seconds_to_timedelta, guard_no_cluster


def all_jobs_completed(jobs):
    """Returns jobs if they are all completed, otherwise False."""
    if all(j.get('status') == 'completed' for j in jobs):
        return jobs
    else:
        return False


def all_instances_completed(instances):
    """Returns instances if they are all completed, otherwise False."""
    if all(i.get('status') == 'completed' for i in instances):
        return instances
    else:
        return False


def all_groups_completed(groups):
    """Returns groups if they are all completed, otherwise False."""
    if all(len(g.get('jobs')) == g.get('completed') for g in groups):
        return groups
    else:
        return False


def wait(clusters, args, _):
    """Waits for jobs / instances / groups with the given UUIDs to complete."""
    guard_no_cluster(clusters)
    timeout = args.get('timeout')
    interval = args.get('interval')
    entity_refs, _ = parse_entity_refs(clusters, args.get('uuid'))
    timeout_text = ('up to %s' % seconds_to_timedelta(timeout)) if timeout else 'indefinitely'
    print_info('Will wait %s.' % timeout_text)
    query_result, clusters_of_interest = query_with_stdin_support(clusters, entity_refs, all_jobs_completed,
                                                                  all_instances_completed, all_groups_completed,
                                                                  timeout, interval)
    if query_result['count'] > 0:
        return 0
    else:
        print_no_data(clusters_of_interest)
        return 1


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    default_timeout = None
    default_timeout_text = 'wait indefinitely'
    default_interval = 15
    wait_parser = add_parser('wait', help='wait for jobs / instances / groups to complete by uuid')
    wait_parser.add_argument('uuid', nargs='*')
    wait_parser.add_argument('--timeout', '-t',
                             help=f'maximum time (in seconds) to wait (default = {default_timeout_text})', type=int)
    wait_parser.add_argument('--interval', '-i',
                             help=f'time (in seconds) to wait between polling (default = {default_interval})', type=int)

    add_defaults('wait', {'timeout': default_timeout, 'interval': default_interval})

    return wait
