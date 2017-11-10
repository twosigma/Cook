import sys

from cook.querying import query, print_no_data
from cook.util import strip_all, print_info, seconds_to_timedelta, guard_no_cluster, read_lines


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
    uuids = strip_all(args.get('uuid'))
    stdin_from_pipe = not sys.stdin.isatty()

    if uuids and stdin_from_pipe:
        raise Exception('When piping to wait, you cannot also supply UUIDs as arguments.')

    if stdin_from_pipe:
        uuids = read_lines()

    if not uuids:
        raise Exception('You must specify at least one UUID on which to wait.')

    timeout_text = ('up to %s' % seconds_to_timedelta(timeout)) if timeout else 'indefinitely'
    print_info('Will wait %s.' % timeout_text)
    query_result = query(clusters, uuids, all_jobs_completed, all_instances_completed,
                         all_groups_completed, timeout, interval)
    if query_result['count'] > 0:
        return 0
    else:
        print_no_data(clusters)
        return 1


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    default_timeout = None
    default_timeout_text = 'wait indefinitely'
    default_interval = 5
    wait_parser = add_parser('wait', help='wait for jobs / instances / groups to complete by uuid')
    wait_parser.add_argument('uuid', nargs='*')
    wait_parser.add_argument('--timeout', '-t',
                             help=f'maximum time (in seconds) to wait (default = {default_timeout_text})', type=int)
    wait_parser.add_argument('--interval', '-i',
                             help=f'time (in seconds) to wait between polling (default = {default_interval})', type=int)

    add_defaults('wait', {'timeout': default_timeout, 'interval': default_interval})

    return wait
