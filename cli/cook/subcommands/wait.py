import time

from cook.subcommands.show import query, print_no_data, seconds_to_timedelta

from cook.util import strip_all, print_info


def all_jobs_completed(jobs):
    """Returns jobs if they are all completed, otherwise False."""
    if not [j for j in jobs if j.get('status') != 'completed']:
        return jobs
    else:
        return False


def all_instances_completed(instances):
    """Returns instances if they are all completed, otherwise False."""
    if not [i for i in instances if i.get('status') != 'completed']:
        return instances
    else:
        return False


def all_groups_completed(groups):
    """Returns groups if they are all completed, otherwise False."""
    if not [g for g in groups if len(g.get('jobs')) != g.get('completed')]:
        return groups
    else:
        return False


def wait(clusters, args):
    """Waits for jobs / instances / groups with the given UUIDs to complete."""
    timeout = args.get('timeout')
    interval = args.get('interval')
    uuids = strip_all(args.get('uuid'))
    timeout_text = ('up to %s' % seconds_to_timedelta(timeout)) if timeout else 'indefinitely'
    print_info('Waiting %s for job(s) to complete...' % timeout_text)
    start = time.time()
    query_result = query(clusters, uuids, all_jobs_completed, all_instances_completed,
                         all_groups_completed, timeout, interval)
    end = time.time()
    if query_result['count'] > 0:
        elapsed = end - start
        if elapsed >= interval:
            print_info('Job(s) completed after waiting for a total of %s.' % seconds_to_timedelta(elapsed))
        else:
            print_info('Job(s) are already complete.')
        return 0
    else:
        print_no_data(clusters)
        return 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    wait_parser = add_parser('wait', help='wait for job(s) to complete by uuid')
    wait_parser.add_argument('uuid', nargs='+')
    wait_parser.add_argument('--timeout', '-t', default=None, help='maximum time (in seconds) to wait', type=int)
    wait_parser.add_argument('--interval', '-i', default=5, help='time (in seconds) to wait between polling', type=int)
    return wait
