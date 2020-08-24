import os

from cook import configuration, http, terminal
from cook.subcommands.usage import filter_query_result_by_pools, print_formatted, query
from cook.util import date_time_string_to_ms_since_epoch, guard_no_cluster, load_target_clusters, print_error, str2bool

admin_parser = None
limits_parser = None
copy_limits_parser = None


def copy_limits(args, config_path):
    """Copies limits (share and quota) for a particular user from one cluster to another cluster"""
    user = args.get('user')

    from_cluster = args.get('from')
    from_url = args.get('from_url')
    if not from_cluster and not from_url:
        copy_limits_parser.print_help()
        print()
        raise Exception(f'You must provide either a from-cluster name (--from) or URL (--from-url).')

    to_cluster = args.get('to')
    to_url = args.get('to_url')
    if not to_cluster and not to_url:
        copy_limits_parser.print_help()
        print()
        raise Exception(f'You must provide either a to-cluster name (--to) or URL (--to-url).')

    _, config_map = configuration.load_config_with_defaults(config_path)
    from_clusters = load_target_clusters(config_map, from_url, from_cluster)
    to_clusters = load_target_clusters(config_map, to_url, to_cluster)
    assert len(from_clusters) == 1, 'Only a single from-cluster is supported.'
    assert len(to_clusters) == 1, 'Only a single to-cluster is supported.'
    from_cluster = from_clusters[0]
    to_cluster = to_clusters[0]
    from_cluster_name = from_cluster['name']
    to_cluster_name = to_cluster['name']
    print(f'Copying limits for {terminal.bold(user)} user '
          f'from {terminal.bold(from_cluster_name)} '
          f'to {terminal.bold(to_cluster_name)}:')
    from_pools = http.make_data_request(from_cluster, lambda: http.get(from_cluster, 'pools', params={}))
    to_pools = http.make_data_request(to_cluster, lambda: http.get(to_cluster, 'pools', params={}))
    from_pools_dict = {pool['name']: pool for pool in from_pools}
    to_pools_dict = {pool['name']: pool for pool in to_pools}
    for pool_name, from_pool in from_pools_dict.items():
        if pool_name in to_pools_dict and to_pools_dict[pool_name]['state'] != 'inactive':
            print(f'\n=== Pool: {pool_name} ===')
            query_result = query([from_cluster, to_cluster], user)
            query_result = filter_query_result_by_pools(query_result, [pool_name])
            print_formatted(query_result)
            answer = input(f'Copy limits for {terminal.bold(pool_name)} pool '
                           f'from {terminal.bold(from_cluster_name)} '
                           f'to {terminal.bold(to_cluster_name)}? ')
            should_copy = str2bool(answer)
            if should_copy:
                from_dict = query_result['clusters'][from_cluster_name]['pools'][pool_name]
                reason = f'Copying limits for {user} user from {from_cluster_name} to {to_cluster_name}'

                from_share = from_dict['share']
                resp = http.post(to_cluster,
                                 'share',
                                 {'pool': pool_name,
                                  'user': user,
                                  'reason': reason,
                                  'share': from_share})
                if resp.status_code != 201:
                    print_error(f'Setting share for {pool_name} on {to_cluster_name} '
                                f'failed with status code {resp.status_code}: {resp.text}')
                else:
                    print(terminal.success(f'Copied share for {pool_name} pool '
                                           f'from {from_cluster_name} '
                                           f'to {to_cluster_name}.'))

                from_quota = from_dict['quota']
                resp = http.post(to_cluster,
                                 'quota',
                                 {'pool': pool_name,
                                  'user': user,
                                  'reason': reason,
                                  'quota': from_quota})
                if resp.status_code != 201:
                    print_error(f'Setting quota for {pool_name} on {to_cluster_name} '
                                f'failed with status code {resp.status_code}: {resp.text}')
                else:
                    print(terminal.success(f'Copied quota for {pool_name} pool '
                                           f'from {from_cluster_name} '
                                           f'to {to_cluster_name}.'))


def limits(args, config_path):
    """Entrypoint for the admin limits sub-command"""
    limits_action = args.get('limits-action')
    if not limits_action:
        limits_parser.print_help()

    if limits_action == 'copy':
        copy_limits(args, config_path)


def query_instances_on_cluster(cluster, status, start_ms, end_ms):
    """Queries cluster for instance stats with the given status / time"""
    params = {'status': status, 'start': start_ms, 'end': end_ms}
    stats = http.make_data_request(cluster, lambda: http.get(cluster, 'stats/instances', params=params))
    overall_stats = stats['overall']
    data = {'count': overall_stats['count'] if 'count' in overall_stats else 0}
    return data


def instances(clusters, args):
    """Prints the count of instances with the given criteria"""
    guard_no_cluster(clusters)

    if len(clusters) != 1:
        raise Exception(f'You must specify a single cluster to query.')

    status = args.get('status')
    started_after = args.get('started_after')
    started_before = args.get('started_before')

    status = status or 'success'
    start_ms = date_time_string_to_ms_since_epoch(started_after or '10 minutes ago')
    end_ms = date_time_string_to_ms_since_epoch(started_before or 'now')

    cluster = clusters[0]
    data = query_instances_on_cluster(cluster, status, start_ms, end_ms)
    print(data['count'])
    return 0


def admin(clusters, args, config_path):
    """Entrypoint for the admin sub-command"""
    admin_action = args.get('admin-action')
    if not admin_action:
        admin_parser.print_help()

    if admin_action == 'limits':
        limits(args, config_path)
    elif admin_action == 'instances':
        instances(clusters, args)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    is_admin_enabled = str2bool(os.environ.get('CS_ADMIN', 'false'))
    if is_admin_enabled:
        global admin_parser
        global limits_parser
        global copy_limits_parser

        # cs admin
        admin_parser = add_parser('admin', help='administrative tasks')
        admin_subparsers = admin_parser.add_subparsers(dest='admin-action')

        # cs admin limits
        limits_parser = admin_subparsers.add_parser('limits', help='tasks related to user limits')
        limits_subparsers = limits_parser.add_subparsers(dest='limits-action')

        # cs admin limits copy
        copy_limits_parser = limits_subparsers.add_parser('copy', help='copy limits from another cluster')
        copy_limits_parser.add_argument('--user', '-u', help='the user to copy limits for', default='default')
        copy_limits_parser.add_argument('--from', '-f', help='the name of the Cook scheduler cluster to copy from')
        copy_limits_parser.add_argument('--from-url', help='the URL of the Cook scheduler cluster to copy from')
        copy_limits_parser.add_argument('--to', '-t', help='the name of the Cook scheduler cluster to copy to')
        copy_limits_parser.add_argument('--to-url', help='the URL of the Cook scheduler cluster to copy to')

        # cs admin instances
        instances_parser = admin_subparsers.add_parser('instances', help='queries instance information from a cluster')
        instances_parser.add_argument('--started-after', '-A', help=f'include instances started after the given time')
        instances_parser.add_argument('--started-before', '-B', help=f'include instances started before the given time')
        group = instances_parser.add_mutually_exclusive_group()
        group.add_argument('--running', '-r', help='running instances', dest='status',
                           action='store_const', const='running')
        group.add_argument('--failed', '-f', help='failed instances', dest='status',
                           action='store_const', const='failed')
        group.add_argument('--success', '-s', help='successful instances', dest='status',
                           action='store_const', const='success')

        return admin
