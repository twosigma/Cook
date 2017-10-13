import logging
import os

from cook.subcommands.show import query, print_no_data
from cook.util import print_info, strip_all


def ssh(clusters, args):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    if len(uuids) > 1:
        # The argparse library should prevent this, but we're defensive here anyway
        raise Exception(f'You can only provide a single uuid.')
    query_result = query(clusters, uuids)
    num_results = query_result['count']
    if num_results > 1:
        # This is unlikely to happen in the wild, but it could
        print_info('There is more than one match for the given uuid.')
        return 0
    elif num_results == 1:
        for cluster_name, entities in query_result['clusters'].items():
            jobs = entities['jobs']
            instances = [[i, j] for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids]
            if len(jobs) > 0:
                print('NOT YET IMPLEMENTED')
            elif len(instances) > 0:
                instance, job = instances[0]
                hostname = instance['hostname']
                logging.info(f'attempting ssh to {hostname}')
                os.execlp('ssh', 'ssh', hostname)
            elif len(entities['groups']) > 0:
                print_info('You must provide a job or instance uuid. You provided a job group uuid.')
            else:
                raise Exception(f'Encountered unexpected error in ssh command for uuid {uuids[0]}')
    else:
        print_no_data(clusters)
        return 1


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('ssh', help='ssh to the corresponding Mesos agent by jobs or instance uuid')
    show_parser.add_argument('uuid', nargs=1)
    return ssh
