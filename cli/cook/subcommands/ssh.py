import logging
import os

from cook import colors
from cook.querying import query_unique_and_run, parse_entity_refs
from cook.util import print_info, guard_no_cluster


def ssh_to_instance(instance, sandbox_dir):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given instance."""
    print_info(f'Attempting ssh for job instance {colors.bold(instance["task_id"])}...')
    command = os.environ.get('CS_SSH', 'ssh')
    logging.info(f'using ssh command: {command}')
    hostname = instance['hostname']
    print_info(f'Executing ssh to {colors.bold(hostname)}.')
    os.execlp(command, 'ssh', '-t', hostname, f'cd "{sandbox_dir}" ; bash')


def ssh(clusters, args, _):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
    guard_no_cluster(clusters)
    entity_refs, clusters_of_interest = parse_entity_refs(clusters, args.get('uuid'))
    if len(entity_refs) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    query_unique_and_run(clusters_of_interest, entity_refs[0], ssh_to_instance)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh', help='ssh to Mesos agent by job or instance uuid')
    parser.add_argument('uuid', nargs=1)
    return ssh
