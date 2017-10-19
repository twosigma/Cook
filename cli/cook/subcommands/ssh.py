import logging
import os

from cook import colors, mesos
from cook.util import print_info, strip_all
from cook.querying import query_unique_and_run


def ssh_to_instance(instance, job):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given instance."""
    print_info(f'Attempting ssh for job instance {colors.bold(instance["task_id"])}...')
    directory = mesos.retrieve_instance_sandbox_directory(instance, job)
    command = os.environ.get('CS_SSH', 'ssh')
    logging.info(f'using ssh command: {command}')
    hostname = instance['hostname']
    print_info(f'Executing ssh to {colors.bold(hostname)}.')
    os.execlp(command, 'ssh', '-t', hostname, f'cd "{directory}" ; bash')


def ssh(clusters, args):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    if len(uuids) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    query_unique_and_run(clusters, uuids[0], ssh_to_instance)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh', help='ssh to the corresponding Mesos agent by job or instance uuid')
    parser.add_argument('uuid', nargs=1)
    return ssh
