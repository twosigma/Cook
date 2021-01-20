import argparse
import logging
import os
from functools import partial

from cook import plugins, terminal
from cook.querying import get_compute_cluster_config, query_unique_and_run, parse_entity_refs
from cook.util import print_info, guard_no_cluster


def kubectl_exec_to_instance(_, instance_uuid, __, ___):
    os.execlp('kubectl', 'kubectl',
              'exec',
              '-c', os.getenv('COOK_CONTAINER_NAME_FOR_JOB', 'required-cook-job-container'),
              '-it', instance_uuid,
              '--', '/bin/sh', '-c', 'cd $HOME; exec /bin/sh')


def ssh_to_instance(job, instance, sandbox_dir_fn, cluster, command_to_run=None):
    """
    When using Mesos, attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given instance.
    When using Kubernetes, calls the exec command of the kubectl cli.
    """
    print_info(f'Attempting ssh for job instance {terminal.bold(instance["task_id"])}...')
    compute_cluster = instance["compute-cluster"]
    compute_cluster_type = compute_cluster["type"]
    compute_cluster_name = compute_cluster["name"]
    if compute_cluster_type == "kubernetes":
        kubectl_exec_to_instance_fn = plugins.get_fn('kubectl-exec-to-instance', kubectl_exec_to_instance)
        compute_cluster_config = get_compute_cluster_config(cluster, compute_cluster_name)
        kubectl_exec_to_instance_fn(job["user"], instance["task_id"], compute_cluster_config, command_to_run)
    else:
        command_to_run = command_to_run or ['bash']
        sandbox_dir = sandbox_dir_fn()
        command = os.environ.get('CS_SSH', 'ssh')
        logging.info(f'using ssh command: {command}')
        hostname = instance['hostname']
        print_info(f'Executing ssh to {terminal.bold(hostname)}.')
        args = ['ssh', '-t', hostname, 'cd', sandbox_dir, ';'] + command_to_run
        os.execlp(command, *args)


def ssh(clusters, args, _):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
    guard_no_cluster(clusters)
    entity_refs, clusters_of_interest = parse_entity_refs(clusters, args.get('uuid'))
    if len(entity_refs) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    command_from_command_line = args.get('command', None)
    command_fn = partial(ssh_to_instance, command_to_run=command_from_command_line)
    query_unique_and_run(clusters_of_interest, entity_refs[0], command_fn)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh', help='ssh to container by job or instance uuid')
    parser.add_argument('uuid', nargs=1)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    return ssh
