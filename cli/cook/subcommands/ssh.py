import logging
import os

from cook import terminal
from cook.querying import get_compute_cluster_config, query_unique_and_run, parse_entity_refs
from cook.util import print_info, guard_no_cluster


class Ssh:
    def kubectl_exec_to_instance(self, instance_uuid, _):
        os.execlp('kubectl', 'kubectl',
                  'exec',
                  '-c', os.getenv('COOK_CONTAINER_NAME_FOR_JOB', 'required-cook-job-container'),
                  '-it', instance_uuid,
                  '--', '/bin/sh', '-c', 'cd $HOME; exec /bin/sh')


    def ssh_to_instance(self, instance, sandbox_dir_fn, cluster):
        """
        When using Mesos, attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given instance.
        When using Kubernetes, calls the exec command of the kubectl cli.
        """
        print_info(f'Attempting ssh for job instance {terminal.bold(instance["task_id"])}...')
        compute_cluster = instance["compute-cluster"]
        compute_cluster_type = compute_cluster["type"]
        compute_cluster_name = compute_cluster["name"]
        if compute_cluster_type == "kubernetes":
            compute_cluster_config = get_compute_cluster_config(cluster, compute_cluster_name)
            self.kubectl_exec_to_instance(instance["task_id"], compute_cluster_config)
        else:
            sandbox_dir = sandbox_dir_fn()
            command = os.environ.get('CS_SSH', 'ssh')
            logging.info(f'using ssh command: {command}')
            hostname = instance['hostname']
            print_info(f'Executing ssh to {terminal.bold(hostname)}.')
            os.execlp(command, 'ssh', '-t', hostname, f'cd "{sandbox_dir}" ; bash')


    def ssh(self, clusters, args, _):
        """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
        guard_no_cluster(clusters)
        entity_refs, clusters_of_interest = parse_entity_refs(clusters, args.get('uuid'))
        if len(entity_refs) > 1:
            # argparse should prevent this, but we'll be defensive anyway
            raise Exception(f'You can only provide a single uuid.')

        query_unique_and_run(clusters_of_interest, entity_refs[0], self.ssh_to_instance)


def register(add_parser, _, dependency_overrides):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh', help='ssh to container by job or instance uuid')
    parser.add_argument('uuid', nargs=1)
    factory = dependency_overrides.get('ssh', Ssh)
    return factory().ssh
