import logging
import os
from operator import itemgetter
from urllib.parse import urlparse

from cook import colors, http
from cook.subcommands.show import query, print_no_data
from cook.util import print_info, strip_all


def sandbox_directory(agent_work_dir, instance, job):
    """
    Constructs the agent sandbox directory given the agent working directory, an instance, and its parent job.
    """
    directory = os.path.join(agent_work_dir, 'slaves', f'{instance["slave_id"]}', 'frameworks',
                             f'{job["framework_id"]}', 'executors', f'{instance["executor_id"]}', 'runs', 'latest')
    logging.debug(f'sandbox directory = {directory}')
    return directory


def ssh_to_instance(instance, job):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given instance."""
    if 'output_url' not in instance:
        logging.error(f'output_url was not provided on instance')
        raise Exception('Unable to determine the Mesos agent sandbox for this job instance.')

    output_url = instance['output_url']
    url = urlparse(output_url)
    resp = http.__get(f'{url.scheme}://{url.netloc}/state')
    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for its work directory.')

    resp_json = resp.json()
    flags = resp_json['flags']
    if 'work_dir' not in flags:
        logging.error(f'work_dir was not present in mesos agent state flags: {flags}')
        raise Exception('Mesos agent state did not include its work directory.')

    hostname = instance['hostname']
    print_info(f'Executing ssh to {colors.bold(hostname)}.')
    agent_work_dir = flags['work_dir']
    directory = sandbox_directory(agent_work_dir, instance, job)
    file = os.environ.get('SSH', 'ssh')
    logging.info(f'using ssh file: {file}')
    os.execlp(file, 'ssh', '-t', hostname, f'cd {directory} ; bash')


def ssh_to_job(job):
    """
    Tries ssh_to_instance for each instance from most to least recent, until either
    ssh can be executed for an instance, or all instances have been tried and failed.
    """
    instances = job['instances']
    if len(instances) == 0:
        raise Exception('This job currently has no instances.')

    for instance in sorted(instances, key=itemgetter('start_time'), reverse=True):
        try:
            print_info(f'Attempting ssh for job instance {colors.bold(instance["task_id"])}...')
            ssh_to_instance(instance, job)
        except Exception as e:
            print_info(str(e))
    else:
        raise Exception(f'Unable to ssh for any instance of this job.')


def ssh(clusters, args):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    if len(uuids) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    query_result = query(clusters, uuids)
    num_results = query_result['count']

    if num_results == 0:
        print_no_data(clusters)
        return 1

    if num_results > 1:
        # This is unlikely to happen in the wild, but it could.
        # A couple of examples of how this could happen:
        # - same uuid on an instance and a job (not necessarily the parent job)
        # - same uuid on a job in cluster x as another job in cluster y
        print_info('There is more than one match for the given uuid.')
        return 1

    for cluster_name, entities in query_result['clusters'].items():
        if len(entities['groups']) > 0:
            raise Exception('You must provide a job uuid or job instance uuid. You provided a job group uuid.')

        jobs = entities['jobs']
        instances = [[i, j] for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids]
        if len(jobs) > 0:
            job = jobs[0]
            ssh_to_job(job)
        elif len(instances) > 0:
            instance, job = instances[0]
            ssh_to_instance(instance, job)
    else:
        raise Exception(f'Encountered unexpected error in ssh command for uuid {uuids[0]}.')


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('ssh', help='ssh to the corresponding Mesos agent by job or instance uuid')
    show_parser.add_argument('uuid', nargs=1)
    return ssh
