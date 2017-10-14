import logging
import os
from urllib.parse import urlparse

from cook import colors, http
from cook.subcommands.show import query, print_no_data
from cook.util import print_info, strip_all


def sandbox_directory(agent_work_dir, instance, job):
    """
    Constructs the agent sandbox directory given the agent working directory, and instance, and its parent job.
    """
    directory = os.path.join(agent_work_dir, 'slaves', f'{instance["slave_id"]}', 'frameworks',
                             f'{job["framework_id"]}', 'executors', f'{instance["executor_id"]}', 'runs', 'latest')
    logging.debug(f'sandbox directory = {directory}')
    return directory


def ssh_to_instance(instance, job):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given instance."""
    output_url = instance['output_url']
    if not output_url:
        logging.error(f'output_url was not provided on instance')
        raise Exception('Unable to determine sandbox directory for this job instance.')

    url = urlparse(output_url)
    resp = http.__get(f'{url.scheme}://{url.netloc}/state')
    logging.debug(f'response from mesos agent: {resp.text}')
    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code}')
        raise Exception('Unable to determine sandbox directory for this job instance.')

    resp_json = resp.json()
    agent_work_dir = resp_json['flags']['work_dir']
    if not agent_work_dir:
        logging.error(f'work_dir was not present in mesos agent state')
        raise Exception('Unable to determine sandbox directory for this job instance.')

    hostname = instance['hostname']
    directory = sandbox_directory(agent_work_dir, instance, job)
    print_info(f'Attempting to ssh to {colors.bold(hostname)} and cd to sandbox...')
    os.execlp('ssh', 'ssh', '-t', hostname, f'cd {directory} ; bash')


def ssh(clusters, args):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    if len(uuids) > 1:
        # The argparse library should prevent this, but we're defensive here anyway
        raise Exception(f'You can only provide a single uuid.')

    query_result = query(clusters, uuids)
    num_results = query_result['count']

    if num_results == 0:
        print_no_data(clusters)
        return 1

    if num_results > 1:
        # This is unlikely to happen in the wild, but it could
        print_info('There is more than one match for the given uuid.')
        return 1

    for cluster_name, entities in query_result['clusters'].items():
        if len(entities['groups']) > 0:
            print_info('You must provide a job uuid or job instance uuid. You provided a job group uuid.')
            return 1

        jobs = entities['jobs']
        instances = [[i, j] for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids]
        if len(jobs) > 0:
            print('NOT YET IMPLEMENTED')
        elif len(instances) > 0:
            instance, job = instances[0]
            ssh_to_instance(instance, job)
    else:
        raise Exception(f'Encountered unexpected error in ssh command for uuid {uuids[0]}')


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('ssh', help='ssh to the corresponding Mesos agent by jobs or instance uuid')
    show_parser.add_argument('uuid', nargs=1)
    return ssh
