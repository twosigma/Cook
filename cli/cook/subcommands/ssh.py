import logging
import os
from operator import itemgetter
from urllib.parse import urlparse

from cook import colors, http
from cook.querying import query, no_data_message
from cook.util import print_info, strip_all


def sandbox_directory(agent_work_dir, instance, job):
    """
    Constructs the agent sandbox directory given the agent working directory, an instance, and its parent job.
    """
    directory = os.path.join(agent_work_dir, 'slaves', instance["slave_id"], 'frameworks',
                             job["framework_id"], 'executors', instance["executor_id"], 'runs', 'latest')
    logging.debug(f'sandbox directory = {directory}')
    return directory


def ssh_to_instance(instance, job):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given instance."""
    hostname = instance['hostname']
    if 'output_url' in instance:
        output_url = instance['output_url']
        url = urlparse(output_url)
        netloc = url.netloc
    else:
        logging.info('assuming default agent port of 5051')
        netloc = f'{hostname}:5051'

    resp = http.__get(f'http://{netloc}/state')
    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for its work directory.')

    resp_json = resp.json()
    flags = resp_json['flags']
    if 'work_dir' not in flags:
        logging.error(f'work_dir was not present in mesos agent state flags: {flags}')
        raise Exception('The Mesos agent state did not include its work directory.')

    print_info(f'Executing ssh to {colors.bold(hostname)}.')
    agent_work_dir = flags['work_dir']
    directory = sandbox_directory(agent_work_dir, instance, job)
    command = os.environ.get('CS_SSH', 'ssh')
    logging.info(f'using ssh command: {command}')
    os.execlp(command, 'ssh', '-t', hostname, f'cd "{directory}" ; bash')


def ssh_to_job(job):
    """Tries ssh_to_instance for the most recently started instance of job."""
    instances = job['instances']
    if len(instances) == 0:
        raise Exception('This job currently has no instances.')

    instance = max(instances, key=itemgetter('start_time'))
    print_info(f'Attempting ssh for job instance {colors.bold(instance["task_id"])}...')
    ssh_to_instance(instance, job)


def query_unique(clusters, uuid):
    """Resolves a uuid to a unique job or (instance, job) pair."""
    query_result = query(clusters, [uuid])
    num_results = query_result['count']

    if num_results == 0:
        raise Exception(no_data_message(clusters))

    if num_results > 1:
        # This is unlikely to happen in the wild, but it could.
        # A couple of examples of how this could happen:
        # - same uuid on an instance and a job (not necessarily the parent job)
        # - same uuid on a job in cluster x as another job in cluster y
        raise Exception('There is more than one match for the given uuid.')

    cluster_name, entities = next(iter(query_result['clusters'].items()))

    # Check for a group, which will raise an Exception
    if len(entities['groups']) > 0:
        raise Exception('You must provide a job uuid or job instance uuid. You provided a job group uuid.')

    # Check for a job
    jobs = entities['jobs']
    if len(jobs) > 0:
        job = jobs[0]
        return {'type': 'job', 'data': job}

    # Check for a job instance
    instances = [[i, j] for j in entities['instances'] for i in j['instances'] if i['task_id'] == uuid]
    if len(instances) > 0:
        instance, job = instances[0]
        return {'type': 'instance', 'data': (instance, job)}

    # This should not happen (the only entities we generate are jobs, instances, and groups)
    raise Exception(f'Encountered unexpected error when querying for uuid {uuid}.')


def ssh(clusters, args):
    """Attempts to ssh (using os.execlp) to the Mesos agent corresponding to the given job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    if len(uuids) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    query_result = query_unique(clusters, uuids[0])
    if query_result['type'] == 'job':
        job = query_result['data']
        ssh_to_job(job)
    elif query_result['type'] == 'instance':
        instance, job = query_result['data']
        ssh_to_instance(instance, job)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('ssh', help='ssh to the corresponding Mesos agent by job or instance uuid')
    show_parser.add_argument('uuid', nargs=1)
    return ssh
