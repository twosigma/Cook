import logging
from urllib.parse import urlparse, parse_qs

from cook import http


def instance_to_agent_url(instance):
    """Given a job instance, returns the base Mesos agent URL, e.g. http://agent123.example.com:5051"""
    hostname = instance['hostname']
    if 'output_url' in instance:
        output_url = instance['output_url']
        url = urlparse(output_url)
        netloc = url.netloc
    else:
        logging.info('assuming default agent port of 5051')
        netloc = f'{hostname}:5051'
    return f'http://{netloc}'


def retrieve_instance_sandbox_directory(instance, job):
    """Given an instance and its parent job, determines the Mesos agent sandbox directory"""

    # Check if we've simply been handed the sandbox directory
    if 'sandbox_directory' in instance:
        logging.debug('found sandbox directory directly on instance')
        return instance['sandbox_directory']

    # Try parsing it from the output url if present
    if 'output_url' in instance:
        output_url = instance['output_url']
        url = urlparse(output_url)
        query_dict = parse_qs(url.query)
        if 'path' in query_dict:
            path_list = query_dict['path']
            if len(path_list) == 1:
                logging.debug('parsed sandbox directory from output url')
                return path_list[0]

    # As a last resort, query the Mesos agent state
    agent_url = instance_to_agent_url(instance)
    resp = http.__get(f'{agent_url}/state')
    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for the sandbox directory.')

    # Parse the Mesos agent state and look for a matching executor
    resp_json = resp.json()
    frameworks = resp_json['completed_frameworks'] + resp_json['frameworks']
    cook_framework = next(f for f in frameworks if f['id'] == job['framework_id'])
    cook_executors = cook_framework['completed_executors'] + cook_framework['executors']
    instance_id = instance['task_id']
    directories = [e['directory'] for e in cook_executors if e['id'] == instance_id]

    if len(directories) == 0:
        raise Exception(f'Unable to retrieve sandbox directory for job instance {instance_id}.')

    if len(directories) > 1:
        # This should not happen, but we'll be defensive anyway
        raise Exception(f'Found more than Mesos executor with ID {instance_id}')

    return directories[0]
