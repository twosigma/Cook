import logging
from urllib.parse import urlparse

from cook import http


def instance_to_agent_url(instance):
    """
    Given a job instance, returns the base Mesos agent URL,
    e.g. http://agent123.example.com:5051
    """
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
    """
    Given an instance and its parent job, queries the corresponding
    Mesos agent and returns the sandbox directory for the instance
    """
    agent_url = instance_to_agent_url(instance)
    resp = http.__get(f'{agent_url}/state')
    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for its work directory.')

    resp_json = resp.json()
    frameworks = resp_json['completed_frameworks'] + resp_json['frameworks']
    cook_framework = next(f for f in frameworks if f['id'] == job['framework_id'])
    cook_executors = cook_framework['completed_executors'] + cook_framework['executors']
    directory = next(e['directory'] for e in cook_executors if e['id'] == instance['task_id'])
    return directory
