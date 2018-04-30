import logging
import os
import sys
import urllib.parse as up


def instance_to_agent_url(instance):
    """Given a job instance, returns the base Mesos agent URL, e.g. http://agent123.example.com:5051"""
    if 'output_url' in instance:
        output_url = instance['output_url']
        url = up.urlparse(output_url)
        netloc = url.netloc
    else:
        logging.info('assuming default agent port of 5051')
        hostname = instance['hostname']
        netloc = f'{hostname}:5051'
    return f'http://{netloc}'


def sandbox_directory(session, instance, job):
    """Given an instance and its parent job, determines the Mesos agent sandbox directory"""

    # Check if we've simply been handed the sandbox directory
    if 'sandbox_directory' in instance:
        logging.debug('found sandbox directory directly on instance')
        return instance['sandbox_directory']

    # Try parsing it from the output url if present
    if 'output_url' in instance:
        output_url = instance['output_url']
        url = up.urlparse(output_url)
        query_dict = up.parse_qs(url.query)
        if 'path' in query_dict:
            path_list = query_dict['path']
            if len(path_list) == 1:
                logging.debug('parsed sandbox directory from output url')
                return path_list[0]

    # As a last resort, query the Mesos agent state
    agent_url = instance_to_agent_url(instance)
    resp = session.get(f'{agent_url}/state')
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
        raise Exception(f'Found more than one Mesos executor with ID {instance_id}')

    return directories[0]


def browse_files(session, instance, sandbox_dir, path):
    """
    Calls the files/browse endpoint on the Mesos agent corresponding to
    the given instance, and for the provided sandbox directory and path
    """
    agent_url = instance_to_agent_url(instance)
    resp = session.get(f'{agent_url}/files/browse', params={'path': os.path.join(sandbox_dir, path or '')})
    if resp.status_code == 404:
        raise Exception(f"Cannot access '{path}' (no such file or directory).")

    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when querying Mesos agent for its files.')

    return resp.json()


def is_directory(entry):
    """Returns true if the given entry is a directory"""
    return entry['nlink'] > 1


def download_file(session, instance, sandbox_dir, path):
    """Calls the Mesos agent files/download API for the given path"""
    logging.info(f'downloading file from sandbox {sandbox_dir} with path {path}')
    agent_url = instance_to_agent_url(instance)
    params = {'path': os.path.join(sandbox_dir, path)}
    resp = session.get(f'{agent_url}/files/download', params=params, stream=True)
    if resp.status_code == 404:
        raise Exception(f"Cannot download '{path}' (file was not found).")

    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Could not download the file.')

    return resp


def cat_for_instance(session, instance, sandbox_dir, path):
    """Outputs the contents of the Mesos sandbox path for the given instance."""
    resp = download_file(session, instance, sandbox_dir, path)
    try:
        for data in resp.iter_content(chunk_size=4096):
            if data:
                logging.info(data.decode())
    except BrokenPipeError as bpe:
        sys.stderr.close()
        logging.exception(bpe)


def dump_sandbox_files(session, instance, job):
    """Logs the contents of each file in the root of the given instance's sandbox."""
    try:
        logging.info(f'Attempting to dump sandbox files for {instance}')
        directory = sandbox_directory(session, instance, job)
        entries = browse_files(session, instance, directory, None)
        for entry in entries:
            try:
                if is_directory(entry):
                    logging.info(f'Skipping over directory {entry}')
                else:
                    cat_for_instance(session, instance, directory, entry['path'])
            except Exception as e:
                logging.info(f'Unable to dump sandbox file {entry}: {e}')
    except Exception as e:
        logging.info(f'Unable to dump sandbox files: {e}')
