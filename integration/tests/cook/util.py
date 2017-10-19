import logging
import uuid
from urllib.parse import urlencode

import importlib
import os

import json
import requests
from retrying import retry

logger = logging.getLogger(__name__)
session = importlib.import_module(os.getenv('COOK_SESSION_MODULE', 'requests')).Session()


def get_in(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


def is_valid_uuid(uuid_to_test, version=4):
    """
    Check if uuid_to_test is a valid UUID.
    Parameters
    ----------
    uuid_to_test : str
    version : {1, 2, 3, 4}
    Returns
    -------
    `True` if uuid_to_test is a valid UUID, otherwise `False`.
    Examples
    --------
    >>> is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a')
    True
    >>> is_valid_uuid('c9bf9e58')
    False
    """
    try:
        uuid_obj = uuid.UUID(uuid_to_test, version=version)
    except:
        return False

    return str(uuid_obj) == uuid_to_test


def retrieve_cook_url(varname='COOK_SCHEDULER_URL', value='http://localhost:12321'):
    cook_url = os.getenv(varname, value)
    logger.info('Using cook url %s' % cook_url)
    return cook_url


def retrieve_mesos_url(varname='MESOS_PORT', value='5050'):
    mesos_port = os.getenv(varname, value)
    cook_url = retrieve_cook_url()
    wait_for_cook(cook_url)
    mesos_master_hosts = settings(cook_url).get('mesos-master-hosts', ['localhost'])
    resp = session.get('http://%s:%s/redirect' % (mesos_master_hosts[0], mesos_port), allow_redirects=False)
    if resp.status_code != 307:
        raise RuntimeError('Unable to find mesos leader, redirect endpoint returned %d' % resp.status_code)
    mesos_url = 'http:%s' % resp.headers['Location']
    logger.info('Using mesos url %s' % mesos_url)
    return mesos_url


def is_not_blank(in_string):
    """Test if a string is not None NOR empty NOR blank."""
    return bool(in_string and in_string.strip())


def get_job_executor_type(cook_url):
    """Returns 'cook' or 'mesos' based on the default executor Cook is configured with."""
    return 'cook' if is_not_blank(get_in(settings(cook_url), 'executor', 'command')) else 'mesos'


def is_connection_error(exception):
    return isinstance(exception, requests.exceptions.ConnectionError)


@retry(retry_on_exception=is_connection_error, stop_max_delay=240000, wait_fixed=1000)
def wait_for_cook(cook_url):
    logger.debug('Waiting for connection to cook...')
    # if connection is refused, an exception will be thrown
    session.get(cook_url)


def settings(cook_url):
    return session.get('%s/settings' % cook_url).json()


def minimal_job(**kwargs):
    job = {
        'command': 'echo hello',
        'cpus': 1,
        'max_retries': 1,
        'mem': 256,
        'name': 'echo',
        'priority': 1,
        'uuid': str(uuid.uuid4())
    }
    job.update(kwargs)
    return job


def submit_job(cook_url, **kwargs):
    job_spec = minimal_job(**kwargs)
    request_body = {'jobs': [job_spec]}
    resp = session.post('%s/rawscheduler' % cook_url, json=request_body)
    return job_spec['uuid'], resp


def query_jobs(cook_url, **kwargs):
    """
    Queries cook for a set of jobs, by job and/or instance uuid. The kwargs
    passed to this function are sent straight through as query parameters on
    the request.
    """
    return session.get('%s/rawscheduler' % cook_url, params=kwargs)


def query_groups(cook_url, **kwargs):
    """
    Queries cook for a set of groups, by groups uuid. The kwargs
    passed to this function are sent straight through as query 
    parameters on the request.
    """
    return session.get('%s/group' % cook_url, params=kwargs)


def load_job(cook_url, job_uuid, assert_response=True):
    """Loads a job by UUID using GET /rawscheduler"""
    response = query_jobs(cook_url, job=[job_uuid])
    if assert_response:
        assert 200 == response.status_code
    return response.json()[0]


def get_job(cook_url, job_uuid):
    """Loads a job by UUID using GET /rawscheduler"""
    return query_jobs(cook_url, job=[job_uuid]).json()[0]


def wait_for_job(cook_url, job_id, status, max_delay=120000):
    @retry(stop_max_delay=max_delay, wait_fixed=2000)
    def wait_for_job_inner():
        job = load_job(cook_url, job_id)
        if not job['status'] == status:
            error_msg = 'Job {} had status {} - expected {}. Details: {}'.format(
                job_id, job['status'], status, json.dumps(job, sort_keys=True))
            logger.info(error_msg)
            raise RuntimeError(error_msg)
        else:
            logger.info('Job {} has status {}'.format(job_id, status))
            return job

    try:
        return wait_for_job_inner()
    except:
        job_final = load_job(cook_url, job_id)
        logger.info('Timeout exceeded waiting for job to reach %s. Job details: %s' % (status, job_final))
        raise


def multi_cluster_tests_enabled():
    return os.getenv('COOK_MULTI_CLUSTER') is not None


def wait_for_exit_code(cook_url, job_id):
    @retry(stop_max_delay=2000, wait_fixed=250)
    def wait_for_exit_code_inner():
        job = load_job(cook_url, job_id)
        if 'exit_code' not in job['instances'][0]:
            error_msg = 'Job {} missing exit_code set! Details: {}'.format(job_id, json.dumps(job, sort_keys=True))
            logger.info(error_msg)
            raise RuntimeError(error_msg)
        else:
            logger.info('Job {} has exit_code {}'.format(job_id, job['instances'][0]['exit_code']))
            return job

    try:
        return wait_for_exit_code_inner()
    except:
        job_final = load_job(cook_url, job_id)
        logger.info('Timeout exceeded waiting for job to receive exit code. Job details: %s' % job_final)
        raise


def get_mesos_state(mesos_url):
    """
    Queries the state.json from mesos
    """
    return session.get('%s/state.json' % mesos_url).json()


@retry(stop_max_delay=120000, wait_fixed=5000)
def wait_for_output_url(cook_url, job_uuid):
    """
    Gets the output_url for the given job, retrying every 5 
    seconds for a maximum of 2 minutes. The retries are 
    necessary because currently the Mesos agent sandbox
    directories are cached in Cook.
    """
    job = load_job(cook_url, job_uuid, assert_response=False)
    instance = job['instances'][0]
    if 'output_url' in instance:
        return instance
    else:
        error_msg = 'Job %s had no output_url' % job['uuid']
        logger.info(error_msg)
        raise RuntimeError(error_msg)


def list_jobs(cook_url, **kwargs):
    """Makes a request to the /list endpoint using the provided kwargs as the query params"""
    if 'start_ms' in kwargs:
        kwargs['start-ms'] = kwargs.pop('start_ms')
    if 'end_ms' in kwargs:
        kwargs['end-ms'] = kwargs.pop('end_ms')
    query_params = urlencode(kwargs)
    resp = session.get('%s/list?%s' % (cook_url, query_params))
    return resp


def contains_job_uuid(jobs, job_uuid):
    """Returns true if jobs contains a job with the given uuid"""
    return any(job for job in jobs if job['uuid'] == job_uuid)


def get_executor(agent_state, executor_id):
    """Returns the executor with id executor_id from agent_state"""
    for framework in agent_state['frameworks']:
        for executor in framework['executors']:
            if executor['id'] == executor_id:
                return executor


def get_user(cook_url, job_uuid):
    """Retrieves the job corresponding to the given job_uuid and returns the user"""
    return load_job(cook_url, job_uuid)['user']

def unscheduled_jobs(cook_url, job_uuid):
    """Retrieves the unscheduled_jobs reasons for the given job_uuid"""
    return session.get('%s/unscheduled_jobs?job=%s' % (cook_url, job_uuid)).json()

def kill_job(cook_url, job_uuid):
    """Kills the given job_uuid"""
    session.delete('%s/rawscheduler?job=%s' % (cook_url, job_uuid))
