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


def minimal_jobs(job_count, **kwargs):
    """Build a list of of multiple homogeneous job specifications"""
    return [ minimal_job(**kwargs) for _ in range(job_count) ]


def submit_jobs(cook_url, job_specs, clones=1, **kwargs):
    """
    Create and submit multiple jobs, either cloned from a single job spec,
    or specified individually in multiple job specs.
    Arguments can be manually passed to the scheduler post via kwargs.
    """
    if isinstance(job_specs, dict):
        job_specs = [job_specs] * clones
    jobs = [ minimal_job(**spec) for spec in job_specs ]
    request_body = {'jobs': jobs}
    request_body.update(kwargs)
    logger.info(request_body)
    resp = session.post(f'{cook_url}/rawscheduler', json=request_body)
    return [ j['uuid'] for j in jobs ], resp


def kill_jobs(cook_url, jobs, assert_response=True):
    """Kill one or more jobs"""
    params = {'job': [unpack_uuid(j) for j in jobs]}
    response = session.delete(f'{cook_url}/rawscheduler', params=params)
    if assert_response:
        assert 204 == response.status_code, response.content
    return response


def submit_job(cook_url, **kwargs):
    """Create and submit a single job"""
    uuids, resp = submit_jobs(cook_url, kwargs, 1)
    return uuids[0], resp


def unpack_uuid(entity):
    """Unpack the UUID string from a job spec, or no-op for UUID strings"""
    return entity['uuid'] if isinstance(entity, dict) else entity


def query_jobs(cook_url, assert_response=False, **kwargs):
    """
    Queries cook for a set of jobs, by job and/or instance uuid. The kwargs
    passed to this function are sent straight through as query parameters on
    the request.
    If the job or instance values are dictionaries (e.g., job_specs),
    then they are automatically unpacked to get their UUIDs.
    """
    for key in ('job', 'instance'):
        if key in kwargs:
            kwargs[key] = map(unpack_uuid, kwargs[key])
    response = session.get(f'{cook_url}/rawscheduler', params=kwargs)
    if assert_response:
        assert 200 == response.status_code
    return response


def query_groups(cook_url, **kwargs):
    """
    Queries cook for a set of groups, by groups uuid. The kwargs
    passed to this function are sent straight through as query 
    parameters on the request.
    """
    return session.get('%s/group' % cook_url, params=kwargs)


def load_job(cook_url, job_uuid, assert_response=True):
    """Loads a job by UUID using GET /rawscheduler"""
    response = query_jobs(cook_url, assert_response, job=[job_uuid])
    return response.json()[0]


def get_job(cook_url, job_uuid):
    """Loads a job by UUID using GET /rawscheduler"""
    return query_jobs(cook_url, job=[job_uuid]).json()[0]


def multi_cluster_tests_enabled():
    return os.getenv('COOK_MULTI_CLUSTER') is not None


def wait_until(query, predicate, max_wait_ms=30000, wait_interval_ms=1000):
    """
    Block until the predicate is true for the result of the provided query.
    `query` is a thunk (nullary callable) that may be called multiple times.
    `predicate` is a unary callable that takes the result value of `query`
    and returns True if the condition is met, or False otherwise.
    See `wait_for_job` for an example of using this method.
    """
    @retry(stop_max_delay=max_wait_ms, wait_fixed=wait_interval_ms)
    def wait_until_inner():
        response = query()
        error_msg = None
        if not predicate(response):
            error_msg = "wait_until condition not yet met, retrying..."
            logger.info(error_msg)
            raise RuntimeError(error_msg)
        else:
            logger.info("wait_until condition satisfied")
            return response

    try:
        return wait_until_inner()
    except:
        final_response = query()
        logger.info(f"Timeout exceeded waiting for condition. Details: {final_response.json()}")
        raise


def all_instances_killed(response):
    """
    Helper method used with the wait_until function.
    Checks a response from query_jobs to see if all jobs and instances have been killed.
    """
    for job in response.json():
        if job['state'] != 'failed':
            return False
        for inst in job['instances']:
            if inst['status'] != 'failed':
                logger.info(f"Job {job['uuid']} instance {inst['task_id']} has non-failure status {inst['status']}.")
                return False
    return True


def wait_for_job(cook_url, job_id, status, max_delay=120000):
    """Wait for the given job's status to change to the specified value."""
    job_id = unpack_uuid(job_id)
    query = lambda: query_jobs(cook_url, True, job=[job_id])
    def predicate(response):
        job = response.json()[0]
        logger.info(f"Job {job_id} has status {job['status']}, expecting {status}.")
        return job['status'] == status
    response = wait_until(query, predicate, max_wait_ms=max_delay, wait_interval_ms=2000)
    return response.json()[0]


def wait_for_exit_code(cook_url, job_id):
    """
    Wait for the given job's exit_code field to appear.
    (Only supported by Cook Executor jobs.)
    """
    job_id = unpack_uuid(job_id)
    query = lambda: query_jobs(cook_url, True, job=[job_id])
    def predicate(response):
        job = response.json()[0]
        if not job['instances']:
            logger.info(f"Job {job_id} has no instances.")
        else:
            inst = job['instances'][0]
            if 'exit_code' not in inst:
                logger.info(f"Job {job_id} instance {inst['task_id']} has no exit code.")
            else:
                logger.info(f"Job {job_id} instance {inst['task_id']} has exit code {inst['exit_code']}.")
                return True
    response = wait_until(query, predicate, max_wait_ms=2000, wait_interval_ms=250)
    return response.json()[0]


def get_mesos_state(mesos_url):
    """
    Queries the state.json from mesos
    """
    return session.get('%s/state.json' % mesos_url).json()


def wait_for_output_url(cook_url, job_uuid):
    """
    Wait for the output_url for the given job to be populated,
    retrying every 5 seconds for a maximum of 2 minutes.
    The retries are necessary because currently the Mesos
    agent sandbox directories are cached in Cook.
    """
    query = lambda: load_job(cook_url, job_uuid, assert_response=False)
    def predicate(job):
        if 'output_url' in job['instances'][0]:
            return True
        else:
            logger.info(f"Job {job['uuid']} had no output_url")
    response = wait_until(query, predicate, max_wait_ms=120000)
    return response['instances'][0]


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
