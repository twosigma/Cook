import importlib
import logging
import os
import os.path
import time
import uuid
from urllib.parse import urlencode

import requests
from retrying import retry

logger = logging.getLogger(__name__)
session = importlib.import_module(os.getenv('COOK_SESSION_MODULE', 'requests')).Session()
session.headers['User-Agent'] = f"Cook-Scheduler-Integration-Tests ({session.headers['User-Agent']})"

DEFAULT_TIMEOUT_MS = 120000


def continuous_integration():
    return os.environ.get('CONTINUOUS_INTEGRATION')


def has_docker_service():
    return os.path.exists('/var/run/docker.sock')


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
    return session.get(f'{cook_url}/settings').json()


def scheduler_info(cook_url):
    resp = session.get(f'{cook_url}/info', auth=None)
    assert resp.status_code == 200
    return resp.json()


def minimal_job(**kwargs):
    job = {
        'command': 'echo Default Test Command',
        'cpus': float(os.getenv('COOK_DEFAULT_JOB_CPUS', 1.0)),
        'max_retries': 1,
        'mem': int(os.getenv('COOK_DEFAULT_JOB_MEM_MB', 256)),
        'name': 'default_test_job',
        'priority': 1,
        'uuid': str(uuid.uuid4())
    }
    job.update(kwargs)
    return job


def minimal_jobs(job_count, **kwargs):
    """Build a list of of multiple homogeneous job specifications"""
    return [minimal_job(**kwargs) for _ in range(job_count)]


def minimal_group(**kwargs):
    """Build a minimal group spec"""
    return dict(uuid=str(uuid.uuid4()), **kwargs)


def submit_jobs(cook_url, job_specs, clones=1, **kwargs):
    """
    Create and submit multiple jobs, either cloned from a single job spec,
    or specified individually in multiple job specs.
    Arguments can be manually passed to the scheduler post via kwargs.
    """
    if isinstance(job_specs, dict):
        job_specs = [job_specs] * clones

    def full_spec(spec):
        if 'uuid' not in spec:
            return minimal_job(**spec)
        else:
            return spec

    jobs = [full_spec(j) for j in job_specs]
    request_body = {'jobs': jobs}
    request_body.update(kwargs)
    logger.info(request_body)
    resp = session.post(f'{cook_url}/rawscheduler', json=request_body)
    return [j['uuid'] for j in jobs], resp


def retry_jobs(cook_url, assert_response=True, use_deprecated_post=False, **kwargs):
    """Retry one or more jobs and/or groups of jobs"""
    request_verb = session.post if use_deprecated_post else session.put
    response = request_verb(f'{cook_url}/retry', json=kwargs)
    if assert_response:
        response_info = {'code': response.status_code, 'msg': response.content}
        assert response.status_code in (200, 201), response_info
        retried_job_count = int(response.text)
        # response code 200 OK implies zero retried jobs
        assert response.status_code != 200 or retried_job_count == 0, response_info
        # response code 201 Created implies non-zero retried jobs
        assert response.status_code != 201 or retried_job_count > 0, response_info
    return response


def kill_jobs(cook_url, jobs, assert_response=True):
    """Kill one or more jobs"""
    params = {'job': [unpack_uuid(j) for j in jobs]}
    response = session.delete(f'{cook_url}/rawscheduler', params=params)
    if assert_response:
        assert 204 == response.status_code, response.content
    return response


def kill_groups(cook_url, groups, assert_response=True):
    """Kill one or more groups of jobs"""
    params = {'uuid': [unpack_uuid(g) for g in groups]}
    response = session.delete(f'{cook_url}/group', params=params)
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


def __get(cook_url, endpoint, assert_response=False, **kwargs):
    """Makes a GET request to the given root URL and endpoint"""
    if 'partial' in kwargs:
        kwargs['partial'] = 'true' if (kwargs['partial'] in [True, 'true', '1']) else 'false'
    response = session.get(f'{cook_url}/{endpoint}', params=kwargs)
    if assert_response:
        assert 200 == response.status_code
    return response


def query_jobs_via_rawscheduler_endpoint(cook_url, assert_response=False, **kwargs):
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

    return __get(cook_url, 'rawscheduler', assert_response, **kwargs)


def query_resource(cook_url, resource, assert_response=False, **kwargs):
    """
    Queries cook for a set of entities by uuid. The kwargs
    passed to this function are sent straight through as
    query parameters on the request. If the uuid values are
    dictionaries (e.g., job_specs), then they are
    automatically unpacked to get their UUIDs.
    """
    kwargs['uuid'] = [unpack_uuid(u) for u in kwargs['uuid']]
    return __get(cook_url, resource, assert_response, **kwargs)


def query_jobs(cook_url, assert_response=False, **kwargs):
    """Queries cook for a set of jobs by job uuid"""
    return query_resource(cook_url, 'jobs', assert_response, **kwargs)


def query_instances(cook_url, assert_response=False, **kwargs):
    """Queries cook for a set of job instances by instance uuid"""
    return query_resource(cook_url, 'instances', assert_response, **kwargs)


def query_groups(cook_url, **kwargs):
    """
    Queries cook for a set of groups, by groups uuid. The kwargs
    passed to this function are sent straight through as query 
    parameters on the request.
    """
    return session.get('%s/group' % cook_url, params=kwargs)


def load_resource(cook_url, resource, uuid, assert_response=True):
    """Loads an entity by UUID using GET /resource/UUID"""
    response = session.get(f'{cook_url}/{resource}/{uuid}')
    if assert_response:
        assert 200 == response.status_code
    return response.json()


def load_job(cook_url, job_uuid, assert_response=True):
    """Loads a job by UUID using GET /jobs/UUID"""
    return load_resource(cook_url, 'jobs', job_uuid, assert_response)


def load_instance(cook_url, instance_uuid, assert_response=True):
    """Loads a job instance by UUID using GET /instances/UUID"""
    return load_resource(cook_url, 'instances', instance_uuid, assert_response)


def multi_cluster_tests_enabled():
    return os.getenv('COOK_MULTI_CLUSTER') is not None


def wait_until(query, predicate, max_wait_ms=DEFAULT_TIMEOUT_MS, wait_interval_ms=1000):
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
        if not predicate(response):
            error_msg = "wait_until condition not yet met, retrying..."
            logger.debug(error_msg)
            raise RuntimeError(error_msg)
        else:
            logger.info("wait_until condition satisfied")
            return response

    try:
        return wait_until_inner()
    except:
        final_response = query()
        try:
            details = final_response.content
        except AttributeError:
            details = str(final_response)
        logger.info(f"Timeout exceeded waiting for condition. Details: {details}")
        raise


def all_instances_done(response, accepted_states=('success', 'failed')):
    """
    Helper method used with the wait_until function.
    Checks a response from query_jobs to see if all jobs and instances have completed.
    """
    for job in response.json():
        if job['state'] not in accepted_states:
            return False
        for inst in job['instances']:
            if inst['status'] not in accepted_states:
                logger.info(f"Job {job['uuid']} instance {inst['task_id']} has unaccepted status {inst['status']}.")
                return False
    return True


def all_instances_killed(response):
    """
    Helper method used with the wait_until function.
    Checks a response from query_jobs to see if all jobs and instances have been killed.
    """
    return all_instances_done(response, accepted_states=['failed'])


def group_some_job_started(group_response):
    """
    Helper method used with the wait_until function.
    Checks a response from group_detail_query to see if any job in the group has started.
    """
    group = group_response.json()[0]
    running_count = group['running']
    logger.info(f"Currently {running_count} jobs running in group {group['uuid']}")
    return running_count > 0


def group_some_job_done(group_response):
    """
    Helper method used with the wait_until function.
    Checks a response from group_detail_query to see if any job in the group has completed.
    """
    group = group_response.json()[0]
    completed_count = group['completed']
    logger.info(f"Currently {completed_count} jobs completed in group {group['uuid']}")
    return completed_count > 0


def group_detail_query(cook_url, group_uuid, assert_response=True):
    """Get a group with full status details, returning the response object."""
    response = query_groups(cook_url, uuid=[group_uuid], detailed='true')
    if assert_response:
        assert 200 == response.status_code, response.content
    return response


def wait_for_job(cook_url, job_id, status, max_wait_ms=DEFAULT_TIMEOUT_MS):
    """Wait for the given job's status to change to the specified value."""
    return wait_for_jobs(cook_url, [job_id], status, max_wait_ms)[0]


def wait_for_jobs(cook_url, job_ids, status, max_wait_ms=DEFAULT_TIMEOUT_MS):
    def query():
        return query_jobs(cook_url, True, uuid=job_ids)

    def predicate(resp):
        jobs = resp.json()
        for job in jobs:
            logger.info(f"Job {job['uuid']} has status {job['status']}, expecting {status}.")
        return all([job['status'] == status for job in jobs])

    response = wait_until(query, predicate, max_wait_ms=max_wait_ms, wait_interval_ms=2000)
    return response.json()


def wait_for_exit_code(cook_url, job_id, max_wait_ms=DEFAULT_TIMEOUT_MS):
    """
    Wait for the given job's exit_code field to appear.
    (Only supported by Cook Executor jobs.)
    Returns an up-to-date job description object on success,
    and raises an exception if the max_wait_ms wait time is exceeded.
    """
    job_id = unpack_uuid(job_id)

    def query():
        return query_jobs(cook_url, True, uuid=[job_id])

    def predicate(resp):
        job = resp.json()[0]
        if not job['instances']:
            logger.info(f"Job {job_id} has no instances.")
        else:
            inst = job['instances'][0]
            if 'exit_code' not in inst:
                logger.info(f"Job {job_id} instance {inst['task_id']} has no exit code.")
            else:
                logger.info(f"Job {job_id} instance {inst['task_id']} has exit code {inst['exit_code']}.")
                return True

    response = wait_until(query, predicate, max_wait_ms=max_wait_ms)
    return response.json()[0]


def wait_for_sandbox_directory(cook_url, job_id):
    """
    Wait for the given job's sandbox_directory field to appear.
    Returns an up-to-date job description object on success,
    and raises an exception if the max_wait_ms wait time is exceeded.
    """
    job_id = unpack_uuid(job_id)

    cook_settings = settings(cook_url)
    cache_ttl_ms = get_in(cook_settings, 'agent-query-cache', 'ttl-ms')
    max_wait_ms = min(4 * cache_ttl_ms, 4 * 60 * 1000)

    def query():
        response = query_jobs(cook_url, True, uuid=[job_id])
        return response.json()[0]

    def predicate(job):
        if not job['instances']:
            logger.info(f"Job {job_id} has no instances.")
        else:
            inst = job['instances'][0]
            if 'sandbox_directory' not in inst:
                logger.info(f"Job {job_id} instance {inst['task_id']} has no sandbox directory.")
            else:
                logger.info(
                    f"Job {job_id} instance {inst['task_id']} has sandbox directory {inst['sandbox_directory']}.")
                return True

    return wait_until(query, predicate, max_wait_ms=max_wait_ms, wait_interval_ms=250)


def wait_for_end_time(cook_url, job_id, max_wait_ms=DEFAULT_TIMEOUT_MS):
    """
    Wait for the given job's end_time field to appear in instance 0.
    Returns an up-to-date job description object on success,
    and raises an exception if the max_wait_ms wait time is exceeded.
    """
    job_id = unpack_uuid(job_id)

    def query():
        return query_jobs(cook_url, True, uuid=[job_id])

    def predicate(resp):
        job = resp.json()[0]
        if not job['instances']:
            logger.info(f"Job {job_id} has no instances.")
        else:
            inst = job['instances'][0]
            if 'end_time' not in inst:
                logger.info(f"Job {job_id} instance {inst['task_id']} has no end time.")
            else:
                logger.info(f"Job {job_id} instance {inst['task_id']} has end_time {inst['end_time']}.")
                return True

    response = wait_until(query, predicate, max_wait_ms=max_wait_ms)
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

    def query():
        return load_job(cook_url, job_uuid, assert_response=False)

    def predicate(job):
        if 'output_url' in job['instances'][0]:
            return True
        else:
            logger.info(f"Job {job['uuid']} had no output_url")

    response = wait_until(query, predicate)
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


def unscheduled_jobs(cook_url, *job_uuids, partial=None):
    """Retrieves the unscheduled_jobs reasons for the given job_uuid"""
    query_params = [('job', u) for u in job_uuids]
    if partial is not None:
        query_params.append(('partial', partial))
    resp = session.get(f'{cook_url}/unscheduled_jobs?{urlencode(query_params)}')
    job_reasons = resp.json() if resp.status_code == 200 else []
    return job_reasons, resp


def wait_for_instance(cook_url, job_uuid):
    """Waits for the job with the given job_uuid to have a single instance, and returns the instance uuid"""
    job = wait_until(lambda: load_job(cook_url, job_uuid), lambda j: len(j['instances']) == 1)
    instance = job['instances'][0]
    return instance


def sleep_for_publish_interval(cook_url):
    # allow enough time for progress and sandbox updates to be submitted
    cook_settings = settings(cook_url)
    progress_publish_interval_ms = get_in(cook_settings, 'progress', 'publish-interval-ms')
    wait_publish_interval_ms = min(3 * progress_publish_interval_ms, 20000)
    time.sleep(wait_publish_interval_ms / 1000.0)


def progress_line(cook_url, percent, message):
    """Simple text replacement of regex string using expected patterns of (\d+), (?: )? and (.*)."""
    cook_settings = settings(cook_url)
    regex_string = get_in(cook_settings, 'executor', 'default-progress-regex-string')
    if not regex_string:
        regex_string = 'progress:\s+([0-9]*\.?[0-9]+)($|\s+.*)'
    if '([0-9]*\.?[0-9]+)' not in regex_string:
        raise Exception(f'([0-9]*\.?[0-9]+) not present in {regex_string} regex string')
    if '($|\s+.*)' not in regex_string:
        raise Exception(f'($|\s+.*) not present in {regex_string} regex string')
    return (regex_string
            .replace('([0-9]*\.?[0-9]+)', str(percent))
            .replace('($|\s+.*)', str(f' {message}'))
            .replace('\s+', ' ')
            .replace('\\', ''))


def group_submit_kill_retry(cook_url, retry_failed_jobs_only):
    """
    Helper method for integration tests on groups, following these steps:
    1) Creates a group of 10 jobs
    2) Waits for at least one job to start
    3) Kills all the jobs
    4) Retries the jobs
    5) Waits for at least one job to start (again)
    6) Finally kills all the jobs again (clean up)
    Returns the job info (json response) for the group's jobs after step 5.
    """
    group_spec = minimal_group()
    group_uuid = group_spec['uuid']
    job_spec = {'group': group_uuid, 'command': f'sleep 1'}
    try:
        jobs, resp = submit_jobs(cook_url, job_spec, 10, groups=[group_spec])
        assert resp.status_code == 201, resp

        def group_query():
            return group_detail_query(cook_url, group_uuid)

        # wait for some job to start
        wait_until(group_query, group_some_job_started)
        # kill all jobs in the group (and wait for the kill to complete)
        logger.info(f'Killing all jobs in group {group_uuid}.')
        kill_groups(cook_url, [group_uuid])

        def jobs_query():
            return query_jobs(cook_url, True, uuid=jobs)

        wait_until(jobs_query, all_instances_done)
        # retry all jobs in the group
        retry_jobs(cook_url, retries=2, groups=[group_uuid], failed_only=retry_failed_jobs_only)
        # wait for some job to start
        wait_until(group_query, group_some_job_started)
        # return final job details to caller for assertion checks
        return query_jobs(cook_url, assert_response=True, uuid=jobs).json()
    finally:
        # ensure that we don't leave a bunch of jobs running/waiting
        kill_groups(cook_url, [group_uuid])


def group_submit_retry(cook_url, command, predicate_statuses, retry_failed_jobs_only=True):
    """
    Helper method for integration tests on groups, following these steps:
    1) Creates a group of 5 jobs
    2) Waits for the job statuses to match those in predicate_statuses
    3) Retries the jobs
    4) Waits for the job statuses to match those in predicate_statuses (again)
    5) Finally kills all the jobs again (clean up)
    Returns the job info (json response) for the group's jobs after step 4.
    """
    job_count = 5
    group_spec = minimal_group()
    group_uuid = group_spec['uuid']
    job_spec = {'group': group_uuid, 'max_retries': 1, 'command': command}

    def group_query():
        return group_detail_query(cook_url, group_uuid)

    def status_condition(response):
        group = response.json()[0]
        statuses_map = {x: group[x] for x in predicate_statuses}
        status_counts = statuses_map.values()
        # for running & waiting, we want at least one running (not all waiting)
        not_all_waiting = group['waiting'] != job_count
        logger.debug(f"Currently {statuses_map} jobs in group {group['uuid']}")
        return not_all_waiting and sum(status_counts) == job_count

    try:
        jobs, resp = submit_jobs(cook_url, job_spec, job_count, groups=[group_spec])
        assert resp.status_code == 201, resp
        # wait for the expected job statuses specified in predicate_statuses
        wait_until(group_query, status_condition)
        # retry all failed jobs in the group (if any)
        retry_jobs(cook_url, increment=1, failed_only=retry_failed_jobs_only, groups=[group_uuid])
        # wait again for the expected job statuses specified in predicate_statuses
        wait_until(group_query, status_condition)
        # return final job details to caller for assertion checks
        return query_jobs(cook_url, assert_response=True, uuid=jobs).json()
    finally:
        # ensure that we don't leave a bunch of jobs running/waiting
        kill_groups(cook_url, [group_uuid])


def user_current_usage(cook_url, **kwargs):
    """
    Queries cook for a user's current resource usage
    based on their currently running jobs.
    """
    return session.get('%s/usage' % cook_url, params=kwargs)


def retrieve_progress_file_env(cook_url):
    """Retrieves the environment variable used by the cook executor to lookup the progress file."""
    cook_settings = settings(cook_url)
    default_value = 'EXECUTOR_PROGRESS_OUTPUT_FILE'
    return get_in(cook_settings, 'executor', 'environment', 'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV') or default_value
