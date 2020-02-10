import functools
import importlib
import itertools
import json
import logging
import os
import os.path
import re
import subprocess
import threading
import time
import unittest
import uuid
from datetime import datetime
from urllib.parse import urlencode, urlparse

import numpy
import requests
from retrying import retry

from tests.cook import mesos

logger = logging.getLogger(__name__)
session = importlib.import_module(os.getenv('COOK_SESSION_MODULE', 'requests')).Session()
session.headers['User-Agent'] = f"Cook-Scheduler-Integration-Tests ({session.headers['User-Agent']})"

# default time limit for each individual integration test
# if a test takes more than 10 minutes, it's probably broken
DEFAULT_TEST_TIMEOUT_SECS = int(os.getenv('COOK_TEST_DEFAULT_TEST_TIMEOUT_SECS', 600))

# default time limit used by most wait_* utility functions
# 2 minutes should be more than sufficient on most cases
DEFAULT_TIMEOUT_MS = int(os.getenv('COOK_TEST_DEFAULT_TIMEOUT_MS', 120000))

# default wait interval (i.e. time between attempts) used by most wait_* utility functions
DEFAULT_WAIT_INTERVAL_MS = int(os.getenv('COOK_TEST_DEFAULT_WAIT_INTERVAL_MS', 1000))

# Name of our custom HTTP header for user impersonation
IMPERSONATION_HEADER = 'X-Cook-Impersonate'

# Reason used by tests that should be skipped on clusters with ephemeral hosts
EPHEMERAL_HOSTS_SKIP_REASON = 'If the cluster under test has ephemeral hosts, then it is generally ' \
                              'a bad idea to use HOSTNAME EQUALS constraints, because it can cause ' \
                              'the process responsible for launching hosts to launch hosts that ' \
                              'never get used'

# We need a way for tests to explicitly specify
# that they don't want to specify a pool for a
# particular job submission
POOL_UNSPECIFIED = 'COOK_TEST_POOL_UNSPECIFIED'


def continuous_integration():
    """Returns true if the CONTINUOUS_INTEGRATION environment variable is set, as done by Travis-CI."""
    return to_bool(os.getenv('CONTINUOUS_INTEGRATION'))


def has_docker_service():
    """Returns true if docker services appear to be available to the testing environment."""
    return os.path.exists('/var/run/docker.sock')


# Default user for multi-user test runs
_default_user_name = 'root'
_default_admin_name = 'root'
_default_impersonator_name = 'poser'


def _get_default_user_name():
    return os.getenv('USER', _default_user_name)


@functools.lru_cache()
def _test_user_ids():
    """
    Get the numeric user suffixes for this test worker.
    Returns the range 0 to 1 million if COOK_MAX_TEST_USERS is not set.
    If this is a distributed run with a limited number of users,
    e.g., 10 per worker, then this function returns range(0, 10) for worker 0,
    or range(20, 30) for worker 2.
    """
    pytest_worker = os.getenv('PYTEST_XDIST_WORKER')
    max_test_users = int(os.getenv('COOK_MAX_TEST_USERS', 0))
    if pytest_worker and max_test_users:
        pytest_worker_id = int(pytest_worker[2:])  # e.g., "gw4" -> 4
        test_user_min_id = max_test_users * pytest_worker_id
        test_user_max_id = test_user_min_id + max_test_users
        return range(test_user_min_id, test_user_max_id)
    else:
        return range(1000000)


def _test_user_names(test_name_prefix=None):
    """
    Returns a generator of unique test user names, with form {PREFIX}{ID}.
    The COOK_TEST_USER_PREFIX environment variable is used by default;
    otherwise, the test_name_prefix value is used as the PREFIX.
    """
    name_prefix = os.getenv('COOK_TEST_USER_PREFIX', test_name_prefix)
    return (f'{name_prefix}{i}' for i in _test_user_ids())


# Shell command used to obtain Kerberos credentials for a given test user
_kerberos_missing_cmd = 'echo "MISSING COOK_KERBEROS_TEST_AUTH_CMD" && exit 1'
_kerberos_auth_cmd = os.getenv('COOK_KERBEROS_TEST_AUTH_CMD', _kerberos_missing_cmd)


class _AuthenticatedUser(object):
    """
    Object representing a Cook user, which holds authentication details.
    User objects can be used with python's `with` blocks
    to conveniently set a user for a sequence of commands.
    """

    def __init__(self, name, impersonatee=None):
        self.name = name
        self.impersonatee = impersonatee
        self.previous_impersonatee = None

    def impersonating(self, other_user):
        other_username = other_user.name if isinstance(other_user, _AuthenticatedUser) else other_user
        return type(self)(self.name, impersonatee=other_username)

    def __enter__(self):
        if self.impersonatee:
            logger.debug(f'Starting impersonation of {self.impersonatee}')
            self.previous_impersonatee = session.headers.get(IMPERSONATION_HEADER)
            session.headers[IMPERSONATION_HEADER] = self.impersonatee

    def __exit__(self, ex_type, ex_val, ex_trace):
        if self.impersonatee:
            logger.debug(f'Stopping impersonation of {self.impersonatee}')
            if self.previous_impersonatee:
                session.headers[IMPERSONATION_HEADER] = self.previous_impersonatee
                self.previous_impersonatee = None
            else:
                del session.headers[IMPERSONATION_HEADER]


class _BasicAuthUser(_AuthenticatedUser):
    """
    Object representing a Cook user with HTTP Basic Auth credentials.
    """

    def __init__(self, name, impersonatee=None):
        super().__init__(name, impersonatee)
        self.auth = (name, '')
        self.previous_auth = None

    def __enter__(self):
        logger.info(f"Setting auth to {self.auth}")
        global session
        super().__enter__()
        assert self.previous_auth is None
        self.previous_auth = session.auth
        session.auth = self.auth

    def __exit__(self, ex_type, ex_val, ex_trace):
        global session
        super().__exit__(ex_type, ex_val, ex_trace)
        assert self.previous_auth is not None
        session.auth = self.previous_auth
        logger.info(f"Resetting auth to {session.auth}")
        self.previous_auth = None


class _KerberosUser(_AuthenticatedUser):
    """
    Object representing a Cook user with Kerberos credentials.
    """

    def __init__(self, name, impersonatee=None):
        super().__init__(name, impersonatee)
        self.auth = None
        self.previous_token = None
        self.stop_event = None

    def _generate_kerberos_ticket_for_user(self, username):
        """
        Get a Kerberos authentication ticket for the given user.
        Depends on COOK_KERBEROS_TEST_AUTH_CMD being set in the environment.
        """
        subcommand = (_kerberos_auth_cmd
                      .replace('{{COOK_USER}}', username)
                      .replace('{{COOK_SCHEDULER_URL}}', retrieve_cook_url()))
        return subprocess.check_output(subcommand, shell=True).rstrip()

    def _reset_auth_header(self, stop):
        global session
        if not stop.is_set():
            logger.info(f'Refreshing kerberos tickets for {self.name}')
            session.headers['Authorization'] = self._generate_kerberos_ticket_for_user(self.name)
            threading.Timer(60.0, lambda: self._reset_auth_header(stop)).start()
        else:
            logger.info(f'Stopping kerberos ticket refresh for {self.name}')

    def __enter__(self):
        global session
        logger.info(f"Setting kerberos user {self.name}")
        super().__enter__()
        assert self.previous_token is None
        assert self.stop_event is None
        self.previous_token = session.headers.get('Authorization')
        self.stop_event = threading.Event()
        self._reset_auth_header(self.stop_event)

    def __exit__(self, ex_type, ex_val, ex_trace):
        global session
        super().__exit__(ex_type, ex_val, ex_trace)
        if self.stop_event is not None:
            self.stop_event.set()
            self.stop_event = None
        if self.previous_token is None:
            del session.headers['Authorization']
        else:
            session.headers['Authorization'] = self.previous_token
            self.previous_token = None
        logger.info(f"Resetting kerberos auth back from {self.name}")


class UserFactory(object):
    """
    Factory object used to create unique user names for a given test function.
    Usernames are composed of the test name and and increasing integer values.
    """

    def __init__(self, test_handle):
        # Select the authentication scheme
        if http_basic_auth_enabled():
            self.user_class = _BasicAuthUser
        elif kerberos_enabled():
            self.user_class = _KerberosUser
        else:
            raise NotImplementedError(f'Unsupported user authentication scheme: {_cook_auth_scheme()}')
        # Set up generator for new user objects
        if test_handle:
            test_id = test_handle.id()
            test_base_name = test_id[test_id.rindex('.test_') + 6:].lower()
            self.__user_generator = _test_user_names(test_base_name)

        self.cook_url = retrieve_cook_url()
        _wait_for_cook(self.cook_url)

    def new_user(self):
        """Return a fresh user object."""
        user = self.user_class(next(self.__user_generator))
        with self.admin():
            reset_share_and_quota(self.cook_url, user.name)
        return user

    def new_users(self, count=None):
        """Return a sequence of `count` fresh user objects."""
        users = [self.user_class(x) for x in itertools.islice(self.__user_generator, 0, count)]
        with self.admin():
            for user in users:
                reset_share_and_quota(self.cook_url, user.name)
        return users

    @functools.lru_cache()
    def default(self):
        """Return the default user"""
        return self.user_class(_get_default_user_name())

    @functools.lru_cache()
    def admin(self):
        """Return the administrator user"""
        name = os.getenv('COOK_ADMIN_USER_NAME', _default_admin_name)
        return self.user_class(name)

    @functools.lru_cache()
    def impersonator(self):
        """Return the impersonator user"""
        name = os.getenv('COOK_IMPERSONATOR_USER_NAME', _default_impersonator_name)
        return self.user_class(name)

    def specific_user(self, name):
        """Return the specific user provided"""
        return self.user_class(name)


def multi_cluster_tests_enabled():
    """
    Returns true if the COOK_MULTI_CLUSTER environment variable is set,
    indicating that multiple cook scheduler instances are running.
    """
    return os.getenv('COOK_MULTI_CLUSTER') is not None


@functools.lru_cache()
def _cook_auth_scheme():
    """Get the authentication scheme name from the cook scheduler info endpoint"""
    cook_url = retrieve_cook_url()
    _wait_for_cook(cook_url)
    cook_info = scheduler_info(cook_url)
    logger.info(f"Cook's authentication scheme is {cook_info['authentication-scheme']}")
    return cook_info['authentication-scheme']


def http_basic_auth_enabled():
    """Returns true if Cook was configured to use the http-basic authentication scheme."""
    return 'http-basic' == _cook_auth_scheme()


def kerberos_enabled():
    """Returns true if Cook was configured to use the Kerberos authentication scheme."""
    return 'kerberos' == _cook_auth_scheme()


def multi_user_tests_enabled():
    """Returns true if Cook was configured to support multiple users."""
    return http_basic_auth_enabled() or kerberos_enabled()


def get_in(dct, *keys):
    for key in keys:
        if not dct:
            return None
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


@functools.lru_cache()
def retrieve_cook_url(varname='COOK_SCHEDULER_URL', value='http://localhost:12321'):
    cook_url = os.getenv(varname, value)
    logger.info('Using cook url %s' % cook_url)
    return cook_url


@functools.lru_cache()
def retrieve_mesos_url(varname='MESOS_PORT', value='5050'):
    mesos_url = os.getenv('COOK_MESOS_LEADER_URL')
    if mesos_url is None:
        mesos_master_host = 'localhost'
        mesos_port = os.getenv(varname, value)
        if os.getenv('COOK_TEST_DERIVE_MESOS_HOST') is not None:
            cook_url = retrieve_cook_url()
            _wait_for_cook(cook_url)
            mesos_master = settings(cook_url)['compute-clusters'][0]['config'].get('master')
            if not mesos_master:
                raise RuntimeError('Unable to derive Mesos host, mesos-master is not present in settings')

            mesos_master_parts = mesos_master.split(',')
            result = urlparse(mesos_master_parts[0])
            if not result.hostname:
                raise RuntimeError(f'Unable to derive Mesos host, hostname is not present in {result}')

            mesos_master_host = result.hostname

        logger.debug(f'Using mesos master host {mesos_master_host}')
        resp = session.get(f'http://{mesos_master_host}:{mesos_port}/redirect', allow_redirects=False)
        if resp.status_code != 307:
            raise RuntimeError(f'Unable to find mesos leader, redirect endpoint returned {resp.status_code}')
        mesos_url = f"http:{resp.headers['Location']}"
    logger.info(f'Using mesos url {mesos_url}')
    return mesos_url


def is_not_blank(in_string):
    """Test if a string is not None NOR empty NOR blank."""
    return bool(in_string and in_string.strip())


def get_job_executor_type():
    """Returns 'cook' or 'mesos' based on the default executor Cook is configured with."""
    return 'cook' if is_cook_executor_in_use() else 'mesos'


def is_connection_error(exception):
    return isinstance(exception, requests.exceptions.ConnectionError)


@retry(retry_on_exception=is_connection_error, stop_max_delay=240000, wait_fixed=1000)
def _wait_for_cook(cook_url):
    logger.debug('Waiting for connection to cook...')
    # if connection is refused, an exception will be thrown
    session.get(cook_url)


def init_cook_session(*cook_urls):
    for cook_url in cook_urls:
        _wait_for_cook(cook_url)

    # If we're using basic auth, init the session auth.
    # But, don't override it if it's already set
    # (for example, by a with user block).
    if http_basic_auth_enabled() and session.auth is None:
        session.auth = UserFactory(None).default().auth
        logger.info(f'Session auth set to {session.auth}')


def settings(cook_url):
    return session.get(f'{cook_url}/settings').json()


@functools.lru_cache()
def scheduler_info(cook_url):
    resp = session.get(f'{cook_url}/info', auth=None)
    response_info = {'code': resp.status_code, 'msg': resp.content}
    assert resp.status_code == 200, response_info
    return resp.json()


def docker_image():
    return os.getenv('COOK_TEST_DOCKER_IMAGE')


def docker_working_directory():
    return os.getenv('COOK_TEST_DOCKER_WORKING_DIRECTORY')


def get_default_cpus():
    return float(os.getenv('COOK_DEFAULT_JOB_CPUS', 0.5))


def make_temporal_uuid():
    """Make a UUID object that has a datestamp as its prefix. The datestamp being yymmddhh. This will cluster
    UUID's in a temporal manner, so jobs submitted on the same day or week will be clustered together in the
    datomic storage"""
    base_uuid = uuid.uuid4()
    now = datetime.now()
    date_prefix = now.strftime("%y%m%d%H")
    suffix = str(base_uuid)[8:]
    temporal_uuid = uuid.UUID(date_prefix + suffix)
    return temporal_uuid


def job_label():
    return os.getenv('COOK_TEST_JOB_LABEL')


def minimal_job(**kwargs):
    job = {
        'command': 'echo Default Test Command',
        'cpus': get_default_cpus(),
        'max_retries': 1,
        'mem': int(os.getenv('COOK_DEFAULT_JOB_MEM_MB', 256)),
        'name': 'default_test_job',
        'priority': 1,
        'uuid': str(make_temporal_uuid())
    }
    image = docker_image()
    if image:
        work_dir = docker_working_directory()
        docker_parameters = []
        if work_dir:
            docker_parameters.append({'key': 'workdir', 'value': work_dir})
        job['container'] = {
            'type': 'docker',
            'docker': {
                'image': image,
                'network': 'HOST',
                'force-pull-image': False,
                'parameters': docker_parameters
            }
        }

    label = job_label()
    if label:
        label_parts = label.split('=')
        if len(label_parts) != 2:
            raise Exception('COOK_TEST_JOB_LABEL must be of the form "key=value"')

        if 'labels' not in job:
            job['labels'] = {}

        job['labels'][label_parts[0]] = label_parts[1]

    job.update(kwargs)
    no_container_volume = os.getenv('COOK_NO_CONTAINER_VOLUME') is not None
    if (not no_container_volume
            and is_cook_executor_in_use()
            and 'container' in job):
        config = settings(retrieve_cook_url())
        executor_path = get_in(config, 'executor', 'command')
        if not executor_path.startswith('.'):
            if 'volumes' not in job['container']:
                job['container']['volumes'] = []
            executor_dir = os.path.dirname(os.path.dirname(executor_path))
            job['container']['volumes'].append({'host-path': executor_dir,
                                                'container-path': executor_dir})
    return job


def minimal_jobs(job_count, **kwargs):
    """Build a list of of multiple homogeneous job specifications"""
    return [minimal_job(**kwargs) for _ in range(job_count)]


def minimal_group(**kwargs):
    """Build a minimal group spec"""
    return dict(uuid=str(make_temporal_uuid()), **kwargs)


def default_submit_pool():
    """
    Returns the default pool to use for job submissions, or None if
    COOK_TEST_DEFAULT_SUBMIT_POOL is not defined. Returning None here is OK
    because sending no explicit pool means "use Cook Scheduler's default".
    """
    return os.getenv('COOK_TEST_DEFAULT_SUBMIT_POOL')


def submit_jobs(cook_url, job_specs, clones=1, pool=None, headers=None, log_request_body=True, **kwargs):
    """
    Create and submit multiple jobs, either cloned from a single job spec,
    or specified individually in multiple job specs.
    Arguments can be manually passed to the scheduler post via kwargs.
    """
    if headers is None:
        headers = {}
    if isinstance(job_specs, dict):
        job_specs = [job_specs] * clones

    def full_spec(spec):
        if 'uuid' not in spec:
            return minimal_job(**spec)
        else:
            return spec

    jobs = [full_spec(j) for j in job_specs]
    request_body = {'jobs': jobs}
    default_pool = default_submit_pool()
    if pool == POOL_UNSPECIFIED:
        logger.info('Submitting with no explicit pool (via POOL_UNSPECIFIED)')
    elif pool:
        logger.info(f'Submitting explicitly to the {pool} pool')
        request_body['pool'] = pool
    elif 'x-cook-pool' in headers:
        logger.info(f"Submitting explicitly to the {headers['x-cook-pool']} pool via x-cook-pool header")
    elif default_pool:
        logger.info(f'Submitting explicitly to the {default_pool} pool (set as default)')
        request_body['pool'] = default_pool
    else:
        logger.info(f'Submitting with no explicit pool')

    request_body.update(kwargs)
    if log_request_body:
        logger.info(request_body)
    else:
        logger.info('Not logging request body for job submission')

    resp = session.post(f'{cook_url}/jobs', json=request_body, headers=headers)
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


def kill_jobs(cook_url, jobs, assert_response=True, expected_status_code=204, log_before_killing=False):
    """Kill one or more jobs"""
    if log_before_killing:
        logger.info(f'Jobs: {json.dumps(query_jobs(cook_url, True, uuid=jobs).json(), indent=2)}')

    chunksize = 100
    chunks = [jobs[i:i + chunksize] for i in range(0, len(jobs), chunksize)]
    response = []
    for chunk in chunks:
        params = {'job': [unpack_uuid(j) for j in chunk]}
        response = session.delete(f'{cook_url}/rawscheduler', params=params)
        if assert_response:
            assert expected_status_code == response.status_code, response.text
    return response


def kill_groups(cook_url, groups, assert_response=True, expected_status_code=204):
    """Kill one or more groups of jobs"""
    params = {'uuid': [unpack_uuid(g) for g in groups]}
    response = session.delete(f'{cook_url}/group', params=params)
    if assert_response:
        assert expected_status_code == response.status_code, response.content
    return response


def submit_job(cook_url, pool=None, headers=None, **kwargs):
    """Create and submit a single job"""
    if headers is None:
        headers = {}
    uuids, resp = submit_jobs(cook_url, job_specs=[kwargs], pool=pool, headers=headers)
    return uuids[0], resp


def unpack_uuid(entity):
    """Unpack the UUID string from a job spec, or no-op for UUID strings"""
    return entity['uuid'] if isinstance(entity, dict) else entity


def to_bool(v):
    """Converts the given argument to a boolean value"""
    return v is True or str(v).lower() in ['true', '1']


def __get(cook_url, endpoint, assert_response=False, **kwargs):
    """Makes a GET request to the given root URL and endpoint"""
    if 'partial' in kwargs:
        kwargs['partial'] = 'true' if to_bool(kwargs['partial']) else 'false'
    response = session.get(f'{cook_url}/{endpoint}', params=kwargs)
    if assert_response:
        response_info = {'code': response.status_code, 'msg': response.content}
        if 200 != response.status_code:
            logging.info(f'Got a non-200 response: {response_info}')
        assert 200 == response.status_code, response_info
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
        assert 200 == response.status_code, f'Expected 200, got {response.status_code} with body {response.text}'
    return response.json()


def load_job(cook_url, job_uuid, assert_response=True):
    """Loads a job by UUID using GET /jobs/UUID"""
    return load_resource(cook_url, 'jobs', job_uuid, assert_response)


def load_instance(cook_url, instance_uuid, assert_response=True):
    """Loads a job instance by UUID using GET /instances/UUID"""
    return load_resource(cook_url, 'instances', instance_uuid, assert_response)


def wait_until(query, predicate, max_wait_ms=DEFAULT_TIMEOUT_MS, wait_interval_ms=DEFAULT_WAIT_INTERVAL_MS):
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
    logger.info(f"Currently {running_count} jobs running in group {group}")
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
    return wait_for_jobs_in_statuses(cook_url, [job_id], [status], max_wait_ms)[0]


def wait_for_jobs(cook_url, job_ids, status, max_wait_ms=DEFAULT_TIMEOUT_MS):
    return wait_for_jobs_in_statuses(cook_url, job_ids, [status], max_wait_ms)


def wait_for_job_in_statuses(cook_url, job_id, statuses, max_wait_ms=DEFAULT_TIMEOUT_MS):
    """Wait for the given job's status to change to one of the specified statuses."""
    return wait_for_jobs_in_statuses(cook_url, [job_id], statuses, max_wait_ms)[0]


def wait_for_jobs_in_statuses(cook_url, job_ids, statuses, max_wait_ms=DEFAULT_TIMEOUT_MS):
    """Wait for the given job's status to change to one of the specified statuses."""

    def query():
        return query_jobs(cook_url, True, uuid=job_ids)

    def predicate(resp):
        jobs = resp.json()
        for job in jobs:
            logger.info(f"Job {job['uuid']} has status {job['status']}, expecting {statuses}.")
        return all([job['status'] in statuses for job in jobs])

    response = wait_until(query, predicate, max_wait_ms=max_wait_ms, wait_interval_ms=DEFAULT_WAIT_INTERVAL_MS * 2)
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
        return query_jobs(cook_url, True, uuid=[job_id]).json()[0]

    def predicate(job):
        if not job['instances']:
            logger.info(f"Job {job_id} has no instances.")
        else:
            for inst in job['instances']:
                if 'exit_code' not in inst:
                    logger.info(f"Job {job_id} instance {inst['task_id']} has no exit code.")
                else:
                    logger.info(f"Job {job_id} instance {inst['task_id']} has exit code {inst['exit_code']}.")
                    job['instance-with-exit-code'] = inst
                    return True

    job = wait_until(query, predicate, max_wait_ms=max_wait_ms)
    return job['instance-with-exit-code']


def wait_for_output_url(cook_url, job_id, status=None):
    """
    Wait for the given job's output_url field to appear.
    Returns an up-to-date job description object on success,
    and raises an exception if the max_wait_ms wait time is exceeded.
    """
    job_id = unpack_uuid(job_id)

    def query():
        response = query_jobs(cook_url, True, uuid=[job_id])
        return response.json()[0]

    def predicate(job):
        if not job['instances']:
            logger.info(f"Job {job_id} has no instances.")
        else:
            for inst in job['instances']:
                if 'output_url' not in inst:
                    logger.info(f"Job {job_id} instance {inst['task_id']} has no output url.")
                elif status is not None and inst['status'] != status:
                    logger.info(f"Job {job_id} instance {inst['task_id']} has status {inst['status']}")
                else:
                    logger.info(
                        f"Job {job_id} instance {inst['task_id']} has output url {inst['output_url']}.")
                    return True

    job = wait_until(query, predicate)
    for inst in job['instances']:
        if 'output_url' in inst and (status is None or inst['status'] == status):
            return inst


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


def wait_for_running_instance(cook_url, job_id, max_wait_ms=DEFAULT_TIMEOUT_MS):
    """Waits for the job with the given job_id to have a running instance"""
    job_id = unpack_uuid(job_id)

    def query():
        return query_jobs(cook_url, True, uuid=[job_id])

    def predicate(resp):
        job = resp.json()[0]
        if not job['instances']:
            logger.info(f"Job {job_id} has no instances.")
            jobs, _ = unscheduled_jobs(cook_url, job_id)
            logger.info(f'Unscheduled info: {jobs}')
        else:
            for inst in job['instances']:
                status = inst['status']
                logger.info(f"Job {job_id} instance {inst['task_id']} has status {status}, expected running.")
                if status == 'running':
                    return True
            logger.info(f"Job {job_id} has no running instances.")
            return False

    response = wait_until(query, predicate, max_wait_ms=max_wait_ms)
    return next(i for i in response.json()[0]['instances'] if i['status'] == 'running')


def get_mesos_slaves(mesos_url):
    """
    Queries mesos slave state from /slaves
    """
    return session.get('%s/slaves' % mesos_url).json()


def list_jobs(cook_url, **kwargs):
    """Makes a request to the /list endpoint using the provided kwargs as the query params"""
    if 'start_ms' in kwargs:
        kwargs['start-ms'] = kwargs.pop('start_ms')
    if 'end_ms' in kwargs:
        kwargs['end-ms'] = kwargs.pop('end_ms')
    query_params = urlencode(kwargs)
    resp = session.get('%s/list?%s' % (cook_url, query_params))
    return resp


def jobs(cook_url, headers=None, **kwargs):
    """Makes a request to the /jobs endpoint using the provided kwargs as the query params"""
    if headers is None:
        headers = {}
    query_params = urlencode(kwargs, doseq=True)
    resp = session.get('%s/jobs?%s' % (cook_url, query_params), headers=headers)
    return resp


def contains_job_uuid(jobs, job_uuid):
    """Returns true if jobs contains a job with the given uuid"""
    return any(job for job in jobs if job['uuid'] == job_uuid)


def get_executor(agent_state, executor_id, include_completed=False):
    """Returns the executor with id executor_id from agent_state"""
    for framework in agent_state['frameworks']:
        for executor in framework['executors']:
            if executor['id'] == executor_id:
                return executor
    if include_completed:
        for framework in agent_state['frameworks'] + agent_state['completed_frameworks']:
            for executor in framework['completed_executors']:
                if executor['id'] == executor_id:
                    return executor
    return None


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


def wait_for_instance(cook_url, job_uuid, max_wait_ms=DEFAULT_TIMEOUT_MS, wait_interval_ms=1000, status=None):
    """Waits for the job with the given job_uuid to have at least one instance, and returns the first instance uuid"""

    def instances_with_status(job):
        logger.debug(f'Job {job_uuid}: {json.dumps(job, indent=2)}')
        if status is None:
            return job['instances']
        else:
            return [i for i in job['instances'] if i['status'] == status]

    job = wait_until(lambda: load_job(cook_url, job_uuid), lambda j: len(instances_with_status(j)) >= 1,
                     max_wait_ms=max_wait_ms, wait_interval_ms=wait_interval_ms)
    instance = instances_with_status(job)[0]
    instance['parent'] = job
    return instance


def sleep_for_publish_interval(cook_url):
    # allow enough time for progress and sandbox updates to be submitted
    cook_settings = settings(cook_url)
    progress_publish_interval_ms = get_in(cook_settings, 'progress', 'publish-interval-ms')
    wait_publish_interval_ms = min(3 * progress_publish_interval_ms, 20000)
    time.sleep(wait_publish_interval_ms / 1000.0)


def progress_line(cook_url, percent, message, write_to_file=False):
    """Simple text replacement of regex string using the default progress patterns."""
    cook_settings = settings(cook_url)
    regex_string = get_in(cook_settings, 'executor', 'default-progress-regex-string')
    percent_pattern = r'([0-9]*\.?[0-9]+)'
    message_pattern = r'($|\s+.*)'

    if not regex_string:
        regex_string = f'progress:\\s+{percent_pattern}{message_pattern}'

    assert percent_pattern in regex_string
    assert message_pattern in regex_string

    progress_string = (regex_string
                       .replace(percent_pattern, f'{percent}')
                       .replace(message_pattern, f' {message}')
                       .replace('\\s+', ' ')
                       .replace('\\', ''))

    if write_to_file:
        progress_env = retrieve_progress_file_env(cook_url)
        return f'echo "{progress_string}" >> ${{{progress_env}}}'
    else:
        return progress_string


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
    job_spec = {'group': group_uuid, 'command': f'sleep 600'}
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
        jobs = query_jobs(cook_url, assert_response=True, uuid=jobs).json()
        for job in jobs:
            logger.info(f'Job details: {json.dumps(job, sort_keys=True)}')
        # retry all jobs in the group
        retry_jobs(cook_url, retries=2, groups=[group_uuid], failed_only=retry_failed_jobs_only)
        # wait for some job to start
        wait_until(group_query, group_some_job_started)
        # return final job details to caller for assertion checks
        jobs = query_jobs(cook_url, assert_response=True, uuid=jobs).json()
        for job in jobs:
            for instance in job['instances']:
                mesos.dump_sandbox_files(session, instance, job)
        return jobs
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
        if not_all_waiting and sum(status_counts) == job_count:
            return True
        else:
            logger.debug(f'Group details: {group}')
            jobs = query_jobs(cook_url, assert_response=True, uuid=group['jobs']).json()
            for job in jobs:
                logger.debug(f'Job details: {json.dumps(job, sort_keys=True)}')
            return False

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


def user_current_usage(cook_url, headers=None, **kwargs):
    """
    Queries cook for a user's current resource usage
    based on their currently running jobs.
    """
    if headers is None:
        headers = {}
    return session.get('%s/usage' % cook_url, params=kwargs, headers=headers)


def query_queue(cook_url, **kwargs):
    """Get current jobs via the queue endpoint (admin-only)"""
    return session.get(f'{cook_url}/queue', **kwargs)


def get_limit(cook_url, limit_type, user, pool=None, headers=None):
    if headers is None:
        headers = {}
    params = {'user': user}
    if pool is not None:
        params['pool'] = pool
    return session.get(f'{cook_url}/{limit_type}', params=params, headers=headers)


def set_limit(cook_url, limit_type, user, mem=None, cpus=None, gpus=None, count=None, reason='testing', pool=None,
              headers=None):
    """
    Set resource limits for the given user.
    The limit_type parameter should be either 'share' or 'quota', specifying which type of limit is being set.
    Any subset of the mem, cpus, gpus and count (job-count) limits can be specified.
    """
    if headers is None:
        headers = {}
    if pool is None and not 'x-cook-pool' in headers:
        pool = default_submit_pool()
    limits = {}
    body = {'user': user, limit_type: limits}
    if reason is not None:
        body['reason'] = reason
    if mem is not None:
        limits['mem'] = mem
    if cpus is not None:
        limits['cpus'] = cpus
    if gpus is not None:
        limits['gpus'] = gpus
    if count is not None:
        limits['count'] = count
    if pool is not None:
        body['pool'] = pool
    logger.debug(f'Setting {user} {limit_type} to {limits}: {body}')
    return session.post(f'{cook_url}/{limit_type}', json=body, headers=headers)


def set_limit_to_default(cook_url, limit_type, user, pool_name):
    limit = get_limit(cook_url, limit_type, 'default', pool_name).json()
    logger.debug(f'Default {limit_type} in {pool_name}: {limit}')
    resp = set_limit(cook_url, limit_type, user, mem=limit['mem'],
                     cpus=limit['cpus'], gpus=limit['gpus'], pool=pool_name)
    assert 201 == resp.status_code, f'Expected 201, got {resp.status_code} with body {resp.text}'


def reset_limit(cook_url, limit_type, user, reason='testing', pool=None, headers=None):
    """
    Resets resource limits for the given user to the default for the cluster.
    The limit_type parameter should be either 'share' or 'quota', specifying which type of limit is being reset.
    """
    if headers is None:
        headers = {}
    if pool is None and not 'x-cook-pool' in headers:
        pool = default_submit_pool()
    params = {'user': user}
    if reason is not None:
        params['reason'] = reason
    if pool is not None:
        params['pool'] = pool
    return session.delete(f'{cook_url}/{limit_type}', params=params, headers=headers)


def retrieve_progress_file_env(cook_url):
    """Retrieves the environment variable used by the cook executor to lookup the progress file."""
    cook_settings = settings(cook_url)
    default_env_value = 'EXECUTOR_PROGRESS_OUTPUT_FILE'
    if using_kubernetes():
        return default_env_value
    else:
        env_value = get_in(cook_settings, 'executor', 'environment', 'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV')
        return env_value or default_env_value


def get_instance_stats(cook_url, **kwargs):
    """Gets instance stats using the provided kwargs as query params"""
    resp = session.get(f'{cook_url}/stats/instances', params=kwargs)
    return resp.json(), resp


def to_iso(time_millis):
    """Converts the given time since epoch in millis to an ISO 8601 string"""
    return datetime.utcfromtimestamp(time_millis / 1000).isoformat()


def percentile(a, q):
    """Returns the qth percentile of a"""
    return numpy.percentile(a, q, interpolation='higher')


def default_pool(cook_url):
    """Returns the configured default pool, or None if one is not configured"""
    cook_settings = settings(cook_url)
    default_pool = get_in(cook_settings, 'pools', 'default')
    return default_pool if default_pool != '' else None


def all_pools(cook_url):
    """Returns the list of all pools that exist"""
    resp = session.get(f'{cook_url}/pools')
    disallow_pools_regex = os.getenv('COOK_TEST_DISALLOW_POOLS_REGEX')
    all_pools = resp.json() if disallow_pools_regex is None else \
        [p for p in resp.json() if not re.match(disallow_pools_regex, p['name'])]
    return all_pools, resp


def active_pools(cook_url):
    """Returns the list of all active pools that exist"""
    pools, resp = all_pools(cook_url)
    all_active_pools = [p for p in pools if p['state'] == 'active']
    return all_active_pools, resp


def has_ephemeral_hosts():
    """Returns True if the cluster under test has ephemeral hosts"""
    s = os.getenv('COOK_TEST_EPHEMERAL_HOSTS')
    if s is not None:
        return to_bool(s)
    else:
        # Default to assuming that the cluster
        # under test does not have ephemeral hosts
        return False


@functools.lru_cache()
def _cook_executor_config():
    """Get the cook executor config from the /settings endpoint"""
    cook_url = retrieve_cook_url()
    _wait_for_cook(cook_url)
    init_cook_session(cook_url)
    cook_executor_config = get_in(settings(cook_url), 'executor')
    logger.info(f"Cook's executor config is {cook_executor_config}")
    return cook_executor_config


@functools.lru_cache()
def is_cook_executor_in_use():
    """Returns true if the cook executor is configured"""
    is_cook_executor_configured = is_not_blank(get_in(_cook_executor_config(), 'command'))
    if is_cook_executor_configured:
        if http_basic_auth_enabled():
            logger.info('Using mesos executor (cook executor is configured, but so is HTTP basic auth, and they are '
                        'incompatible)')
            return False
        elif using_kubernetes():
            logger.info('Not using cook executor (it is configured, but we are using k8s)')
            return False
        else:
            logger.info('Using cook executor as configured')
            return True
    else:
        logger.info('Using mesos executor (cook executor is not configured)')
        return False


@functools.lru_cache()
def is_job_progress_supported():
    """Returns true if the current job execution environment supports progress reporting"""
    # Mesos supports progress reporting only with Cook Executor,
    # but our progress reporter sidecar is always enabled on Kubernetes.
    # TODO: Update predicate below once progress sidecar is enabled in k8s (#1404)
    return is_cook_executor_in_use() # or using_kubernetes()


def slave_cpus(mesos_url, hostname):
    """Returns the cpus of the specified Mesos agent"""
    slaves = get_mesos_slaves(mesos_url)['slaves']
    # Here we need to use unreserved_resources because Mesos might only
    # send offers for the unreserved (role = "*") portions of the agents.
    slave_cpus = next(s['unreserved_resources']['cpus'] for s in slaves if s['hostname'] == hostname)
    return slave_cpus


def _pool_attribute_name(cook_url):
    pool_selection_plugin_config = settings(cook_url).get('plugins', {}).get('pool-selection', {})
    return pool_selection_plugin_config.get('attribute-name', None) or 'cook-pool'


def slave_pool(cook_url, mesos_url, hostname):
    """Returns the pool of the specified Mesos agent, or None if the agent doesn't have the attribute"""
    slaves = get_mesos_slaves(mesos_url)['slaves']
    attribute_name = _pool_attribute_name(cook_url)
    pool = next(s.get('attributes', {}).get(attribute_name, None) for s in slaves if s['hostname'] == hostname)
    return pool


def max_mesos_slave_cpus(mesos_url):
    """Returns the max cpus of all current Mesos agents"""
    slaves = get_mesos_slaves(mesos_url)['slaves']
    max_slave_cpus = max([s['resources']['cpus'] for s in slaves])
    return max_slave_cpus


@functools.lru_cache()
def get_kubernetes_compute_clusters():
    cook_url = retrieve_cook_url()
    _wait_for_cook(cook_url)
    init_cook_session(cook_url)
    compute_clusters = settings(cook_url)['compute-clusters']
    kubernetes_compute_clusters = [cc for cc in compute_clusters
                                   if cc['factory-fn'] == 'cook.kubernetes.compute-cluster/factory-fn']
    return kubernetes_compute_clusters


@functools.lru_cache()
def get_kubernetes_compute_cluster():
    kubernetes_compute_clusters = get_kubernetes_compute_clusters()
    if len(kubernetes_compute_clusters) > 0:
        return kubernetes_compute_clusters[0]
    else:
        return None


@functools.lru_cache()
def get_kubernetes_nodes():
    kubernetes_compute_cluster = get_kubernetes_compute_cluster()
    if 'config-file' in kubernetes_compute_cluster['config']:
        nodes = subprocess.check_output(['kubectl', '--kubeconfig', kubernetes_compute_cluster['config']['config-file'],
                                         'get', 'nodes', '-o=json'])
        node_json = json.loads(nodes)
    elif 'base-path' in kubernetes_compute_cluster['config']:
        authorization_header = subprocess.check_output(os.getenv('COOK_KUBERNETES_AUTH_CMD'), shell=True).decode(
            'utf-8').strip()
        nodes_url = kubernetes_compute_cluster['config']['base-path'] + '/api/v1/nodes'
        node_json = requests.get(nodes_url, headers={'Authorization': authorization_header}, verify=False).json()
    else:
        raise RuntimeError(f'Unable to get node info for configured kubernetes cluster: {kubernetes_compute_cluster}')
    logging.info(f'Retrieved kubernetes nodes: {node_json}')
    return node_json['items']


@functools.lru_cache()
def kubernetes_node_pool(nodename):
    node = [node for node in get_kubernetes_nodes() if node['metadata']['name'] == nodename]
    poolname_taint = [taint for taint in node[0]['spec']['taints'] if taint['key'] == 'cook-pool']
    if len(poolname_taint) == 0:
        return None
    poolname = poolname_taint[0].get('value', None)
    logging.info(f"K8S pool for {nodename} is {poolname}")
    return poolname


@functools.lru_cache()
def mesos_node_pool(nodename):
    cook_url = retrieve_cook_url()
    mesos_url = retrieve_mesos_url()
    node_pool = slave_pool(cook_url, mesos_url, nodename)
    return node_pool


def node_pool(nodename):
    """Get the pool for a node."""
    if using_mesos():
        return mesos_node_pool(nodename)
    elif using_kubernetes():
        return kubernetes_node_pool(nodename)
    else:
        raise RuntimeError(f'Unable to determine node pool for {nodename}')


def max_kubernetes_node_cpus():
    nodes = get_kubernetes_nodes()
    return max([float(n['status']['capacity']['cpu'])
                for n in nodes])


def get_compute_cluster_type(compute_cluster_dictionary):
    if compute_cluster_dictionary is None:
        raise Exception("compute-cluster-dictionary is None. Cannot determine the type")
    elif compute_cluster_dictionary['factory-fn'] == 'cook.mesos.mesos-compute-cluster/factory-fn':
        return 'mesos'
    elif compute_cluster_dictionary['factory-fn'] == 'cook.kubernetes.compute-cluster/factory-fn':
        return 'kubernetes'
    else:
        raise Exception(
            "compute-cluster-dictionary is " + repr(compute_cluster_dictionary) + " Cannot determine the type")


def task_constraint_cpus(cook_url):
    """Returns the max cpus that can be submitted to the cluster"""
    task_constraint_cpus = settings(cook_url)['task-constraints']['cpus']
    return task_constraint_cpus


def max_node_cpus():
    if using_mesos():
        return max_mesos_slave_cpus(retrieve_mesos_url())
    elif using_kubernetes():
        return max_kubernetes_node_cpus()
    else:
        raise RuntimeError('Unable to determine cluster max CPUs')


def node_count():
    if using_mesos():
        return len(get_mesos_slaves(retrieve_mesos_url())['slaves'])
    elif using_kubernetes():
        return len(get_kubernetes_nodes())
    else:
        raise RuntimeError('Unable to determine number of nodes')


def max_cpus():
    """Returns the maximum cpus we can submit that actually fits on a slave"""
    cook_url = retrieve_cook_url()
    slave_cpus = max_node_cpus()
    constraint_cpus = task_constraint_cpus(cook_url)
    max_cpus = min(slave_cpus, constraint_cpus)
    logging.debug(f'Max cpus we can submit that will get scheduled is {max_cpus}')
    return max_cpus


class CookTest(unittest.TestCase):
    def current_name(self):
        """Returns the name of the currently running test function"""
        test_id = self.id()
        return test_id.split('.')[-1]


def docker_tests_enabled():
    return docker_image() is not None


@functools.lru_cache()
def is_preemption_enabled():
    """Returns true if task preemption is enabled on the cluster"""
    max_preemption = rebalancer_settings().get('max-preemption')
    return max_preemption is not None


@functools.lru_cache()
def rebalancer_settings():
    cook_url = retrieve_cook_url()
    init_cook_session(cook_url)
    _wait_for_cook(cook_url)
    return settings(cook_url)['rebalancer']


def current_milli_time():
    """Returns the current epoch time in milliseconds"""
    return int(round(time.time() * 1000))


@functools.lru_cache()
def are_pools_enabled():
    """Returns true if there are at least 2 active pools on the cluster"""
    cook_url = retrieve_cook_url()
    init_cook_session(cook_url)
    _wait_for_cook(cook_url)
    return len(active_pools(cook_url)[0]) > 1


def mesos_hostnames_to_consider(cook_url, mesos_url):
    """
    Returns the hostnames in the default pool, or all hosts if the cluster is not using pools
    """
    slaves = get_mesos_slaves(mesos_url)['slaves']
    pool = default_pool(cook_url)
    attribute_name = _pool_attribute_name(cook_url)
    slaves = [s for s in slaves if s['attributes'].get(attribute_name, None) == pool] if pool else slaves
    num_to_log = min(len(slaves), 10)
    logging.info(f'First {num_to_log} hosts to consider: {json.dumps(slaves[:num_to_log], indent=2)}')
    return [s['hostname'] for s in slaves]


def kubernetes_hostnames_to_consider():
    # TODO: This should check a 'cook-pool' attribute on the node for the pool.
    # Currently, pools are not supported on kubernetes, so it's fine to ignore for now.
    # Once support is added, we should fix this function.
    nodes = get_kubernetes_nodes()
    hostnames = []
    for node in nodes:
        for address in node['status']['addresses']:
            if address['type'] == 'Hostname':
                hostnames.append(address['address'])
    return hostnames


def hostnames_to_consider(cook_url):
    if using_mesos():
        return mesos_hostnames_to_consider(cook_url, retrieve_mesos_url())
    elif using_kubernetes():
        return kubernetes_hostnames_to_consider()
    else:
        return 'Unable to retrieve hostnames to consider'


def num_hosts_to_consider(cook_url):
    """
    Returns the number of hosts in the default pool, or the
    total number of hosts if the cluster is not using pools
    """
    num_hosts = len(hostnames_to_consider(cook_url))
    logging.info(f'There are {num_hosts} hosts to consider')
    return num_hosts


def should_expect_sandbox_directory(instance):
    """
    Returns true if we should expect the sandbox directory
    to get populated for the provided instance
    """
    # TODO: remove when kubernetes sandbox support is added
    expect_sandbox = not using_kubernetes() and (not has_ephemeral_hosts() or instance['executor'] == 'cook')
    if expect_sandbox:
        logging.info('The sandbox directory is expected to get populated')
    else:
        logging.info('The sandbox directory is not expected to get populated')
    return expect_sandbox


def should_expect_sandbox_directory_for_job(job):
    """
    Returns true if we should expect the sandbox directory
    to get populated for the provided job
    """
    instance = job['instances'][0]
    return should_expect_sandbox_directory(instance)


def data_local_service_is_set():
    return os.getenv('DATA_LOCAL_SERVICE', None) is not None


def demo_plugins_are_configured(cook_url):
    settings_dict = settings(cook_url)
    # Because we always create plugin configuration in config.clj, the first keys always exist.
    # The actual factory-fn keys are not set unless the user specifies them.
    submission_fns = [
        "cook.plugins.demo-plugin/submission-factory", "cook.plugins.demo-plugin/submission-factory2"]
    if settings_dict['plugins']['job-submission-validator'].get('factory-fns') != submission_fns:
        return False
    if settings_dict['plugins']['job-launch-filter'].get('factory-fn') != "cook.plugins.demo-plugin/launch-factory":
        return False
    return True


@functools.lru_cache()
def pool_mover_plugin_configured():
    cook_url = retrieve_cook_url()
    settings_dict = settings(cook_url)
    plugins = settings_dict['plugins']
    configured = plugins.get('job-adjuster', {}).get('factory-fn') == \
                 'cook.plugins.pool-mover/make-pool-mover-job-adjuster'
    return configured


@functools.lru_cache()
def _fenzo_fitness_calculator():
    """Get the cook executor config from the /settings endpoint"""
    cook_url = retrieve_cook_url()
    _wait_for_cook(cook_url)
    init_cook_session(cook_url)
    fitness_calculator = get_in(settings(cook_url), 'fenzo-fitness-calculator')
    logger.info(f"Cook's fitness calculator is {fitness_calculator}")
    return fitness_calculator


def using_data_local_fitness_calculator():
    return _fenzo_fitness_calculator() == 'cook.scheduler.data-locality/make-data-local-fitness-calculator'


def get_agent_endpoint(slaves, agent_hostname):
    agent = [agent for agent in slaves
             if agent['hostname'] == agent_hostname][0]
    if agent is None:
        logger.warning(f"Could not find agent for hostname {agent_hostname}")
        logger.warning(f"slaves: {slaves}")
        return None
    else:
        # Get the host and port of the agent API.
        # Example pid: "slave(1)@172.17.0.7:5051"
        agent_hostport = agent['pid'].split('@')[1]
        agent_port = agent_hostport.split(':')[1]
        return f'http://{agent_hostname}:{agent_port}/state.json'


@functools.lru_cache()
def _supported_isolators():
    mesos_url = retrieve_mesos_url()
    slaves = get_mesos_slaves(mesos_url)['slaves']
    slave_endpoint = get_agent_endpoint(slaves, slaves[0]['hostname'])
    slave_response = session.get(slave_endpoint)
    try:
        slave_state = slave_response.json()
        if 'flags' in slave_state:
            return set(slave_state['flags']['isolation'].split(','))
        else:
            logger.error(f'Unable to determine flags from slave state at {slave_endpoint}: {slave_state}')
            return []
    except Exception:
        logger.exception(f'Unable to parse response as json: {slave_response.text}')
    return []


def supports_mesos_containerizer_images():
    if using_kubernetes():
        return False
    isolators = _supported_isolators()
    return 'filesystem/linux' in isolators and 'docker/runtime' in isolators


@functools.lru_cache()
def _get_compute_cluster_factory_fn():
    cook_url = retrieve_cook_url()
    _wait_for_cook(cook_url)
    init_cook_session(cook_url)
    compute_clusters = settings(cook_url)['compute-clusters']
    return compute_clusters[0]['factory-fn']


def get_compute_cluster_test_mode():
    return os.getenv("COOK_TEST_COMPUTE_CLUSTER_TYPE", "mesos")


def using_kubernetes():
    return get_compute_cluster_test_mode() == "kubernetes"


def using_mesos():
    return get_compute_cluster_test_mode() == "mesos"


def has_one_agent():
    return node_count() == 1


def supports_exit_code():
    return using_kubernetes() or is_cook_executor_in_use()


def kill_running_and_waiting_jobs(cook_url, user):
    one_hour_in_millis = 60 * 60 * 1000
    start = current_milli_time() - (72 * one_hour_in_millis)
    end = current_milli_time() + one_hour_in_millis
    running = jobs(cook_url, user=user, state=['running', 'waiting'], start=start, end=end).json()
    logger.info(f'Currently running/waiting jobs: {json.dumps(running, indent=2)}')
    kill_jobs(cook_url, running)


def running_tasks(cook_url):
    return session.get(f'{cook_url}/running', params={'limit': 20}).json()


@functools.lru_cache()
def timeout_interval_minutes():
    cook_url = retrieve_cook_url()
    _wait_for_cook(cook_url)
    settings_timeout_interval_minutes = get_in(settings(cook_url), 'task-constraints',
                                               'timeout-interval-minutes')
    return settings_timeout_interval_minutes


def reset_share_and_quota(cook_url, user):
    pool = default_submit_pool()
    logger.info(f'Resetting share and quota for {user} in pool {pool}')
    set_limit_to_default(cook_url, 'share', user, pool)
    set_limit_to_default(cook_url, 'quota', user, pool)


@functools.lru_cache()
def rebalancer_interval_seconds():
    interval_seconds = rebalancer_settings().get('interval-seconds', 0)
    return interval_seconds


def send_progress_update(cook_url, instance, assert_response=True, allow_redirects=True,
                         sequence=None, percent=None, message=None):
    """Submit a job instance progress update via the rest api"""
    payload = {}
    if sequence is not None:
        payload['progress_sequence'] = sequence
    if percent is not None:
        payload['progress_percent'] = percent
    if message is not None:
        payload['progress_message'] = message
    # NOTE: using `requests` rather than `session` here because this endpoint should not require authentication
    response = requests.post(f'{cook_url}/progress/{instance}', allow_redirects=allow_redirects, json=payload)
    if assert_response:
        response_info = {'code': response.status_code, 'msg': response.content}
        assert response.status_code == 202, response_info
        response_json = response.json()
        assert response_json['instance'] == instance, response_info
        assert response_json['message'] == 'progress update accepted', response_info
    return response


def job_progress_is_present(job, progress):
    present = any(i['progress'] == progress for i in job['instances'])
    if not present:
        logger.info(f'Job does not yet have progress {progress}: {json.dumps(job, indent=2)}')
    return present


def wait_for_instance_with_progress(cook_url, job_uuid, progress_percent):
    job = wait_until(lambda: load_job(cook_url, job_uuid),
                     lambda j: job_progress_is_present(j, progress_percent))
    return next(i for i in job['instances'] if i['progress'] == progress_percent)


def make_failed_job(cook_url, **kwargs):
    def __make_failed_job():
        job_uuid, resp = submit_job(cook_url, **kwargs)
        assert resp.status_code == 201

        # Wait for the job to start running, and kill it
        # It's possible for our kill below to race with
        # e.g. an "Agent removed" failure, which is why
        # we check for a non-mea-culpa failure at the end
        wait_for_job(cook_url, job_uuid, 'running')
        kill_jobs(cook_url, [job_uuid])

        def instance_query():
            return query_jobs(cook_url, True, uuid=[job_uuid])

        # Wait for the job (and its instances) to die
        wait_until(instance_query, all_instances_killed)
        job = load_job(cook_url, job_uuid)
        assert 'failed' == job['state']

        # Ensure that there is a non-mea culpa failure
        non_mea_culpa_failure_exists = any(not i['reason_mea_culpa'] for i in job['instances'])
        assert non_mea_culpa_failure_exists
        return job

    return wait_until(__make_failed_job, lambda _: True)


@functools.lru_cache()
def kubernetes_settings():
    cook_url = retrieve_cook_url()
    return settings(cook_url)['kubernetes']
