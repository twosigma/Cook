# Copyright (c) Two Sigma Open Source, LLC
#
# Licensed under the Apache license, Version 2.0 (the "License");
# you may not use this file ecept in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import getpass
import json
import logging

import requests

from datetime import timedelta
from typing import Dict, Iterable, List, Optional, Union
from urllib.parse import urlencode, urlparse, urlunparse
from uuid import UUID

from . import util
from .containers import AbstractContainer
from .jobs import Application, Disk, Job

CLIENT_VERSION = '0.3.5'

_LOG = logging.getLogger(__name__)
_LOG.addHandler(logging.StreamHandler())
_LOG.setLevel(logging.DEBUG)

_COOK_IMPERSONATE_HEADER = 'X-Cook-Impersonate'

_CLIENT_APP = Application('cook-python-client', CLIENT_VERSION)

_DEFAULT_REQUEST_TIMEOUT_SECONDS = 60
_DEFAULT_JOB_ENDPOINT = '/jobs'
_DEFAULT_DELETE_ENDPOINT = '/rawscheduler'


class JobClient:
    """A client to some remote Cook instance.

    :param url: The base URL of the Cook instance. Includes hostname and
        port. If no scheme is provided, http will be assumed.
    :type url: str
    :param session: Requests Session object to use for making requests. If
        provided, then the client will make requests using this session.
        Otherwise, the top-level ``requests`` functions will be used.
    :type auth: requests.Session, optional
    :param kwargs: Kwargs to provide to the request functions. If a session
        was provided, then these options will take precedence over options
        specified there. For example, if ``auth`` is set in both ``session``
        and ``kwargs``, then the value from ``kwargs`` will be used. However,
        if ``auth`` is set in ``session`` but not ``kwargs``, then the
        ``session``'s auth will be used. Additionally, note that if no timeout
        is provided here, a default timeout of 60 seconds will be used instead.
    """
    __scheme: str
    __netloc: str

    __job_endpoint: str
    __delete_endpoint: str

    __session: Optional[requests.Session] = None
    __kwargs: dict

    def __init__(self, url: str, *,
                 session: Optional[requests.Session] = None,
                 **kwargs):
        # In addition to explicitly defaulting to the HTTP scheme, this also
        # helps to resolve ambiguity in urllib when handling 'localhost' (note
        # the netloc and path params):
        #
        # >>> urlparse('localhost:8080')
        # ParseResult(scheme='', netloc='', path='localhost:8080', params='',
        #             query='', fragment='')
        # >>> urlparse('http://localhost:8080')
        # ParseResult(scheme='http', netloc='localhost:8080', path='',
        #             params='', query='', fragment='')
        if not url.startswith(('http://', 'https://')):
            url = f'http://{url}'
        parsed = urlparse(url)
        self.__scheme = parsed.scheme
        self.__netloc = parsed.netloc
        self.__job_endpoint = _DEFAULT_JOB_ENDPOINT
        self.__delete_endpoint = _DEFAULT_DELETE_ENDPOINT
        self.__session = session
        self.__kwargs = kwargs or {}
        if 'timeout' not in self.__kwargs:
            self.__kwargs['timeout'] = _DEFAULT_REQUEST_TIMEOUT_SECONDS

    def submit(self, *,
               command: str,

               cpus: float = 1.0,
               mem: float = 128.0,
               max_retries: int = 1,

               uuid: Optional[Union[str, UUID]] = None,
               env: Optional[Dict[str, str]] = None,
               labels: Optional[Dict[str, str]] = None,
               max_runtime: timedelta = timedelta(days=1),
               name: str = f'{getpass.getuser()}-job',
               priority: Optional[int] = None,
               container: Optional[AbstractContainer] = None,
               application: Application = _CLIENT_APP,
               gpus: Optional[int] = None,
               disk: Optional[Disk] = None,
               disable_mea_culpa_retries: Optional[bool] = None,
               constraints: Optional[list] = None,

               pool: Optional[str] = None,

               **kwargs) -> UUID:
        """Submit a single job to Cook.

        If an error occurs when issuing the submit request to the remote Cook
        instance, an error message will be printed to the logger, and the
        ``raise_for_status`` method will be invoked on the response object.

        :param command: The command to run on Cook.
        :type command: str
        :param cpus: The number of CPUs to request from Cook. Defaults to 1.
        :type cpus: float, optional
        :param mem: The amount of memory, in MB, to request from Cook. Defaults
            to 128.
        :type mem: float, optional
        :param max_retries: The *total* number of times this job should be
            attempted before failing. Naming is to keep association with the
            REST API. Defaults to 1.
        :type max_retries: int, optional
        :param uuid: The UUID of the job to submit. If this value is not
            provided, then a random UUID will be generated.
        :type uuid: str or UUID, optional
        :param env: Environment variables to set within the job's context,
            defaults to None.
        :type env: Dict[str, str], optional
        :param labels: Labels to assign to the job, defaults to None.
        :type labels: Dict[str, str], optional
        :param max_runtime: The maximum time this job should be allowed to run,
            defaults to one day.
        :type max_runtime: timedelta, optional
        :param name: A name to assign to the job, defaults to ``$USER-job``.
        :type name: str, optional
        :param priority: A priority to assign to the job, defaults to None.
        :type priority: int, optional
        :param container: Which container to use for the job. Currently, only
            Docker containers are supported. Defaults to None.
        :type container: AbstractContainer, optional
        :param application: Application information to assign to the job,
            defaults to ``cook-python-client`` with the current client
            library version.
        :type application: Application, optional
        :param pool: Which pool the job should be submitted to, defaults to
            None.
        :type pool: str, optional
        :param gpus: Number of GPUs to request from Cook, if any. Defaults to
            None. If you wish to specify the GPU model, add the
            ``COOK_GPU_MODEL`` environment variable to the environment variable
            list.
        :type gpus: int, optional
        :param kwargs: Request kwargs. If kwargs were specified to the client
            on construction, then these will take precedence over those.
        :return: The UUID of the newly-created job.
        :rtype: UUID
        :param disk: Disk information to assign to the job, which includes request, limit, and type
        :type disk: Disk, optional
        :param disable_mea_culpa_retries: Flag to disable mea culpa retries.
            If true, mea culpa retries will count against the job’s retry count.
        :type disable_mea_culpa_retries: bool, optional
        """
        uuid = str(uuid or util.make_temporal_uuid())
        jobspec = {
            'command': command,
            'cpus': cpus,
            'mem': mem,
            'uuid': uuid,
            'max-retries': max_retries
        }
        if env is not None:
            jobspec['env'] = env
        if labels is not None:
            jobspec['labels'] = labels
        if max_runtime is not None:
            jobspec['max-runtime'] = max_runtime
        if name is not None:
            jobspec['name'] = name
        if priority is not None:
            jobspec['priority'] = priority
        if application is not None:
            jobspec['application'] = application
        if container is not None:
            jobspec['container'] = container
        if gpus is not None:
            jobspec['gpus'] = gpus
        if disk is not None:
            jobspec['disk'] = disk
        if disable_mea_culpa_retries is not None:
            jobspec['disable_mea_culpa_retries'] \
                = disable_mea_culpa_retries
        if constraints is not None:
            jobspec['constraints'] = constraints

        return self.submit_all([jobspec], pool=pool, **kwargs)[0]

    def submit_all(self, jobspecs: Iterable[dict], *,
                   pool: Optional[str] = None,
                   **kwargs) -> List[UUID]:
        """Submit several jobs to Cook.

        Each entry in the iterable of jobspecs should contain the parameters
        shown in :py:meth:`JobSpec.submit`, save for ``pool``, which is exposed
        as a parameter to this function. This means that all jobs submitted
        with this function will share a pool. This is a restriction of Cook's
        REST API for bulk submissions.

        :param jobspecs: Jobspecs to submit to Cook. For information on
            jobspec structure, see :py:meth:`JobSpec.submit`.
        :type jobspecs: iterable of dict
        :param pool: Which pool to submit the jobs to. Defaults to None.
        :type pool: str, optional
        :param kwargs: Request kwargs. If kwargs were specified to the client
            on construction, then these will take precedence over those.
        :return: The UUIDs of the created jobs.
        :rtype: List[UUID]
        """
        jobspecs = list(
            map(
                JobClient._convert_jobspec,
                map(
                    JobClient._apply_jobspec_defaults,
                    jobspecs
                )
            )
        )
        payload = {'jobs': jobspecs}

        if pool is not None:
            payload['pool'] = pool

        url = urlunparse((self.__scheme, self.__netloc, self.__job_endpoint,
                          '', '', ''))
        _LOG.debug(f"Sending POST to {url}")
        _LOG.debug("Payload:")
        _LOG.debug(json.dumps(payload, indent=4))

        kwargs = {**kwargs, **self.__kwargs}
        r = self.__session or requests

        resp = r.post(url, json=payload, **kwargs)
        if not resp.ok:
            _LOG.error(f"Could not submit job: {resp.status_code} {resp.text}")
            resp.raise_for_status()

        return [UUID(jobspec['uuid']) for jobspec in jobspecs]

    def query(self, uuid: Union[str, UUID], **kwargs) -> Job:
        """Query Cook for a job's status.

        If an error occurs when issuing the query request to the remote Cook
        instance, an error message will be printed to the logger, and the
        ``raise_for_status`` method will be invoked on the response object.

        :param uuid: The UUID to query.
        :type uuid: str or UUID
        :param kwargs: Request kwargs. If kwargs were specified to the client
            on construction, then these will take precedence over those.
        :return: A Job object containing the job's information.
        :rtype: Job
        """
        return self.query_all([uuid], **kwargs)[0]

    def query_all(self, uuids: Iterable[Union[str, UUID]],
                  **kwargs) -> List[Job]:
        """Query Cook for a job's status.

        If an error occurs when issuing the query request to the remote Cook
        instance, an error message will be printed to the logger, and the
        ``raise_for_status`` method will be invoked on the response object.

        :param uuids: The UUIDs to query.
        :type uuid: iterable of str or UUID
        :param kwargs: Request kwargs. If kwargs were specified to the client
            on construction, then these will take precedence over those.
        :return: A list of Job objects containing the jobs' information.
        :rtype: List[Job]
        """
        query = urlencode([('uuid', str(uuid)) for uuid in uuids])
        url = urlunparse((self.__scheme, self.__netloc, self.__job_endpoint,
                          '', query, ''))
        _LOG.debug(f'Sending GET to {url}')

        kwargs = {**self.__kwargs, **kwargs}
        r = self.__session or requests

        resp = r.get(url, **kwargs)
        if not resp.ok:
            _LOG.error(f"Could not query job: {resp.status_code} {resp.text}")
            resp.raise_for_status()
        return list(map(Job.from_dict, resp.json()))

    def kill(self, uuid: Union[str, UUID], **kwargs):
        """Stop a job on Cook.

        If an error occurs when issuing the delete request to the remote Cook
        instance, an error message will be printed to the logger, and the
        ``raise_for_status`` method will be invoked on the response object.

        :param uuid: The UUID of the job to kill.
        :type uuid: str or UUID
        :param kwargs: Request kwargs. If kwargs were specified to the client
            on construction, then these will take precedence over those.
        """
        self.kill_all([uuid], **kwargs)

    def kill_all(self, uuids: Iterable[Union[str, UUID]], **kwargs):
        """Stop several jobs on Cook.

        If an error occurs when issuing the delete request to the remote Cook
        instance, an error message will be printed to the logger, and the
        ``raise_for_status`` method will be invoked on the response object.

        :param uuids: The UUIDs of the job to kill.
        :type uuids: iterable of str or UUID
        :param kwargs: Request kwargs. If kwargs were specified to the client
            on construction, then these will take precedence over those.
        """
        query = urlencode([('job', str(uuid)) for uuid in uuids])
        url = urlunparse((self.__scheme, self.__netloc, self.__delete_endpoint,
                          '', query, ''))
        _LOG.debug(f'Sending DELETE to {url}')

        kwargs = {**self.__kwargs, **kwargs}
        r = self.__session or requests

        resp = r.delete(url, **kwargs)
        if not resp.ok:
            _LOG.error(f"Could not delete job: {resp.status_code} {resp.text}")
            resp.raise_for_status()

    def close(self):
        """Close this client.

        If this client was created with a requests Session, then the underlying
        Session object is closed. Otherwise, this function is a no-op.

        The client should not be used after this method is called. If you need
        to call this method manually, consider wrapping the client in a
        ``with`` block, like so:

        ::

            with JobClient('localhost:12321', requests.Session()) as client:
                client.submit('ls')
                # ... do more stuff with the client
        """
        if self.__session is not None:
            self.__session.close()

    @staticmethod
    def _apply_jobspec_defaults(jobspec: dict) -> dict:
        """Apply default values to a jobspec.

        This function will add default values to a jobspec if no value is
        provided. The provided jobspec will not be touched and a copy will be
        returned.
        """
        jobspec = copy.deepcopy(jobspec)
        if not util.is_field_set(jobspec, 'uuid'):
            jobspec['uuid'] = str(util.make_temporal_uuid())
        if not util.is_field_set(jobspec, 'cpus'):
            jobspec['cpus'] = 1.0
        if not util.is_field_set(jobspec, 'mem'):
            jobspec['mem'] = 128.0
        if not util.is_field_set(jobspec, 'max-retries'):
            jobspec['max-retries'] = 1
        if not util.is_field_set(jobspec, 'max-runtime'):
            jobspec['max-runtime'] = timedelta(days=1)
        if not util.is_field_set(jobspec, 'name'):
            jobspec['name'] = f'{getpass.getuser()}-job'
        if not util.is_field_set(jobspec, 'application'):
            jobspec['application'] = _CLIENT_APP
        return jobspec

    @staticmethod
    def _convert_jobspec(jobspec: dict) -> dict:
        """Convert a Python jobspec into a JSON jobspec.

        This function will convert the higher-level Python types used in job
        submissions into their JSON primitive counterparts (e.g., timedelta is
        converted into the number of milliseconds). Additionally, this function
        will also remove all keys with a value of ``None``.

        The provided jobspec will not be touched and a copy will be returned.
        """
        jobspec = copy.deepcopy(jobspec)
        if util.is_field_set(jobspec, 'max-runtime'):
            jobspec['max-runtime'] = (
                jobspec['max-runtime'].total_seconds() * 1000
            )
        if util.is_field_set(jobspec, 'application'):
            jobspec['application'] = jobspec['application'].to_dict()
        if util.is_field_set(jobspec, 'container'):
            jobspec['container'] = jobspec['container'].to_dict()
        if util.is_field_set(jobspec, 'disk') and isinstance(jobspec['disk'], Disk):
            jobspec['disk'] = jobspec['disk'].to_dict()
        return util.prune_nones(jobspec)

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
