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

import getpass
import json
import logging

import requests

from datetime import timedelta
from typing import Dict, Optional, Union
from urllib.parse import urlencode, urlparse, urlunparse
from uuid import UUID

from . import util
from .containers import AbstractContainer
from .jobs import Application, Job

CLIENT_VERSION = '0.2.1'

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
        :param kwargs: Request kwargs. If kwargs were specified to the client
            on construction, then these will take precedence over those.
        :return: The UUID of the newly-created job.
        :rtype: UUID
        """
        uuid = str(uuid or util.make_temporal_uuid())
        payload = {
            'command': command,
            'cpus': cpus,
            'mem': mem,
            'uuid': uuid,
            'max-retries': max_retries
        }
        if env is not None:
            payload['env'] = env
        if labels is not None:
            payload['labels'] = labels
        if max_runtime is not None:
            payload['max-runtime'] = max_runtime.total_seconds() * 1000
        if name is not None:
            payload['name'] = name
        if priority is not None:
            payload['priority'] = priority
        if application is not None:
            payload['application'] = application.to_dict()
        if container is not None:
            payload['container'] = container.to_dict()
        payload = {'jobs': [payload]}

        # Pool requests are assigned to the group payload instead of each
        # individual job submission's payload.
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

        return UUID(uuid)

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
        uuid = str(uuid)
        query = urlencode([('uuid', uuid)])
        url = urlunparse((self.__scheme, self.__netloc, self.__job_endpoint,
                          '', query, ''))
        _LOG.debug(f'Sending GET to {url}')

        kwargs = {**self.__kwargs, **kwargs}
        r = self.__session or requests

        resp = r.get(url, **kwargs)
        if not resp.ok:
            _LOG.error(f"Could not query job: {resp.status_code} {resp.text}")
            resp.raise_for_status()
        return Job.from_dict(resp.json()[0])

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
        uuid = str(uuid)
        query = urlencode([('job', uuid)])
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

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
