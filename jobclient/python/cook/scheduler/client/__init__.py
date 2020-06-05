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
from urllib.parse import urlencode, urlunparse
from uuid import UUID

from . import util
from .jobs import Application, Job

_LOG = logging.getLogger(__name__)
_LOG.addHandler(logging.StreamHandler())
_LOG.setLevel(logging.DEBUG)

_COOK_IMPERSONATE_HEADER = 'X-Cook-Impersonate'

_CLIENT_APP = Application('cook-python-client', '0.1.0')

_DEFAULT_REQUEST_TIMEOUT_SECONDS = 60
_DEFAULT_JOB_ENDPOINT = '/jobs'
_DEFAULT_DELETE_ENDPOINT = '/rawscheduler'


class InstanceDecorator:
    def decorate(self, inst_args: dict) -> dict:
        raise NotImplementedError("stub")


class JobClient:
    __netloc: str

    __job_endpoint: str
    __delete_endpoint: str
    __group_endpoint: str

    __request_timeout_seconds: int = _DEFAULT_REQUEST_TIMEOUT_SECONDS

    def __init__(self, url: str):
        """Initialize an instance of the Cook client.

        Parameters
        ----------
        :param url: The base URL of the Cook instance. Includes hostname and
            port.
        :type url: str
        """
        self.__netloc = url
        self.__job_endpoint = _DEFAULT_JOB_ENDPOINT
        self.__delete_endpoint = _DEFAULT_DELETE_ENDPOINT

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
               application: Application = _CLIENT_APP) -> UUID:
        """Submit a single job to Cook.

        Required Parameters
        -------------------
        :param command: The command to run on Cook.
        :type command: str

        Optional Parameters
        -------------------
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
        :type uuid: Union[str, UUID], optional
        :param env: Environment variables to set within the job's context,
            defaults to None.
        :type env: Dict[str, str], optional
        :param labels: Labels to assign to the job, defaults to None.
        :type labels: Dict[str, str], optional
        :param max_runtime: The maximum time this job should be allowed to run,
            defaults to one day.
        :type max_runtime: timedelta, optional
        :param name: A name to assign to the job, defaults to `$USER-job`.
        :type name: str, optional
        :param priority: A priority to assign to the job, defaults to None.
        :type priority: int, optional
        :param application: Application information to assign to the job,
            defaults to `cook-python-client` with version 0.1.
        :type application: Application, optional
        Output
        ------
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
            payload['max-runtime'] = max_runtime.total_seconds()
        if name is not None:
            payload['name'] = name
        if priority is not None:
            payload['priority'] = priority
        if application is not None:
            payload['application'] = application.to_dict()
        payload = {'jobs': [payload]}
        url = urlunparse(('http', self.__netloc, self.__job_endpoint, '', '', ''))  # noqa E501
        _LOG.debug(f"Sending POST to {url}")
        _LOG.debug("Payload:")
        _LOG.debug(json.dumps(payload, indent=4))
        resp = requests.post(url, json=payload,
                             timeout=self.__request_timeout_seconds)
        if not resp.ok:
            _LOG.error(f"Could not submit job: {resp.status_code} {resp.text}")
            resp.raise_for_status()

        return UUID(uuid)

    def query(self, uuid: Union[str, UUID]) -> Job:
        """Query Cook for a job's status.

        :param uuid: The UUID to query.
        :type uuid: Union[str, UUID]
        :return: A Job object containing the job's information.
        :rtype: Job
        """
        uuid = str(uuid)
        query = urlencode([('uuid', uuid)])
        url = urlunparse(('http', self.__netloc, self.__job_endpoint, '',
                          query, ''))
        _LOG.debug(f'Sending GET to {url}')
        resp = requests.get(url, timeout=self.__request_timeout_seconds)
        if not resp.ok:
            _LOG.error(f"Could not query job: {resp.status_code} {resp.text}")
            resp.raise_for_status()
        return Job.from_dict(resp.json()[0])

    def kill(self, uuid: Union[str, UUID]):
        """Stop a job on Cook.

        Exceptions
        ----------
        If an error occurs when issuing the delete request to the remote Cook
        instance, an error message will be printed to the logger, and the
        `raise_for_status` method will be invoked on the response object.

        Parameters
        ----------
        :param uuid: The UUID of the job to kill.
        :type uuid: Union[str, UUID]
        """
        uuid = str(uuid)
        query = urlencode([('job', uuid)])
        url = urlunparse(('http', self.__netloc, self.__delete_endpoint, '',
                          query, ''))
        _LOG.debug(f'Sending DELETE to {url}')
        resp = requests.delete(url, timeout=self.__request_timeout_seconds)
        if not resp.ok:
            _LOG.error(f"Could not delete job: {resp.status_code} {resp.text}")
            resp.raise_for_status()
