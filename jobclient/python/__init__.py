import json
import logging

import requests

import util

from typing import Dict, Optional
from urllib.parse import urlencode, urlunparse
from uuid import UUID

from jobs import Application, Job

_LOG = logging.getLogger(__name__)
_LOG.addHandler(logging.StreamHandler())
_LOG.setLevel(logging.DEBUG)

_COOK_IMPERSONATE_HEADER = 'X-Cook-Impersonate'

_DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS = 10
_DEFAULT_BATCH_REQUEST_SIZE = 32
_DEFAULT_REQUEST_TIMEOUT_SECONDS = 60
_DEFAULT_SUBMIT_RETRY_INTERVAL_SECONDS = 10
_DEFAULT_DELETE_ENDPOINT = '/rawscheduler'


class InstanceDecorator:
    def decorate(self, inst_args: dict) -> dict:
        raise NotImplementedError("stub")


class JobClient:
    __netloc: str

    __job_endpoint: str
    __delete_endpoint: str
    __group_endpoint: str

    __status_update_interval: int = _DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS
    __submit_retry_interval: int = _DEFAULT_SUBMIT_RETRY_INTERVAL_SECONDS
    __batch_request_size: int = _DEFAULT_BATCH_REQUEST_SIZE
    __request_timeout_seconds: int = _DEFAULT_REQUEST_TIMEOUT_SECONDS

    def __init__(self, host: str, port: int, *,
                 job_endpoint: str,
                 delete_endpoint: str = _DEFAULT_DELETE_ENDPOINT):
        self.__netloc = f'{host}:{port}'
        self.__job_endpoint = job_endpoint
        self.__delete_endpoint = delete_endpoint

    def submit(self, *,
               command: str,
               cpus: float,
               mem: float,
               max_retries: int,

               uuid: Optional[UUID] = None,
               env: Optional[Dict[str, str]] = None,
               labels: Optional[Dict[str, str]] = None,
               max_runtime: Optional[int] = None,
               name: Optional[str] = None,
               priority: Optional[int] = None,
               application: Optional[Application] = None) -> UUID:
        """Submit a single job to Cook.

        :param command: The command to run on Cook.
        :type command: str
        :param cpus: The number of CPUs to request from Cook.
        :type cpus: float
        :param mem: The amount of memory, in GB, to request from Cook.
        :type mem: float
        :param max_retries: The number of times this job should be retried
            before failing.
        :type max_retries: int
        :param uuid: The UUID of the job to submit. If this value is not
            provided, then a random UUID will be generated.
        :type uuid: UUID, optional
        :param env: Environment variables to set within the job's context,
            defaults to None.
        :type env: Dict[str, str], optional
        :param labels: Labels to assign to the job, defaults to None.
        :type labels: Dict[str, str], optional
        :param max_runtime: The maximum number seconds this job should be
            allowed to run, defaults to None.
        :type max_runtime: int, optional
        :param name: A name to assign to the job, defaults to None.
        :type name: str, optional
        :param priority: A priority to assign to the job, defaults to None.
        :type priority: int, optional
        :param application: Application information to assign to the job,
            defaults to None.
        :type application: Application, optional
        :return: The UUID of the newly-created job.
        :rtype: UUID
        """
        uuid = str(uuid if uuid is not None else util.make_temporal_uuid())
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
            payload['max-runtime'] = max_runtime
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
        resp = requests.post(url, json=payload)
        if not resp.ok:
            _LOG.error(f"Could not submit job: {resp.status_code} {resp.text}")
            resp.raise_for_status()

        return payload['uuid']

    def query(self, uuid: UUID) -> Job:
        """Query Cook for a job's status.

        :param uuid: The UUID to query.
        :type uuid: UUID
        :return: A Job object containing the job's information.
        :rtype: Job
        """
        if self.__job_endpoint == '/jobs':
            param_name = 'uuid'
        else:
            param_name = 'job'
        query = urlencode([(param_name, str(uuid))])
        url = urlunparse(('http', self.__netloc, self.__job_endpoint, '',
                          query, ''))
        _LOG.debug(f'Sending GET to {url}')
        resp = requests.get(url)
        if not resp.ok:
            _LOG.error(f"Could not query job: {resp.status_code} {resp.text}")
            resp.raise_for_status()
        return Job.from_dict(resp.json()[0])

    def kill(self, uuid: UUID):
        """Stop a job on Cook.

        :param uuid: The UUID of the job to kill.
        :type uuid: UUID
        """
        query = urlencode([('job', str(uuid))])
        url = urlunparse(('http', self.__netloc, self.__delete_endpoint, '',
                          query, ''))
        _LOG.debug(f'Sending DELETE to {url}')
        resp = requests.delete(url)
        if not resp.ok:
            _LOG.error(f"Could not delete job: {resp.status_code} {resp.text}")
            resp.raise_for_status()
