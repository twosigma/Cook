"""Module containing Cook API client classes."""

import os
import uuid
import logging
import functools

from cook.util import merge_dicts, await_until, hashabledict

from bravado_core.formatter import SwaggerFormat
from bravado_core.exception import SwaggerValidationError

from bravado.client import SwaggerClient
from bravado.exception import HTTPError
from bravado.requests_client import RequestsClient

def basic_auth_http_client(**kwargs):
    """Builds an http client with basic auth."""
    http_client = RequestsClient()
    http_client.set_basic_auth(**kwargs)

    return http_client

def key_auth_http_client(**kwargs):
    """Builds an http client with key auth."""
    http_client = RequestClient()
    http_client.set_api_key(**kwargs)

    return http_client

def uuid_to_python(v):
    return uuid.UUID(v)

def uuid_to_wire(v):
    return str(v)

def validate_uuid(v):
    try:
        return str(uuid.UUID(v)) == v
    except:
        return False

UUID_SWAGGER_FORMAT=SwaggerFormat(
    format='uuid',
    description='format and validate uuid values',
    to_python=uuid_to_python,
    to_wire=uuid_to_wire,
    validate=validate_uuid
)

class CookSwaggerResource:
    """A wrapper around a swagger resource adding behavior to every request.

    *Not intended to be used directly.*

    """
    def __init__(self, resource, **kwargs):
        self.resource = resource
        self.options = kwargs

    def __getattr__(self, name):
        return functools.partial(self.__request, name)

    def __request(self, name, json=True, **kwargs):
        try:
            result, response = getattr(
                self.resource, name)(**kwargs).result(**self.options)

            return (True, response.json() if json else result)
        except HTTPError as e:
            return (False, e.response.json())

class CookSwaggerClient:
    """A wrapper around a swagger client adding behavior to every request.

    Creates a swagger client from either a URL or a spec. See the Bravado documentation
    for more information of the swagger specification.

    *Not intended to be used directly.*

    """
    def __init__(self, url, spec=None, http_client=None, **kwargs):
        config = {
            'use_models': False,
            'also_return_response': True,
            'formats': [UUID_SWAGGER_FORMAT]
        }

        if spec:
            self.client = SwaggerClient.from_spec(
                spec, http_client=http_client, config=config)
        else:
            self.client = SwaggerClient.from_url(
                url, http_client=http_client, config=config)

        self.options = kwargs

    def __getattr__(self, name):
        return CookSwaggerResource(getattr(self.client, name), **self.options)

class CookClient:
    """A low level client for the Cook scheduler API.

    Arguments to __init__ are passed directly to the CookSwaggerClient.

    All methods return `(okay, result)`, where `okay` is a boolean indicating whether
    the request succeeded or failed, and `result` is the result of the request.

    """

    def __init__(self, **kwargs):
        self.client = CookSwaggerClient(**kwargs)

    def create_jobs(self, jobs):
        """Create jobs from an array of job specs. The format of the job depends on Cook.

        If successful, the result will be a list of UUIDs.

        """
        okay, result = self.client.rawscheduler.post_rawscheduler(
            rawrequest={"jobs": jobs}
        )

        return [okay, result.get('jobs') if okay else result]

    def create_groups(self, groups):
        """Create groups from an array of group specs. The format of the group depends on Cook.

        If successful, the result will be a list of UUIDs.

        """
        okay, result = self.client.rawscheduler.post_rawscheduler(
            rawrequest={"jobs": [], "groups": groups}
        )

        return [okay, result.get('groups') if okay else result]

    def delete_jobs(self, uuids):
        """Delete jobs by UUID."""
        okay, result = self.client.rawscheduler.delete_rawscheduler(job=uuids, json=False)

        return (okay, 'true') if okay else (okay, result)

    def delete_instances(self, uuids):
        """Delete instances by UUID."""
        okay, result = self.client.rawscheduler.delete_rawscheduler(instance=uuids, json=False)

        return (okay, 'true') if okay else (okay, result)

    def poll_jobs(self, uuids):
        """Poll jobs by UUID, returning a list of jobs if successful."""
        return self.client.rawscheduler.get_rawscheduler(job=uuids)

    def poll_instances(self, uuids):
        """Poll instances by UUID, returning a list of instances if successful."""
        uuids = [str(u) for u in uuids]
        okay, result = self.client.rawscheduler.get_rawscheduler(instance=uuids)

        if okay:
            return (True, [instance for job in result for instance in job.get('instances')
                           if instance.get('task_id') in uuids])
        else:
            return (False, result)

    def poll_groups(self, uuids, detailed):
        """Poll groups by UUID, returning a list of groups if successful."""
        return self.client.group.get_group(uuid=uuids, detailed=detailed)

    def await_jobs(self, uuids, timeout=30, interval=5):
        """Wait for jobs to reach the 'completed' status, returning a list of jobs if successful."""
        def completed_jobs():
            okay, jobs = self.poll_jobs(uuids)

            if okay and not [j for j in jobs if j.get('status') != 'completed']:
                return jobs

        jobs = await_until(completed_jobs, timeout, interval)

        if jobs:
            return (True, jobs)
        else:
            return (False, '{"error": "timeout waiting for jobs}')

    def await_instances(self, uuids, timeout=30, interval=5):
        """Wait for instances to have an end_time, returning a list of instances if successful."""
        def completed_instances():
            okay, instances = self.poll_instances(uuids)

            if okay and not [i for i in instances if not i.get('end_time')]:
                return instances

        instances = await_until(completed_instances, timeout, interval)

        if instances:
            return (True, instances)
        else:
            return (False, '{"error": "timeout waiting for instances}')

    def retry_jobs(self, uuids, max_retries=1):
        """Update the retry count for jobs by UUID."""
        return self.client.retry.put_retry(retry={'jobs': uuids, 'retries': max_retries})

class CookMultiClient:
    """A wrapper around a CookClient in order to try hitting multiple clusters.

    With the exception of taking a list of client specs (instead of just one), it
    exposes the same API as the CookClient class.

    """
    def __init__(self, specs):
        self.specs = [hashabledict(s) for s in specs]

    def __getattr__(self, name):
        return functools.partial(self.request, name)

    @functools.lru_cache(maxsize=1000)
    def client(self, spec):
        """Returns a client for a spec, caching the result."""
        spec = dict(spec)
        cluster = spec.pop('name')

        try:
            return CookClient(**spec)
        except Exception:
            logging.exception('client creation failed for cluster "%s"' % cluster)

            return None

    def request(self, name, *args):
        """Tries client requests against all known clusters until one succeeds or all fail."""
        for s in self.specs:
            client = self.client(s)
            cluster = s.get('name')

            if client:
                try:
                    return getattr(client, name)(*args)
                except Exception:
                    logging.exception('request "%s" failed for cluster "%s"' % (name, cluster))

                    continue
            else:
                continue
        else:
            return (False, '{"error": "request failed on all clusters"}')

class Instance:
    """A sugary sweet class for high level interaction with Cook job instances.

    Accepts a CookClient and a UUID corresponding to an instance, and immediately
    looks up the instance attributes. Dynamic attributes are automatically refreshed
    when accessed.

    """
    def __init__(self, client, uuid):
        self.__uuid = uuid
        self.__client = client

        if self.__uuid:
            self.__refresh()
        else:
            raise ValueError('missing instance id')

    def __str__ (self):
        return 'Instance{0}'.format(self.__attrs)

    def __getattr__(self, name):
        if name in self.__dynamic_instance_attrs():
            self.__refresh()

        return self.__attrs.get(name)

    def __dynamic_instance_attrs(self):
        """Instance attributes requiring a refresh."""
        return ['status', 'preempted']

    def __refresh(self):
        """Refresh instance attributes."""
        okay, attrs = self.__client.poll_instances([self.__uuid])

        if okay:
            self.__attrs = attrs[0]
            return self
        else:
            raise ValueError('failed to refresh instance')

    def kill(self):
        """Kill the instance."""
        okay, _ = self.__client.delete_instances([self.__uuid])

        if okay:
            self.__refresh()
            return self
        else:
            raise ValueError('failed to kill instance')

    def wait(self):
        """Wait for the instance to have an end_time."""
        okay, attrs = self.__client.await_instances([self.__uuid])

        if okay:
            self.__attrs = attrs[0]
            return self
        else:
            raise ValueError('failed to wait for instance')

class Job:
    """A sugary sweet class for high level interaction with Cook jobs.

    Accepts a CookClient and a UUID corresponding to a job, and immediately
    looks up the job attributes. Dynamic attributes are automatically refreshed
    when accessed. Alternatively, this class can be used to create a job by
    providing job attributes and no UUID. A UUID will automatically be created
    and the job will be submmitted to the Cook API.

    """

    def __init__(self, client, **kwargs):
        self.__uuid = kwargs.get('uuid')
        self.__client = client

        if self.__uuid:
            self.__refresh()
        else:
            self.__uuid = uuid.uuid4()
            self.__attrs = merge_dicts(self.__default_job_attrs(), kwargs)
            self.__create()

    def __str__ (self):
        return 'Job{0}'.format(merge_dicts(self.__attrs))

    def __getattr__(self, name):
        if name in self.__dynamic_job_attrs():
            self.__refresh()

        attr = self.__attrs.get(name)

        if name == 'instances':
            return self.__instances()
        else:
            return attr

    def __instances(self):
        """Create real instance classes for instances associated with the job."""
        return [Instance(self.__client, uuid=i.get('task_id'))
                for i in self.__attrs.get('instances')]

    def __dynamic_job_attrs(self):
        """Job attributes requiring a refresh."""
        return ['state', 'status', 'instances', 'retries_remaining']

    def __default_job_attrs(self):
        """Default attribute values when creating a new job."""
        return {
            'uuid': self.__uuid,
            'name': 'job-' + str(self.__uuid),
            'cpus': 1,
            'mem': 512,
            'priority': 0,
            'max_retries': 1,
            'max_runtime': 200
        }

    def __refresh(self):
        okay, attrs = self.__client.poll_jobs([self.__uuid])

        if okay:
            self.__attrs = attrs[0]
            return self
        else:
            raise ValueError('failed to refresh job')

    def __create(self):
        okay, _ = self.__client.create_jobs([self.__attrs])

        if okay:
            self.__refresh()
            return self
        else:
            raise ValueError('failed to create job')

    def kill(self):
        """Kill the job."""
        okay, _ = self.__client.delete_jobs([self.__uuid])

        if okay:
            self.__refresh()
            return self
        else:
            raise ValueError('failed to kill job')

    def retry(self):
        """Retry the job."""
        okay, _ = self.__client.retry_jobs([self.__uuid])

        if okay:
            self.__refresh()
            return self
        else:
            raise ValueError('failed to retry job')

    def wait(self):
        """Wait for the job to have a 'completed' status."""
        okay, attrs = self.__client.await_jobs([self.__uuid])

        if okay:
            self.__attrs = attrs[0]
            return self
        else:
            raise ValueError('failed to wait for job')

class Group:
    """A sugary sweet class for high level interaction with Cook groups.

    Accepts a CookClient and a UUID corresponding to a group, and immediately
    looks up the group attributes. Dynamic attributes are automatically refreshed
    when accessed. Alternatively, this class can be used to create a group by
    providing group attributes and no UUID. A UUID will automatically be created
    and the group will be submmitted to the Cook API.

    """
    def __init__(self, client, **kwargs):
        self.__uuid = kwargs.get('uuid')
        self.__client = client

        if self.__uuid:
            self.__refresh()
        else:
            self.__uuid = uuid.uuid4()
            self.__attrs = merge_dicts(self.__default_group_attrs(), kwargs)
            self.__create()

    def __str__(self):
        return 'Group{0}'.format(merge_dicts(self.__attrs))

    def __getattr__(self, name):
        if name in self.__dynamic_group_attrs():
            self.__refresh()

        return self.__attrs.get(name)

    def __jobs(self):
        return [Job(self.__client, uuid=j) for j in self.__attrs.get('jobs')]

    def __dynamic_group_attrs(self):
        """Group attributes requiring a refresh."""
        return ['jobs', 'waiting', 'running', 'completed']

    def __default_group_attrs(self):
        """Default attribute values when creating a new group."""
        return {
            'uuid': self.__uuid,
            'name': 'group-' + str(self.__uuid)
        }

    def __refresh(self):
        print(self.__uuid)
        okay, attrs = self.__client.poll_groups([self.__uuid], True)

        if okay:
            self.__attrs = attrs[0]
            return self
        else:
            raise ValueError('failed to refresh group')

    def __create(self):
        okay, _ = self.__client.create_groups([self.__attrs])

        if okay:
            self.__refresh()
            return self
        else:
            raise ValueError('failed to create group')
