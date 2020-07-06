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

import socket
import unittest

from cookclient import JobClient
from cookclient.containers import DockerContainer, DockerPortMapping
from cookclient.jobs import (
    State as JobState,
    Status as JobStatus
)

from tests.cook import util


class ClientTest(util.CookTest):
    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retrieve_cook_url()
        util.init_cook_session(cls.cook_url)

    def setUp(self):
        self.client = JobClient(type(self).cook_url, session=util.session)

    def test_submit(self):
        uuid = self.client.submit(command='ls',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5,
                                  pool=util.default_submit_pool())
        try:
            self.assertTrue(uuid is not None)
        finally:
            self.client.kill(uuid)

    def test_query(self):
        uuid = self.client.submit(command='ls',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5,
                                  pool=util.default_submit_pool())
        try:
            job = self.client.query(uuid)
            self.assertEqual(job.command, 'ls')
            self.assertAlmostEqual(job.cpus, 0.5)
            self.assertAlmostEqual(job.mem, 1.0)
            self.assertEqual(job.max_retries, 5)
        finally:
            self.client.kill(uuid)

    def test_kill(self):
        uuid = self.client.submit(command=f'sleep {util.DEFAULT_TEST_TIMEOUT_SECS}',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5,
                                  pool=util.default_submit_pool())
        killed = False
        try:
            job = self.client.query(uuid)
            # Ensure the job is either waiting or running
            self.assertNotEqual(job.status, JobStatus.COMPLETED)
            self.client.kill(uuid)
            killed = True
            job = self.client.query(uuid)
            self.assertEqual(job.status, JobStatus.COMPLETED)
        finally:
            if not killed:
                self.client.kill(uuid)

    @unittest.skipUnless(util.docker_tests_enabled(),
                         "Requires setting the COOK_TEST_DOCKER_IMAGE environment variable")
    def test_container_submit(self):
        container = DockerContainer(util.docker_image())
        self.assertIsNotNone(container.image)
        uuid = self.client.submit(command='ls', container=container,
                                  pool=util.default_submit_pool())
        try:
            job = self.client.query(uuid)

            remote_container = job.container
            self.assertEqual(remote_container.kind.lower(), 'docker')
            self.assertEqual(remote_container.image, container.image)
        finally:
            self.client.kill(uuid)

    @unittest.skipUnless(util.docker_tests_enabled(), "Requires setting the COOK_TEST_DOCKER_IMAGE environment variable")
    @unittest.skipUnless(util.using_kubernetes(), "Requires running on Kubernetes")
    @unittest.skipUnless(util.is_job_progress_supported(), "Requires progress reporting")
    def test_container_port_submit(self):
        """Test submitting a job with a port specification."""
        JOB_PORT = 30030
        progress_file_env = util.retrieve_progress_file_env(type(self).cook_url)
        hostname_progress_cmd = util.progress_line(type(self).cook_url,
                                                   50,  # Don't really care, we just need a val
                                                   '$(hostname -I)',
                                                   write_to_file=True)

        container = DockerContainer(util.docker_image(), port_mapping=[
            DockerPortMapping(host_port=0, container_port=JOB_PORT,
                              protocol='tcp')
        ])
        uuid = self.client.submit(command=f'{hostname_progress_cmd} && nc -l 0.0.0.0 {JOB_PORT}',
                                  container=container,
                                  env={progress_file_env: 'progress.txt'},
                                  pool=util.default_submit_pool())

        try:
            util.wait_for_instance_with_progress(type(self).cook_url, str(uuid), 50)
            job = self.client.query(uuid)
            addr = job.instances[0].progress_message

            self.assertIsNotNone(addr)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((addr, JOB_PORT))
                message = "hello world!"

                self.assertEqual(sock.send(message), len(message))
        finally:
            self.client.kill(uuid)

    def test_instance_query(self):
        """Test that parsing an instance yielded from Cook works."""
        uuid = self.client.submit(command=f'sleep {util.DEFAULT_TEST_TIMEOUT_SECS}',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5,
                                  pool=util.default_submit_pool())

        try:
            util.wait_for_instance(type(self).cook_url, uuid)

            job = self.client.query(uuid)

            self.assertNotEqual(job.instances, [])
            self.assertIsNotNone(job.instances[0])
        finally:
            self.client.kill(uuid)
