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

import unittest

from cookclient import JobClient
from cookclient.containers import DockerContainer
from cookclient.jobs import Job, Status as JobStatus

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
                                  max_retries=5)
        try:
            self.assertTrue(uuid is not None)
        finally:
            self.client.kill(uuid)

    def test_query(self):
        uuid = self.client.submit(command='ls',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5)
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
                                  max_retries=5)
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
        uuid = self.client.submit(command='ls', container=container)
        try:
            job = self.client.query(uuid)

            remote_container = job.container
            self.assertEqual(remote_container['type'].lower(), 'docker')
            self.assertEqual(remote_container['docker']['image'], container.image)
        finally:
            self.client.kill(uuid)

    def test_instance_query(self):
        """Test that parsing an instance yielded from Cook works."""
        uuid = self.client.submit(command=f'sleep {util.DEFAULT_TEST_TIMEOUT_SECS}',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5)

        try:
            util.wait_for_instance(type(self).cook_url, uuid)

            job = self.client.query(uuid)

            self.assertNotEqual(job.instances, [])
            self.assertIsNotNone(job.instances[0])
        finally:
            self.client.kill(uuid)
