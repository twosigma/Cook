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

from cookclient import JobClient
from cookclient.jobs import Status as JobStatus

from tests.cook import util


class ClientTest(util.CookTest):
    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retreive_cook_url()

    def setUp(self):
        self.client = JobClient(type(self).cook_url)

    def test_submit(self):
        uuid = self.client.submit(command='ls',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5)
        self.assertTrue(uuid is not None)

    def test_query(self):
        uuid = self.client.submit(command='ls',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5)
        job = self.client.query(uuid)
        self.assertEqual(job.command, 'ls')
        self.assertAlmostEqual(job.cpus, 0.5)
        self.assertAlmostEqual(job.mem, 1.0)
        self.assertEqual(job.max_retries, 5)

    def test_kill(self):
        uuid = self.client.submit(command=f'sleep {util.DEFAULT_TEST_TIMEOUT_SECS}',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5)
        job = self.client.query(uuid)
        # Ensure the job is either waiting or running
        self.assertNotEqual(job.status, JobStatus.COMPLETED)
        self.client.kill(uuid)
        job = self.client.query(uuid)
        self.assertEqual(job.status, JobStatus.COMPLETED)
