import os

from unittest import TestCase

from cook.scheduler.client import JobClient
from cook.scheduler.client.jobs import Status as JobStatus

CLIENT_HOST = os.environ.get('TEST_CLIENT_HOST', 'localhost:12321')


class ClientTest(TestCase):
    def setUp(self):
        self.client = JobClient(CLIENT_HOST)

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
        self.assertAlmostEqual(job.max_retries, 5)

    def test_kill(self):
        uuid = self.client.submit(command='ls',
                                  cpus=0.5,
                                  mem=1.0,
                                  max_retries=5)
        self.client.kill(uuid)
        job = self.client.query(uuid)
        self.assertEqual(job.status, JobStatus.COMPLETED)
