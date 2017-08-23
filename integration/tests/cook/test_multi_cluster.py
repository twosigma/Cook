import os
import unittest

import logging

from tests.cook import util


@unittest.skipUnless(os.getenv('COOK_MULTI_CLUSTER') is not None,
                     'Requires setting the COOK_MULTI_CLUSTER environment variable')
class MultiClusterTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        self.cook_url_1 = util.retrieve_cook_url()
        self.cook_url_2 = util.retrieve_cook_url('COOK_SCHEDULER_URL_2', 'http://localhost:22321')
        self.logger = logging.getLogger(__name__)
        util.wait_for_cook(self.cook_url_1)
        util.wait_for_cook(self.cook_url_2)

#    Commented out due to flakiness, github issue #381
#    def test_basic_submit(self):
#        job_uuid_1, resp = util.submit_job(self.cook_url_1)
#        self.assertEqual(resp.status_code, 201)
#        job_uuid_2, resp = util.submit_job(self.cook_url_2)
#        self.assertEqual(resp.status_code, 201)
#        job = util.wait_for_job(self.cook_url_1, job_uuid_1, 'completed')
#        self.assertEqual('success', job['instances'][0]['status'])
#        job = util.wait_for_job(self.cook_url_2, job_uuid_2, 'completed')
#        self.assertEqual('success', job['instances'][0]['status'])

    def test_federated_fetch(self):
        # Submit to cluster #1
        job_uuid_1, resp = util.submit_job(self.cook_url_1)
        self.assertEqual(resp.status_code, 201)

        # Submit to cluster #2
        job_uuid_2, resp = util.submit_job(self.cook_url_2)
        self.assertEqual(resp.status_code, 201)

        # Ask for both jobs from cluster #1, expect to get the first
        resp = util.query_jobs(self.cook_url_1, job=[job_uuid_1, job_uuid_2], partial='true')
        self.assertEqual(200, resp.status_code, resp.json())
        self.assertEqual(1, len(resp.json()))
        self.assertEqual([job_uuid_1], [job['uuid'] for job in resp.json()])

        # Ask for both jobs from cluster #2, expect to get the second
        resp = util.query_jobs(self.cook_url_2, job=[job_uuid_1, job_uuid_2], partial='true')
        self.assertEqual(200, resp.status_code, resp.json())
        self.assertEqual(1, len(resp.json()))
        self.assertEqual([job_uuid_2], [job['uuid'] for job in resp.json()])
