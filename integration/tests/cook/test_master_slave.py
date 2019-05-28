import logging
import os
import unittest

import pytest
from retrying import retry

from tests.cook import util


@unittest.skipUnless(os.getenv('COOK_MASTER_SLAVE') is not None,
                     'Requires setting the COOK_MASTER_SLAVE environment variable')
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class MasterSlaveTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.master_url = util.retrieve_cook_url()
        cls.slave_url = util.retrieve_cook_url('COOK_SLAVE_URL', 'http://localhost:12322')
        cls.logger = logging.getLogger(__name__)
        util.init_cook_session(cls.master_url, cls.slave_url)

    def setUp(self):
        self.master_url = type(self).master_url
        self.slave_url = type(self).slave_url
        self.logger = logging.getLogger(__name__)

    def test_get_queue(self):
        bad_constraint = [["HOSTNAME",
                           "EQUALS",
                           "lol won't get scheduled"]]
        uuid, resp = util.submit_job(self.master_url, command='sleep 30', constraints=bad_constraint)
        self.assertEqual(201, resp.status_code, resp.content)
        try:
            slave_queue = util.session.get('%s/queue' % self.slave_url, allow_redirects=False)
            self.assertEqual(307, slave_queue.status_code)
            default_pool = util.default_pool(self.master_url)
            pool = default_pool or 'no-pool'
            self.logger.info(f'Checking the queue endpoint for pool {pool}')

            @retry(stop_max_delay=30000, wait_fixed=1000)  # Need to wait for a rank cycle
            def check_queue():
                master_queue = util.session.get(slave_queue.headers['Location'])
                self.assertEqual(200, master_queue.status_code, master_queue.content)
                pool_queue = master_queue.json()[pool]
                self.assertTrue(any([job['job/uuid'] == uuid for job in pool_queue]), pool_queue)

            check_queue()
        finally:
            util.kill_jobs(self.master_url, [uuid])
