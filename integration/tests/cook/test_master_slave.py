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

    @unittest.skipIf(util.has_ephemeral_hosts(), 'If the cluster under test has ephemeral hosts, then it is generally '
                                                 'a bad idea to use HOSTNAME EQUALS constraints, because it can cause '
                                                 'the process responsible for launching hosts to launch hosts that '
                                                 'never get used')
    def test_get_queue(self):
        job_uuid, resp = util.submit_job(self.master_url, constraints=[["HOSTNAME",
                                                                        "EQUALS",
                                                                        "can't schedule"]])
        self.assertEqual(201, resp.status_code, resp.content)
        slave_queue = util.session.get('%s/queue' % self.slave_url, allow_redirects=False)
        self.assertEqual(307, slave_queue.status_code)

        @retry(stop_max_delay=30000, wait_fixed=1000)  # Need to wait for a rank cycle
        def check_queue():
            master_queue = util.session.get(slave_queue.headers['Location'])
            self.assertEqual(200, master_queue.status_code, master_queue.content)
            self.assertTrue(any([job['job/uuid'] == job_uuid for job in master_queue.json()['normal']]))

        check_queue()
        util.session.delete('%s/rawscheduler?job=%s' % (self.master_url, job_uuid))
