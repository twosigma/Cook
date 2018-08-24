import logging
import unittest

import pytest
from retrying import retry

from tests.cook import util, reasons


@pytest.mark.multi_user
@unittest.skipUnless(util.multi_user_tests_enabled(), 'Requires using multi-user coniguration '
                                                      '(e.g., BasicAuth) for Cook Scheduler')
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class PoolsCookTest(util.CookTest):

    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retrieve_cook_url()
        util.init_cook_session(cls.cook_url)

    def setUp(self):
        self.cook_url = type(self).cook_url
        self.mesos_url = util.retrieve_mesos_url()
        self.logger = logging.getLogger(__name__)
        self.user_factory = util.UserFactory(self)

    @unittest.skipUnless(util.are_pools_enabled(), 'Pools are not enabled on the cluster')
    def test_pool_scheduling(self):
        admin = self.user_factory.admin()
        user = self.user_factory.new_user()
        pools, _ = util.active_pools(self.cook_url)
        all_job_uuids = []
        try:
            default_pool = util.default_pool(self.cook_url)
            self.assertLess(1, len(pools))
            self.assertIsNotNone(default_pool)

            cpus = 0.1
            with admin:
                for pool in pools:
                    # Lower the user's cpu quota on this pool
                    pool_name = pool['name']
                    quota_multiplier = 1 if pool_name == default_pool else 2
                    util.set_limit(self.cook_url, 'quota', user.name, cpus=cpus * quota_multiplier, pool=pool_name)

            with user:
                for pool in pools:
                    pool_name = pool['name']

                    # Submit a job that fills the user's quota on this pool
                    quota = util.get_limit(self.cook_url, 'quota', user.name, pool_name).json()
                    quota_cpus = quota['cpus']
                    filling_job_uuid, _ = util.submit_job(self.cook_url, cpus=quota_cpus,
                                                          command='sleep 600', pool=pool_name)
                    all_job_uuids.append(filling_job_uuid)
                    instance = util.wait_for_running_instance(self.cook_url, filling_job_uuid)
                    slave_pool = util.slave_pool(self.mesos_url, instance['hostname'])
                    self.assertEqual(pool_name, slave_pool)

                    # Submit a job that should not get scheduled
                    job_uuid, _ = util.submit_job(self.cook_url, cpus=cpus, command='ls', pool=pool_name)
                    all_job_uuids.append(job_uuid)
                    job = util.load_job(self.cook_url, job_uuid)
                    self.assertEqual('waiting', job['status'])

                    # Assert that the unscheduled reason and data are correct
                    @retry(stop_max_delay=60000, wait_fixed=5000)
                    def check_unscheduled_reason():
                        jobs, _ = util.unscheduled_jobs(self.cook_url, job_uuid)
                        self.logger.info(f'Unscheduled jobs: {jobs}')
                        self.assertEqual(job_uuid, jobs[0]['uuid'])
                        job_reasons = jobs[0]['reasons']
                        # Check the spot-in-queue reason
                        reason = next(r for r in job_reasons if r['reason'] == 'You have 1 other jobs ahead in the '
                                                                               'queue.')
                        self.assertEqual({'jobs': [filling_job_uuid]}, reason['data'])
                        # Check the exceeding-quota reason
                        reason = next(r for r in job_reasons if r['reason'] == reasons.JOB_WOULD_EXCEED_QUOTA)
                        self.assertEqual({'cpus': {'limit': quota_cpus, 'usage': quota_cpus + cpus}}, reason['data'])

                    check_unscheduled_reason()
        finally:
            with admin:
                util.kill_jobs(self.cook_url, all_job_uuids, assert_response=False)
                for pool in pools:
                    util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name(), pool=pool['name'])
