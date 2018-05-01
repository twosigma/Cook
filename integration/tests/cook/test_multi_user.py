import logging
import unittest

import pytest

from tests.cook import mesos, util


@pytest.mark.multi_user
@unittest.skipUnless(util.multi_user_tests_enabled(), 'Requires using multi-user coniguration '
                                                      '(e.g., BasicAuth) for Cook Scheduler')
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class MultiUserCookTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retrieve_cook_url()
        util.init_cook_session(cls.cook_url)

    def setUp(self):
        self.cook_url = type(self).cook_url
        self.logger = logging.getLogger(__name__)
        self.user_factory = util.UserFactory(self)

    def test_job_delete_permission(self):
        user1, user2 = self.user_factory.new_users(2)
        with user1:
            job_uuid, resp = util.submit_job(self.cook_url, command='sleep 30')
        try:
            self.assertEqual(resp.status_code, 201, resp.text)
            with user2:
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            with user1:
                util.kill_jobs(self.cook_url, [job_uuid])
            job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
            self.assertEqual('failed', job['state'])
        finally:
            with user1:
                util.kill_jobs(self.cook_url, [job_uuid])

    def test_group_delete_permission(self):
        user1, user2 = self.user_factory.new_users(2)
        with user1:
            group_spec = util.minimal_group()
            group_uuid = group_spec['uuid']
            job_uuid, resp = util.submit_job(self.cook_url, command='sleep 30', group=group_uuid)
        try:
            self.assertEqual(resp.status_code, 201, resp.text)
            with user2:
                util.kill_groups(self.cook_url, [group_uuid], expected_status_code=403)
            with user1:
                util.kill_groups(self.cook_url, [group_uuid])
            job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
            self.assertEqual('failed', job['state'])
        finally:
            with user1:
                util.kill_jobs(self.cook_url, [job_uuid])

    def test_multi_user_usage(self):
        users = self.user_factory.new_users(6)
        job_resources = {'cpus': 0.1, 'mem': 123}
        all_job_uuids = []
        pools, _ = util.all_pools(self.cook_url)
        try:
            # Start jobs for several users
            for i, user in enumerate(users):
                with user:
                    for j in range(i):
                        job_uuid, resp = util.submit_job(self.cook_url, command='sleep 480',
                                                         max_retries=2, **job_resources)
                        self.assertEqual(resp.status_code, 201, resp.content)
                        all_job_uuids.append(job_uuid)
            # Don't query until the jobs are all running
            util.wait_for_jobs(self.cook_url, all_job_uuids, 'running')
            # Check the usage for each of our users
            for i, user in enumerate(users):
                with user:
                    # Get the current usage
                    resp = util.user_current_usage(self.cook_url, user=user.name)
                    self.assertEqual(resp.status_code, 200, resp.content)
                    usage_data = resp.json()
                    # Check that the response structure looks as expected
                    if pools:
                        self.assertEqual(list(usage_data.keys()), ['total_usage', 'pools'], usage_data)
                    else:
                        self.assertEqual(list(usage_data.keys()), ['total_usage'], usage_data)
                    self.assertEqual(len(usage_data['total_usage']), 4, usage_data)
                    # Check that each user's usage is as expected
                    self.assertEqual(usage_data['total_usage']['mem'], job_resources['mem'] * i, usage_data)
                    self.assertEqual(usage_data['total_usage']['cpus'], job_resources['cpus'] * i, usage_data)
                    self.assertEqual(usage_data['total_usage']['gpus'], 0, usage_data)
                    self.assertEqual(usage_data['total_usage']['jobs'], i, usage_data)
        finally:
            for job_uuid in all_job_uuids:
                job = util.load_job(self.cook_url, job_uuid)
                for instance in job['instances']:
                    if instance['status'] == 'failed':
                        mesos.dump_sandbox_files(util.session, instance, job)
            # Terminate all of the jobs
            if all_job_uuids:
                with self.user_factory.admin():
                    util.kill_jobs(self.cook_url, all_job_uuids)

    def test_job_cpu_quota(self):
        admin = self.user_factory.admin()
        user = self.user_factory.new_user()
        all_job_uuids = []
        try:
            # User with no quota can't submit jobs
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, cpus=0)
                self.assertEqual(resp.status_code, 201, resp.text)
            with user:
                _, resp = util.submit_job(self.cook_url)
                self.assertEqual(resp.status_code, 422, msg=resp.text)
            # User with tiny quota can't submit bigger jobs, but can submit tiny jobs
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, cpus=0.25)
                self.assertEqual(resp.status_code, 201, resp.text)
            with user:
                _, resp = util.submit_job(self.cook_url, cpus=0.5)
                self.assertEqual(resp.status_code, 422, msg=resp.text)
                job_uuid, resp = util.submit_job(self.cook_url, cpus=0.25)
                self.assertEqual(resp.status_code, 201, msg=resp.text)
                all_job_uuids.append(job_uuid)
            # Reset user's quota back to default, then user can submit jobs again
            with admin:
                resp = util.reset_limit(self.cook_url, 'quota', user.name)
                self.assertEqual(resp.status_code, 204, resp.text)
            with user:
                job_uuid, resp = util.submit_job(self.cook_url)
                self.assertEqual(resp.status_code, 201, msg=resp.text)
                all_job_uuids.append(job_uuid)
            # Can't set negative quota
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, cpus=-4)
                self.assertEqual(resp.status_code, 400, resp.text)
        finally:
            with admin:
                util.kill_jobs(self.cook_url, all_job_uuids)
                util.reset_limit(self.cook_url, 'quota', user.name)

    def test_job_mem_quota(self):
        admin = self.user_factory.admin()
        user = self.user_factory.new_user()
        all_job_uuids = []
        try:
            # User with no quota can't submit jobs
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, mem=0)
                self.assertEqual(resp.status_code, 201, resp.text)
            with user:
                _, resp = util.submit_job(self.cook_url)
                self.assertEqual(resp.status_code, 422, msg=resp.text)
            # User with tiny quota can't submit bigger jobs, but can submit tiny jobs
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, mem=10)
                self.assertEqual(resp.status_code, 201, resp.text)
            with user:
                _, resp = util.submit_job(self.cook_url, mem=11)
                self.assertEqual(resp.status_code, 422, msg=resp.text)
                job_uuid, resp = util.submit_job(self.cook_url, mem=10)
                self.assertEqual(resp.status_code, 201, msg=resp.text)
                all_job_uuids.append(job_uuid)
            # Reset user's quota back to default, then user can submit jobs again
            with admin:
                resp = util.reset_limit(self.cook_url, 'quota', user.name)
                self.assertEqual(resp.status_code, 204, resp.text)
            with user:
                job_uuid, resp = util.submit_job(self.cook_url)
                self.assertEqual(resp.status_code, 201, msg=resp.text)
                all_job_uuids.append(job_uuid)
            # Can't set negative quota
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, mem=-128)
                self.assertEqual(resp.status_code, 400, resp.text)
        finally:
            with admin:
                util.kill_jobs(self.cook_url, all_job_uuids)
                util.reset_limit(self.cook_url, 'quota', user.name)

    def test_job_count_quota(self):
        admin = self.user_factory.admin()
        user = self.user_factory.new_user()
        all_job_uuids = []
        try:
            # User with no quota can't submit jobs
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, count=0)
                self.assertEqual(resp.status_code, 201, resp.text)
            with user:
                _, resp = util.submit_job(self.cook_url)
                self.assertEqual(resp.status_code, 422, msg=resp.text)
            # Reset user's quota back to default, then user can submit jobs again
            with admin:
                resp = util.reset_limit(self.cook_url, 'quota', user.name)
                self.assertEqual(resp.status_code, 204, resp.text)
            with user:
                job_uuid, resp = util.submit_job(self.cook_url)
                self.assertEqual(resp.status_code, 201, msg=resp.text)
                all_job_uuids.append(job_uuid)
            # Can't set negative quota
            with admin:
                resp = util.set_limit(self.cook_url, 'quota', user.name, count=-1)
                self.assertEqual(resp.status_code, 400, resp.text)
        finally:
            with admin:
                util.kill_jobs(self.cook_url, all_job_uuids)
                util.reset_limit(self.cook_url, 'quota', user.name)
