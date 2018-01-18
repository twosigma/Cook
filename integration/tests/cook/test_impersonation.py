import itertools
import logging
import pytest
import unittest

from tests.cook import util

@pytest.mark.impersonation
@unittest.skipUnless(util.multi_user_tests_enabled(), 'Requires using multi-user coniguration (e.g., BasicAuth) for Cook Scheduler')
class ImpersonationCookTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retrieve_cook_url()
        util.init_cook_session(cls.cook_url)

    def setUp(self):
        self.cook_url = type(self).cook_url
        self.logger = logging.getLogger(__name__)
        self.user_factory = util.UserFactory(self)
        self.poser = util.User('poser')

    def test_impersonated_job_delete(self):
        user1, user2 = self.user_factory.new_users(2)
        with user1:
            job_uuid, resp = util.submit_job(self.cook_url, command='sleep 60')
            self.assertEqual(resp.status_code, 201, resp.text)
        try:
            # authorized impersonator
            with self.poser:
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            with self.poser.impersonating(user2):
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            with self.poser.impersonating(user1):
                util.kill_jobs(self.cook_url, [job_uuid])
            # unauthorized impersonation attempts by arbitrary user
            with user2:
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            with user2.impersonating(user2):
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            with user2.impersonating(user1):
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            # unauthorized impersonation attempts by job owner
            with user1.impersonating(user2):
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            with user1.impersonating(user1):
                util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
            with user1:
                util.kill_jobs(self.cook_url, [job_uuid])
        finally:
            # default user should be an admin, and can kill all the jobs
            util.kill_jobs(self.cook_url, [job_uuid])

    def test_admin_cannot_impersonate(self):
        user1 = self.user_factory.new_user()
        job_uuids = []
        # the default user should have admin rights
        admin = util.default_user
        try:
            # admin can create jobs
            with admin:
                job_uuid, resp = util.submit_job(self.cook_url, command='sleep 1')
                self.assertEqual(resp.status_code, 201, resp.text)
                job_uuids.append(job_uuid)
            # users can create jobs
            with user1:
                job_uuid, resp = util.submit_job(self.cook_url, command='sleep 1')
                self.assertEqual(resp.status_code, 201, resp.text)
                job_uuids.append(job_uuid)
            # admin cannot impersonate others creating jobs (not an authorized impersonator)
            with admin.impersonating(user1):
                job_uuid, resp = util.submit_job(self.cook_url, command='sleep 1')
                self.assertEqual(resp.status_code, 403, resp.text)
        finally:
            # default user should be an admin, and can kill all the jobs
            util.kill_jobs(self.cook_url, [ j for j in job_uuids if j ])

    def test_cannot_impersonate_endpoints(self):
        user1 = self.user_factory.new_user()
        job_uuids = []
        # the default user should have admin rights
        admin = util.default_user
        # admin can do admin things
        with admin:
            # read queue endpoint
            resp = util.query_queue(self.cook_url)
            self.assertEqual(resp.status_code, 200, resp.text)
            # set user quota
            resp = util.set_limit(self.cook_url, 'quota', user1.name, cpus=20)
            self.assertEqual(resp.status_code, 201, resp.text)
            # reset user quota back to default
            resp = util.reset_limit(self.cook_url, 'quota', user1.name)
            self.assertEqual(resp.status_code, 204, resp.text)
            # set user share
            resp = util.set_limit(self.cook_url, 'share', user1.name, cpus=10)
            self.assertEqual(resp.status_code, 201, resp.text)
            # reset user share back to default
            resp = util.reset_limit(self.cook_url, 'share', user1.name)
            self.assertEqual(resp.status_code, 204, resp.text)
        # impersonator cannot indirectly do admin things
        with admin.impersonating(user1):
            # read queue endpoint
            resp = util.query_queue(self.cook_url)
            self.assertEqual(resp.status_code, 403, resp.text)
            # set user quota
            resp = util.set_limit(self.cook_url, 'quota', user1.name, cpus=20)
            self.assertEqual(resp.status_code, 403, resp.text)
            # reset user quota back to default
            resp = util.reset_limit(self.cook_url, 'quota', user1.name)
            self.assertEqual(resp.status_code, 403, resp.text)
            # set user share
            resp = util.set_limit(self.cook_url, 'share', user1.name, cpus=10)
            self.assertEqual(resp.status_code, 403, resp.text)
            # reset user share back to default
            resp = util.reset_limit(self.cook_url, 'share', user1.name)
            self.assertEqual(resp.status_code, 403, resp.text)
