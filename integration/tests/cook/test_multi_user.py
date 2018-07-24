import logging
import unittest

import pytest

from tests.cook import mesos, util


@pytest.mark.multi_user
@unittest.skipUnless(util.multi_user_tests_enabled(), 'Requires using multi-user coniguration '
                                                      '(e.g., BasicAuth) for Cook Scheduler')
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class MultiUserCookTest(util.CookTest):

    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retrieve_cook_url()
        util.init_cook_session(cls.cook_url)

    def setUp(self):
        self.cook_url = type(self).cook_url
        self.mesos_url = util.retrieve_mesos_url()
        self.logger = logging.getLogger(__name__)
        self.user_factory = util.UserFactory(self)

    def test_job_delete_permission(self):
        user1, user2 = self.user_factory.new_users(2)
        with user1:
            job_uuid, resp = util.submit_job(self.cook_url, command='sleep 30')
        try:
            self.assertEqual(resp.status_code, 201, resp.text)
            with user2:
                resp = util.kill_jobs(self.cook_url, [job_uuid], expected_status_code=403)
                self.assertEqual(f'You are not authorized to kill the following jobs: {job_uuid}',
                                 resp.json()['error'])
            with user1:
                util.kill_jobs(self.cook_url, [job_uuid])
            job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
            self.assertEqual('failed', job['state'])
        finally:
            with user1:
                util.kill_jobs(self.cook_url, [job_uuid], assert_response=False)

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
                util.kill_jobs(self.cook_url, [job_uuid], assert_response=False)

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
                        job = util.load_job(self.cook_url, job_uuid)
                        self.assertEqual(user.name, job['user'], job)
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
                    util.kill_jobs(self.cook_url, all_job_uuids, assert_response=False)

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
                resp = util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name())
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
                util.kill_jobs(self.cook_url, all_job_uuids, assert_response=False)
                util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name())

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
                resp = util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name())
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
                util.kill_jobs(self.cook_url, all_job_uuids, assert_response=False)
                util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name())

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
                resp = util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name())
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
                util.kill_jobs(self.cook_url, all_job_uuids, assert_response=False)
                util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name())

    @unittest.skipUnless(util.is_preemption_enabled(), 'Preemption is not enabled on the cluster')
    @pytest.mark.serial
    def test_preemption(self):
        admin = self.user_factory.admin()
        user = self.user_factory.new_user()
        all_job_uuids = []
        try:
            small_cpus = 0.1
            with admin:
                # Lower the user's cpu share
                util.set_limit(self.cook_url, 'share', user.name, cpus=small_cpus)

            with user:
                # Submit a small job
                base_priority = 99
                command = 'sleep 600'
                uuid_small, _ = util.submit_job(self.cook_url, priority=base_priority, cpus=small_cpus, command=command)
                all_job_uuids.append(uuid_small)
                instance = util.wait_for_running_instance(self.cook_url, uuid_small)
                hostname = instance['hostname']
                host_cpus = util.slave_cpus(self.mesos_url, hostname)
                max_cpus = util.task_constraint_cpus(self.cook_url)
                desired_cpus = host_cpus - small_cpus
                if max_cpus >= desired_cpus:
                    job_cpus = [desired_cpus]
                else:
                    num_max_cpu_jobs = int(desired_cpus / max_cpus)
                    job_cpus = [max_cpus] * num_max_cpu_jobs
                    job_cpus.append(desired_cpus - num_max_cpu_jobs * max_cpus)

                # Submit lower priority jobs that fill the rest of the agent
                constraints = [["HOSTNAME", "EQUALS", hostname]]
                low_priority_uuids = []
                for cpus in job_cpus:
                    uuid, _ = util.submit_job(self.cook_url, priority=base_priority - 1,
                                              cpus=cpus,
                                              command=command, constraints=constraints)
                    low_priority_uuids.append(uuid)

                all_job_uuids.extend(low_priority_uuids)
                low_priority_instances = []
                for uuid in low_priority_uuids:
                    instance = util.wait_for_running_instance(self.cook_url, uuid)
                    self.assertEqual(hostname, instance['hostname'])

                # Submit a higher-priority job that should trigger preemption
                uuid_high_priority, _ = util.submit_job(self.cook_url, priority=base_priority + 1, cpus=small_cpus * 2,
                                                        command=command, constraints=constraints)
                all_job_uuids.append(uuid_high_priority)

                # Assert that one of the lower-priority jobs was preempted
                def job_was_preempted(jobs):
                    for job in jobs:
                        for instance in job['instances']:
                            self.logger.debug(f'Checking if instance was preempted: {instance}')
                            if instance.get('reason_string') == 'Preempted by rebalancer':
                                return True
                            else:
                                self.logger.info(f'Job has not been preempted: {job}')
                    return False

                max_wait_ms = util.settings(self.cook_url)['rebalancer']['interval-seconds'] * 1000 * 1.5
                self.logger.info(f'Waiting up to {max_wait_ms} milliseconds for preemption to happen')
                util.wait_until(lambda: [util.load_job(self.cook_url, uuid) for uuid in low_priority_uuids], job_was_preempted,
                                max_wait_ms=max_wait_ms, wait_interval_ms=5000)
        finally:
            with admin:
                util.kill_jobs(self.cook_url, all_job_uuids, assert_response=False)
                util.reset_limit(self.cook_url, 'share', user.name, reason=self.current_name())
