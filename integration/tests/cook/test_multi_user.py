import json
import logging
import os
import time
import unittest

import pytest

from tests.cook import mesos, util, reasons


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

    def test_rate_limit_while_creating_job(self):
        # Make sure the rate limit cuts a user off.
        settings = util.settings(self.cook_url)
        if settings['rate-limit']['job-submission'] is None:
            pytest.skip("Can't test job submission rate limit without submission rate limit set.")
        if not settings['rate-limit']['job-submission']['enforce?']:
            pytest.skip("Enforcing must be on for test to run")
        user = self.user_factory.new_user()
        bucket_size = settings['rate-limit']['job-submission']['bucket-size']
        extra_size = replenishment_rate = settings['rate-limit']['job-submission']['tokens-replenished-per-minute']
        if extra_size < 100:
            extra_size = 100
        if bucket_size > 3000 or extra_size > 1000:
            pytest.skip("Job submission rate limit test would require making too many or too few jobs to run the test.")
        with user:
            jobs_to_kill = []
            try:
                # First, empty most but not all of the tocken bucket.
                jobs1, resp1 = util.submit_jobs(self.cook_url, {}, bucket_size - 60)
                jobs_to_kill.extend(jobs1)
                self.assertEqual(resp1.status_code, 201)
                # Then another 1060 to get us very negative.
                jobs2, resp2 = util.submit_jobs(self.cook_url, {}, extra_size + 60)
                jobs_to_kill.extend(jobs2)
                self.assertEqual(resp2.status_code, 201)
                # And finally a request that gets cut off.
                jobs3, resp3 = util.submit_jobs(self.cook_url, {}, 10)
                self.assertEqual(resp3.status_code, 400)
                # The timestamp can change so we should only match on the prefix.
                expectedPrefix = f'User {user.name} is inserting too quickly. Not allowed to insert for'
                self.assertEqual(resp3.json()['error'][:len(expectedPrefix)], expectedPrefix)
                # Earn back 70 seconds of tokens.
                time.sleep(70.0 * extra_size / replenishment_rate)
                jobs4, resp4 = util.submit_jobs(self.cook_url, {}, 10)
                jobs_to_kill.extend(jobs4)
                self.assertEqual(resp4.status_code, 201)
            finally:
                util.kill_jobs(self.cook_url, jobs_to_kill)

    # Note that subsequent runs of this test under the same user can fail if sufficient time has not
    # passed; the subsequent run will have used up the rate limit quota and it will need time to recharge.
    def test_rate_limit_launching_jobs(self):
        settings = util.settings(self.cook_url)
        if settings['rate-limit']['job-launch'] is None:
            pytest.skip("Can't test job launch rate limit without launch rate limit set.")

        # Allow an environmental variable override.
        name = os.getenv('COOK_LAUNCH_RATE_LIMIT_USER_NAME')
        if name is not None:
            user = self.user_factory.user_class(name)
        else:
            user = self.user_factory.new_user()

        if not settings['rate-limit']['job-launch']['enforce?']:
            pytest.skip("Enforcing must be on for test to run")
        bucket_size = settings['rate-limit']['job-launch']['bucket-size']
        token_rate = settings['rate-limit']['job-launch']['tokens-replenished-per-minute']
        # In some environments, e.g., minimesos, we can only launch so many concurrent jobs.
        if token_rate < 5 or token_rate > 20:
            pytest.skip(
                "Job launch rate limit test is only validated to reliably work correctly with certain token rates.")
        if bucket_size < 10 or bucket_size > 20:
            pytest.skip(
                "Job launch rate limit test is only validated to reliably work correctly with certain token bucket sizes.")
        with user:
            job_uuids = []
            try:
                jobspec = {"command": "sleep 240", 'cpus': 0.03, 'mem': 32}

                self.logger.info(f'Submitting initial batch of {bucket_size-1} jobs')
                initial_uuids, initial_response = util.submit_jobs(self.cook_url, jobspec, bucket_size - 1)
                job_uuids.extend(initial_uuids)
                self.assertEqual(201, initial_response.status_code, msg=initial_response.content)

                def submit_jobs():
                    self.logger.info(f'Submitting subsequent batch of {bucket_size-1} jobs')
                    subsequent_uuids, subsequent_response = util.submit_jobs(self.cook_url, jobspec, bucket_size - 1)
                    job_uuids.extend(subsequent_uuids)
                    self.assertEqual(201, subsequent_response.status_code, msg=subsequent_response.content)

                def is_rate_limit_triggered(_):
                    jobs1 = util.query_jobs(self.cook_url, True, uuid=job_uuids).json()
                    waiting_jobs = [j for j in jobs1 if j['status'] == 'waiting']
                    running_jobs = [j for j in jobs1 if j['status'] == 'running']
                    # We submitted just under two buckets. We should only see a bucket + some extra running. No more.
                    return len(running_jobs) >= bucket_size and len(waiting_jobs) > 0

                util.wait_until(submit_jobs, is_rate_limit_triggered)
                jobs2 = util.query_jobs(self.cook_url, True, uuid=job_uuids).json()
                running_jobs = [j for j in jobs2 if j['status'] == 'running']
                waiting_jobs = [j for j in jobs2 if j['status'] == 'waiting']
                self.assertGreaterEqual(len(running_jobs), bucket_size)
                self.assertLessEqual(len(running_jobs), bucket_size+3)
                self.logger.debug(f'There are {len(waiting_jobs)} waiting jobs')

                unscheduled, _ = util.unscheduled_jobs(self.cook_url, *[j['uuid'] for j in waiting_jobs])

                is_job_launch_rate_limited = [
                    any([reasons.JOB_LAUNCH_RATE_LIMIT == reason['reason'] for reason in ii['reasons']])
                    for ii in unscheduled]
                num_launch_rate_limited = len([ii for ii in is_job_launch_rate_limited if ii])
                self.logger.debug(f'There are {num_launch_rate_limited} jobs being rate-limited')
                self.assertGreaterEqual(num_launch_rate_limited,bucket_size/2)

            finally:
                util.kill_jobs(self.cook_url, job_uuids)

    # Note that subsequent runs of this test under the same user can fail if sufficient time has not
    # passed; the subsequent run will have used up the rate limit quota and it will need time to recharge.
    def test_global_rate_limit_launching_jobs(self):
        settings = util.settings(self.cook_url)
        if settings['rate-limit']['global-job-launch'] is None:
            pytest.skip("Can't test job launch rate limit without launch rate limit set.")

        # Allow an environmental variable override.
        name = os.getenv('COOK_LAUNCH_RATE_LIMIT_USER_NAME')
        if name is not None:
            user = self.user_factory.user_class(name)
        else:
            user = self.user_factory.new_user()

        if not settings['rate-limit']['global-job-launch']['enforce?']:
            pytest.skip("Enforcing must be on for test to run")
        bucket_size = settings['rate-limit']['global-job-launch']['bucket-size']
        token_rate = settings['rate-limit']['global-job-launch']['tokens-replenished-per-minute']
        # In some environments, e.g., minimesos, we can only launch so many concurrent jobs.
        if token_rate < 5 or token_rate > 20:
            pytest.skip(
                "Global job launch rate limit test is only validated to reliably work correctly with certain token rates.")
        if bucket_size < 10 or bucket_size > 20:
            pytest.skip(
                "Global job launch rate limit test is only validated to reliably work correctly with certain token bucket sizes.")
        with user:
            job_uuids = []
            try:
                jobspec = {"command": "sleep 240", 'cpus': 0.03, 'mem': 32}

                self.logger.info(f'Submitting initial batch of {bucket_size-1} jobs')
                initial_uuids, initial_response = util.submit_jobs(self.cook_url, jobspec, bucket_size - 1)
                job_uuids.extend(initial_uuids)
                self.assertEqual(201, initial_response.status_code, msg=initial_response.content)

                def submit_jobs():
                    self.logger.info(f'Submitting subsequent batch of {bucket_size-1} jobs')
                    subsequent_uuids, subsequent_response = util.submit_jobs(self.cook_url, jobspec, bucket_size - 1)
                    job_uuids.extend(subsequent_uuids)
                    self.assertEqual(201, subsequent_response.status_code, msg=subsequent_response.content)

                def is_rate_limit_triggered(_):
                    jobs1 = util.query_jobs(self.cook_url, True, uuid=job_uuids).json()
                    running_jobs = [j for j in jobs1 if j['status'] == 'running']
                    waiting_jobs = [j for j in jobs1 if j['status'] == 'waiting']
                    self.logger.debug(f'There are {len(waiting_jobs)} waiting jobs')
                    return len(waiting_jobs) > 0 and len(running_jobs) >= bucket_size

                util.wait_until(submit_jobs, is_rate_limit_triggered,120000,5000)
                jobs2 = util.query_jobs(self.cook_url, True, uuid=job_uuids).json()
                running_jobs = [j for j in jobs2 if j['status'] == 'running']
                self.assertGreaterEqual(len(running_jobs), bucket_size)
                self.assertLessEqual(len(running_jobs), bucket_size+4)
            finally:
                util.kill_jobs(self.cook_url, job_uuids)


    def trigger_preemption(self, pool):
        """
        Triggers preemption on the provided pool (which can be None) by doing the following:

        1. Choose a user, X
        2. Lower X's cpu share to 0.1 and cpu quota to 1.0
        3. Submit a job, J1, from X with 1.0 cpu and priority 99 (fills the cpu quota)
        4. Wait for J1 to start running
        5. Submit a job, J2, from X with 0.1 cpu and priority 100
        6. Wait until J1 is preempted (to make room for J2)
        """
        admin = self.user_factory.admin()
        user = self.user_factory.new_user()
        all_job_uuids = []
        try:
            small_cpus = 0.1
            large_cpus = small_cpus * 10
            with admin:
                # Lower the user's cpu share and quota
                util.set_limit(self.cook_url, 'share', user.name, cpus=small_cpus, pool=pool)
                util.set_limit(self.cook_url, 'quota', user.name, cpus=large_cpus, pool=pool)

            with user:
                # Submit a large job that fills up the user's quota
                base_priority = 99
                command = 'sleep 600'
                uuid_large, _ = util.submit_job(self.cook_url, priority=base_priority,
                                                cpus=large_cpus, command=command, pool=pool)
                all_job_uuids.append(uuid_large)
                util.wait_for_running_instance(self.cook_url, uuid_large)

                # Submit a higher-priority job that should trigger preemption
                uuid_high_priority, _ = util.submit_job(self.cook_url, priority=base_priority + 1,
                                                        cpus=small_cpus, command=command,
                                                        name='higher_priority_job', pool=pool)
                all_job_uuids.append(uuid_high_priority)

                # Assert that the lower-priority job was preempted
                def low_priority_job():
                    job = util.load_job(self.cook_url, uuid_large)
                    one_hour_in_millis = 60 * 60 * 1000
                    start = util.current_milli_time() - one_hour_in_millis
                    end = util.current_milli_time()
                    running = util.jobs(self.cook_url, user=user.name, state='running', start=start, end=end).json()
                    waiting = util.jobs(self.cook_url, user=user.name, state='waiting', start=start, end=end).json()
                    self.logger.info(f'Currently running jobs: {json.dumps(running, indent=2)}')
                    self.logger.info(f'Currently waiting jobs: {json.dumps(waiting, indent=2)}')
                    return job

                def job_was_preempted(job):
                    for instance in job['instances']:
                        self.logger.debug(f'Checking if instance was preempted: {instance}')
                        if instance.get('reason_string') == 'Preempted by rebalancer':
                            return True
                    self.logger.info(f'Job has not been preempted: {job}')
                    return False

                max_wait_ms = util.settings(self.cook_url)['rebalancer']['interval-seconds'] * 1000 * 1.5
                self.logger.info(f'Waiting up to {max_wait_ms} milliseconds for preemption to happen')
                util.wait_until(low_priority_job, job_was_preempted, max_wait_ms=max_wait_ms, wait_interval_ms=5000)
        finally:
            with admin:
                util.kill_jobs(self.cook_url, all_job_uuids, assert_response=False)
                util.reset_limit(self.cook_url, 'share', user.name, reason=self.current_name(), pool=pool)
                util.reset_limit(self.cook_url, 'quota', user.name, reason=self.current_name(), pool=pool)

    @unittest.skipUnless(util.is_preemption_enabled(), 'Preemption is not enabled on the cluster')
    @pytest.mark.serial
    def test_preemption_basic(self):
        self.trigger_preemption(pool=None)

    @unittest.skipUnless(util.is_preemption_enabled(), 'Preemption is not enabled on the cluster')
    @unittest.skipUnless(util.are_pools_enabled(), 'Pools are not enabled on the cluster')
    @pytest.mark.serial
    def test_preemption_for_pools(self):
        pools, _ = util.active_pools(self.cook_url)
        self.assertLess(0, len(pools))
        for pool in pools:
            self.logger.info(f'Triggering preemption for {pool}')
            self.trigger_preemption(pool=pool['name'])

    @unittest.skipUnless(util.are_pools_enabled(), "Requires pools")
    def test_user_total_usage(self):
        user = self.user_factory.new_user()
        with user:
            job_spec = {'cpus': 0.11, 'mem': 123, 'command': 'sleep 600'}
            pools, _ = util.active_pools(self.cook_url)
            job_uuids = []
            try:
                for pool in pools:
                    job_uuid, resp = util.submit_job(self.cook_url, pool=pool['name'], **job_spec)
                    self.assertEqual(201, resp.status_code, resp.text)
                    job_uuids.append(job_uuid)

                util.wait_for_jobs(self.cook_url, job_uuids, 'running')
                resp = util.user_current_usage(self.cook_url, user=user.name, group_breakdown='true')
                self.assertEqual(resp.status_code, 200, resp.content)
                usage_data = resp.json()
                total_usage = usage_data['total_usage']

                self.assertEqual(job_spec['mem'] * len(job_uuids), total_usage['mem'], total_usage)
                self.assertEqual(job_spec['cpus'] * len(job_uuids), total_usage['cpus'], total_usage)
                self.assertEqual(len(job_uuids), total_usage['jobs'], total_usage)
            finally:
                util.kill_jobs(self.cook_url, job_uuids)


    def test_queue_quota_filtering(self):
        bad_constraint = [["HOSTNAME",
                           "EQUALS",
                           "lol won't get scheduled"]]
        user = self.user_factory.new_user()
        admin = self.user_factory.admin()
        uuids = []
        default_pool = util.default_pool(self.cook_url)
        pool = default_pool or 'no-pool'
        def queue_uuids():
            try:
                queue = util.query_queue(self.cook_url).json()
                uuids = [j['job/uuid'] for j in queue[pool] if j['job/user'] == user.name]
                self.logger.info(f'Queued uuids: {uuids}')
                return uuids
            except BaseException as e:
                self.logger.error(f"Error when querying queue: {e}")
                raise e
        try:
            with admin:
                resp = util.reset_limit(self.cook_url, 'quota', user.name)
                resp = util.set_limit(self.cook_url, 'quota', user.name, count=1)
                self.assertEqual(resp.status_code, 201, resp.text)
            with user:
                uuid1, resp = util.submit_job(self.cook_url, priority=1, constraints=bad_constraint)
                self.assertEqual(resp.status_code, 201, resp.text)
                uuids.append(uuid1)
                self.logger.info(f'Priority 1 uuid: {uuid1}')
                uuid2, resp = util.submit_job(self.cook_url, priority=2, constraints=bad_constraint)
                self.assertEqual(resp.status_code, 201, resp.text)
                uuids.append(uuid2)
                self.logger.info(f'Priority 2 uuid: {uuid2}')
                uuid3, resp = util.submit_job(self.cook_url, priority=3, constraints=bad_constraint)
                self.assertEqual(resp.status_code, 201, resp.text)
                uuids.append(uuid3)
                self.logger.info(f'Priority 3 uuid: {uuid3}')
            with admin:
                # Only the highest priority job should be queued
                util.wait_until(queue_uuids, lambda uuids: uuids == [uuid3])
            with user:
                uuid, resp = util.submit_job(self.cook_url, command='sleep 300', priority=100)
                self.assertEqual(resp.status_code, 201, resp.text)
                uuids.append(uuid)
                util.wait_for_job(self.cook_url, uuid, 'running')
            with admin:
                # No jobs should be in the queue endpoint
                util.wait_until(queue_uuids, lambda uuids: uuids == [])
        finally:
            with admin:
                util.reset_limit(self.cook_url, 'quota', user.name)
                util.kill_jobs(self.cook_url, uuids)
