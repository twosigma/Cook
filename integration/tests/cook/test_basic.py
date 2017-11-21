from collections import Counter
import json
import logging
import math
import subprocess
import time
import unittest
import uuid

from nose.plugins.attrib import attr
from retrying import retry

from tests.cook import reasons
from tests.cook import util


class CookTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        self.cook_url = util.retrieve_cook_url()
        self.mesos_url = util.retrieve_mesos_url()
        self.logger = logging.getLogger(__name__)
        util.wait_for_cook(self.cook_url)

    def test_basic_submit(self):
        job_executor_type = util.get_job_executor_type(self.cook_url)
        job_uuid, resp = util.submit_job(self.cook_url, executor=job_executor_type)
        self.assertEqual(resp.status_code, 201, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual('success', job['instances'][0]['status'])
        self.assertEqual(False, job['disable_mea_culpa_retries'])
        self.assertTrue(len(util.wait_for_output_url(self.cook_url, job_uuid)['output_url']) > 0)

        if job_executor_type == 'cook':
            job = util.wait_for_exit_code(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(0, job['instances'][0]['exit_code'], message)

            job = util.wait_for_sandbox_directory(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertIsNotNone(job['instances'][0]['output_url'], message)
            self.assertIsNotNone(job['instances'][0]['sandbox_directory'], message)

    def test_no_cook_executor_on_subsequent_instances(self):
        job_uuid, resp = util.submit_job(self.cook_url, command='exit 1', max_retries=10)
        self.assertEqual(resp.status_code, 201, msg=resp.content)
        try:
            job = util.wait_for_job(self.cook_url, job_uuid, 'completed', max_delay=60000)
            message = json.dumps(job, sort_keys=True)
            self.assertEqual('failed', job['state'], message)
            self.assertEqual('completed', job['status'], message)
        except:
            pass
        job = util.load_job(self.cook_url, job_uuid)
        message = json.dumps(job, sort_keys=True)
        job_instances = sorted(job['instances'], key=lambda instance: instance['end_time'])
        self.assertTrue(len(job_instances) >= 2, message)  # sort job instances
        for i in range(1, len(job_instances)):
            message = 'Index ' + str(i) + json.dumps(job_instances[i], sort_keys=True)
            self.assertEqual('failed', job_instances[i]['status'], message)
            self.assertEqual('Command exited non-zero', job_instances[i]['reason_string'], message)
            self.assertEqual('mesos', job_instances[i]['executor'], message)

    def test_disable_mea_culpa(self):
        job_uuid, resp = util.submit_job(self.cook_url, disable_mea_culpa_retries=True)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.load_job(self.cook_url, job_uuid)
        self.assertEqual(True, job['disable_mea_culpa_retries'])

        job_uuid, resp = util.submit_job(self.cook_url, disable_mea_culpa_retries=False)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.load_job(self.cook_url, job_uuid)
        self.assertEqual(False, job['disable_mea_culpa_retries'])

    def test_executor_flag(self):
        job_uuid, resp = util.submit_job(self.cook_url, executor='cook')
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.load_job(self.cook_url, job_uuid)
        self.assertEqual('cook', job['executor'])

        job_uuid, resp = util.submit_job(self.cook_url, executor='mesos')
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.load_job(self.cook_url, job_uuid)
        self.assertEqual('mesos', job['executor'])

    def test_job_environment_cook_job_uuid_only(self):
        command = 'echo "Job environment:" && env && echo "Checking environment variables..." && ' \
                  'if [ ${#COOK_JOB_GROUP_UUID} -ne 0 ]; then echo "COOK_JOB_GROUP_UUID env is present"; exit 1; ' \
                  'else echo "COOK_JOB_GROUP_UUID env is missing as expected"; fi && ' \
                  'if [ ${#COOK_JOB_UUID} -eq 0 ]; then echo "COOK_JOB_UUID env is missing"; exit 1; ' \
                  'else echo "COOK_JOB_UUID env is present as expected"; fi'
        job_uuid, resp = util.submit_job(self.cook_url, command=command)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(1, len(job['instances']))
        message = json.dumps(job['instances'][0], sort_keys=True)
        self.assertEqual('success', job['instances'][0]['status'], message)

    def test_job_environment_cook_job_and_group_uuid(self):
        command = 'echo "Job environment:" && env && echo "Checking environment variables..." && ' \
                  'if [ ${#COOK_JOB_GROUP_UUID} -eq 0 ]; then echo "COOK_JOB_GROUP_UUID env is missing"; exit 1; ' \
                  'else echo "COOK_JOB_GROUP_UUID env is present as expected"; fi && ' \
                  'if [ ${#COOK_JOB_UUID} -eq 0 ]; then echo "COOK_JOB_UUID env is missing"; exit 1; ' \
                  'else echo "COOK_JOB_UUID env is present as expected"; fi'
        group_uuid = str(uuid.uuid4())
        job_uuid, resp = util.submit_job(self.cook_url, command=command, group=group_uuid)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(1, len(job['instances']))
        message = json.dumps(job['instances'][0], sort_keys=True)
        self.assertEqual('success', job['instances'][0]['status'], message)

    def test_failing_submit(self):
        job_executor_type = util.get_job_executor_type(self.cook_url)
        job_uuid, resp = util.submit_job(self.cook_url, command='exit 1', executor=job_executor_type)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(1, len(job['instances']))
        message = json.dumps(job['instances'][0], sort_keys=True)
        self.assertEqual('failed', job['instances'][0]['status'], message)

        if job_executor_type == 'cook':
            job = util.wait_for_exit_code(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(1, job['instances'][0]['exit_code'], message)

            job = util.wait_for_sandbox_directory(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertIsNotNone(job['instances'][0]['output_url'], message)
            self.assertIsNotNone(job['instances'][0]['sandbox_directory'], message)

    def test_progress_update_submit(self):
        job_executor_type = util.get_job_executor_type(self.cook_url)
        line = util.progress_line(self.cook_url, 25, 'Twenty-five percent in ${PROGRESS_FILE}')
        command = f'echo "{line}" >> ${{PROGRESS_FILE}}; sleep 1; exit 0'
        job_uuid, resp = util.submit_job(self.cook_url, command=command,
                                         env={'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'PROGRESS_FILE',
                                              'PROGRESS_FILE': 'progress.txt'},
                                         executor=job_executor_type, max_runtime=60000)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(1, len(job['instances']))
        message = json.dumps(job['instances'][0], sort_keys=True)
        self.assertEqual('success', job['instances'][0]['status'], message)

        if job_executor_type == 'cook':
            util.sleep_for_publish_interval(self.cook_url)

            job = util.wait_for_exit_code(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(0, job['instances'][0]['exit_code'], message)

            job = util.wait_for_sandbox_directory(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertIsNotNone(job['instances'][0]['output_url'], message)
            self.assertIsNotNone(job['instances'][0]['sandbox_directory'], message)

            job = util.load_job(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(25, job['instances'][0]['progress'], message)
            self.assertEqual('Twenty-five percent in progress.txt', job['instances'][0]['progress_message'], message)

    def test_configurable_progress_update_submit(self):
        job_executor_type = util.get_job_executor_type(self.cook_url)
        command = 'echo "message: 25 Twenty-five percent" > progress_file.txt; sleep 1; exit 0'
        job_uuid, resp = util.submit_job(self.cook_url, command=command, executor=job_executor_type, max_runtime=60000,
                                         progress_output_file='progress_file.txt',
                                         progress_regex_string='message: (\d*) (.*)')
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        message = json.dumps(job, sort_keys=True)
        self.assertEqual('progress_file.txt', job['progress_output_file'], message)
        self.assertEqual('message: (\d*) (.*)', job['progress_regex_string'], message)
        self.assertEqual(1, len(job['instances']))
        message = json.dumps(job['instances'][0], sort_keys=True)
        self.assertEqual('success', job['instances'][0]['status'], message)
        self.assertEqual('success', job['instances'][0]['status'], message)

        if job_executor_type == 'cook':
            util.sleep_for_publish_interval(self.cook_url)

            job = util.wait_for_exit_code(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(0, job['instances'][0]['exit_code'], message)

            job = util.wait_for_sandbox_directory(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertIsNotNone(job['instances'][0]['output_url'], message)
            self.assertIsNotNone(job['instances'][0]['sandbox_directory'], message)

            job = util.load_job(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(25, job['instances'][0]['progress'], message)
            self.assertEqual('Twenty-five percent', job['instances'][0]['progress_message'], message)

    def test_multiple_progress_updates_submit(self):
        job_executor_type = util.get_job_executor_type(self.cook_url)
        line_1 = util.progress_line(self.cook_url, 25, 'Twenty-five percent')
        line_2 = util.progress_line(self.cook_url, 50, 'Fifty percent')
        line_3 = util.progress_line(self.cook_url, '', 'Sixty percent invalid format')
        line_4 = util.progress_line(self.cook_url, 75, 'Seventy-five percent')
        line_5 = util.progress_line(self.cook_url, '', 'Eighty percent invalid format')
        command = f'echo "{line_1}" && sleep 2 && echo "{line_2}" && sleep 2 && ' \
                  f'echo "{line_3}" && sleep 2 && echo "{line_4}" && sleep 2 && ' \
                  f'echo "{line_5}" && sleep 2 && echo "Done" && exit 0'
        job_uuid, resp = util.submit_job(self.cook_url, command=command, executor=job_executor_type, max_runtime=60000)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(1, len(job['instances']))
        message = json.dumps(job['instances'][0], sort_keys=True)
        self.assertEqual('success', job['instances'][0]['status'], message)

        if job_executor_type == 'cook':
            util.sleep_for_publish_interval(self.cook_url)

            job = util.wait_for_exit_code(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(0, job['instances'][0]['exit_code'], message)

            job = util.wait_for_sandbox_directory(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertIsNotNone(job['instances'][0]['output_url'], message)
            self.assertIsNotNone(job['instances'][0]['sandbox_directory'], message)

            job = util.load_job(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(75, job['instances'][0]['progress'], message)
            self.assertEqual('Seventy-five percent', job['instances'][0]['progress_message'], message)

    def test_multiple_rapid_progress_updates_submit(self):
        job_executor_type = util.get_job_executor_type(self.cook_url)

        def progress_string(a):
            return util.progress_line(self.cook_url, a, f'{a}%')

        items = list(range(1, 100, 4)) + list(range(99, 40, -4)) + list(range(40, 81, 2))
        command = ''.join([f'echo "{progress_string(a)}" && ' for a in items]) + 'echo "Done" && exit 0'
        job_uuid, resp = util.submit_job(self.cook_url, command=command, executor=job_executor_type, max_runtime=60000)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(1, len(job['instances']))
        message = json.dumps(job['instances'][0], sort_keys=True)
        self.assertEqual('success', job['instances'][0]['status'], message)

        if job_executor_type == 'cook':
            util.sleep_for_publish_interval(self.cook_url)

            job = util.wait_for_exit_code(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(0, job['instances'][0]['exit_code'], message)

            job = util.wait_for_sandbox_directory(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertIsNotNone(job['instances'][0]['output_url'], message)
            self.assertIsNotNone(job['instances'][0]['sandbox_directory'], message)

            job = util.load_job(self.cook_url, job_uuid)
            message = json.dumps(job['instances'][0], sort_keys=True)
            self.assertEqual(80, job['instances'][0]['progress'], message)
            self.assertEqual('80%', job['instances'][0]['progress_message'], message)

    def test_max_runtime_exceeded(self):
        job_executor_type = util.get_job_executor_type(self.cook_url)
        settings_timeout_interval_minutes = util.get_in(util.settings(self.cook_url), 'task-constraints',
                                                        'timeout-interval-minutes')
        # the value needs to be a little more than 2 times settings_timeout_interval_minutes to allow
        # at least two runs of the lingering task killer
        job_sleep_seconds = (2 * settings_timeout_interval_minutes * 60) + 15
        job_sleep_ms = job_sleep_seconds * 1000
        max_runtime_ms = 5000
        assert max_runtime_ms < job_sleep_ms
        # schedule a job that will go over its max_runtime time limit
        job_uuid, resp = util.submit_job(self.cook_url,
                                         command=f'sleep {job_sleep_seconds}',
                                         executor=job_executor_type, max_runtime=max_runtime_ms)
        try:
            self.assertEqual(201, resp.status_code, msg=resp.content)
            # We wait for the job to start running, and only then start waiting for the max-runtime timeout,
            # otherwise we could get a false-negative on wait-for-job 'completed' because of a scheduling delay.
            # We wait for the 'end_time' attribute separately because there is another small delay
            # between the job being killed and that attribute being set.
            # Having three separate waits also disambiguates the root cause of a wait-timeout failure.
            util.wait_for_job(self.cook_url, job_uuid, 'running')
            util.wait_for_job(self.cook_url, job_uuid, 'completed', job_sleep_ms)
            job = util.wait_for_end_time(self.cook_url, job_uuid)
            job_details = f"Job details: {json.dumps(job, sort_keys=True)}"
            self.assertEqual(1, len(job['instances']), job_details)
            instance = job['instances'][0]
            # did the job fail as expected?
            self.assertEqual('failed', instance['status'], job_details)
            # We currently have three possible reason codes that we can observe
            # due to a race in the scheduler. See issue #515 on GitHub for more details.
            allowed_reasons = [
                # observed status update from when Cook killed the job
                reasons.MAX_RUNTIME_EXCEEDED,
                # observed status update received when exit code appears
                reasons.CMD_NON_ZERO_EXIT,
                # cook killed the job during setup, so the executor had an error
                reasons.EXECUTOR_UNREGISTERED]
            self.assertIn(instance['reason_code'], allowed_reasons, job_details)
            # was the actual running time consistent with running over time and being killed?
            actual_running_time_ms = instance['end_time'] - instance['start_time']
            self.assertGreater(actual_running_time_ms, max_runtime_ms, job_details)
            self.assertGreater(job_sleep_ms, actual_running_time_ms, job_details)

            # verify additional fields set when the cook executor is used
            if job_executor_type == 'cook':
                job = util.wait_for_exit_code(self.cook_url, job_uuid)
                message = json.dumps(job['instances'][0], sort_keys=True)
                self.assertNotEqual(0, job['instances'][0]['exit_code'], message)

                job = util.wait_for_sandbox_directory(self.cook_url, job_uuid)
                message = json.dumps(job['instances'][0], sort_keys=True)
                self.assertIsNotNone(job['instances'][0]['output_url'], message)
                self.assertIsNotNone(job['instances'][0]['sandbox_directory'], message)
        finally:
            util.kill_jobs(self.cook_url, [job_uuid])

    def test_get_job(self):
        # schedule a job
        job_spec = util.minimal_job()
        resp = util.session.post('%s/rawscheduler' % self.cook_url, json={'jobs': [job_spec]})
        self.assertEqual(201, resp.status_code, msg=resp.content)

        # query for the same job & ensure the response has what it's supposed to have
        job = util.wait_for_job(self.cook_url, job_spec['uuid'], 'completed')
        self.assertEquals(job_spec['mem'], job['mem'])
        self.assertEquals(job_spec['max_retries'], job['max_retries'])
        self.assertEquals(job_spec['name'], job['name'])
        self.assertEquals(job_spec['priority'], job['priority'])
        self.assertEquals(job_spec['uuid'], job['uuid'])
        self.assertEquals(job_spec['cpus'], job['cpus'])
        self.assertTrue('labels' in job)
        self.assertEquals(9223372036854775807, job['max_runtime'])
        # 9223372036854775807 is MAX_LONG(ish), the default value for max_runtime
        self.assertEquals('success', job['state'])
        self.assertTrue('env' in job)
        self.assertTrue('framework_id' in job)
        self.assertTrue('ports' in job)
        self.assertTrue('instances' in job)
        self.assertEquals('completed', job['status'])
        self.assertTrue(isinstance(job['submit_time'], int))
        self.assertTrue('uris' in job)
        self.assertTrue('retries_remaining' in job)
        instance = job['instances'][0]
        self.assertTrue(isinstance(instance['start_time'], int))
        self.assertTrue('executor_id' in instance)
        self.assertTrue('hostname' in instance)
        self.assertTrue('slave_id' in instance)
        self.assertTrue(isinstance(instance['preempted'], bool))
        self.assertTrue(isinstance(instance['end_time'], int))
        self.assertTrue(isinstance(instance['backfilled'], bool))
        self.assertTrue('ports' in instance)
        self.assertEquals('completed', job['status'])
        self.assertTrue('task_id' in instance)

    def determine_user(self):
        job_uuid, resp = util.submit_job(self.cook_url)
        self.assertEqual(resp.status_code, 201)
        return util.get_user(self.cook_url, job_uuid)

    def test_list_jobs_by_state(self):
        # schedule a bunch of jobs in hopes of getting jobs into different statuses
        job_specs = [util.minimal_job(command=f"sleep {i}") for i in range(1, 20)]
        try:
            _, resp = util.submit_jobs(self.cook_url, job_specs)
            self.assertEqual(resp.status_code, 201)

            # let some jobs get scheduled
            time.sleep(10)
            user = self.determine_user()

            for state in ['waiting', 'running', 'completed']:
                resp = util.list_jobs(self.cook_url, user=user, state=state)
                self.assertEqual(200, resp.status_code, msg=resp.content)
                jobs = resp.json()
                for job in jobs:
                    # print "%s %s" % (job['uuid'], job['status'])
                    self.assertEquals(state, job['status'])
        finally:
            util.kill_jobs(self.cook_url, job_specs)

    def test_list_jobs_by_time(self):
        # schedule two jobs with different submit times
        job_specs = [util.minimal_job() for _ in range(2)]

        request_body = {'jobs': [job_specs[0]]}
        resp = util.session.post('%s/rawscheduler' % self.cook_url, json=request_body)
        self.assertEqual(resp.status_code, 201)

        time.sleep(5)

        request_body = {'jobs': [job_specs[1]]}
        resp = util.session.post('%s/rawscheduler' % self.cook_url, json=request_body)
        self.assertEqual(resp.status_code, 201)

        submit_times = [util.load_job(self.cook_url, job_spec['uuid'])['submit_time'] for job_spec in job_specs]

        user = self.determine_user()

        # start-ms and end-ms are exclusive

        # query where start-ms and end-ms are the submit times of jobs 1 & 2 respectively
        resp = util.list_jobs(self.cook_url, user=user, state='waiting+running+completed',
                              start_ms=submit_times[0] - 1, end_ms=submit_times[1] + 1)
        self.assertEqual(200, resp.status_code, msg=resp.content)
        jobs = resp.json()
        self.assertTrue(util.contains_job_uuid(jobs, job_specs[0]['uuid']))
        self.assertTrue(util.contains_job_uuid(jobs, job_specs[1]['uuid']))

        # query just for job 1
        resp = util.list_jobs(self.cook_url, user=user, state='waiting+running+completed',
                              start_ms=submit_times[0] - 1, end_ms=submit_times[1])
        self.assertEqual(200, resp.status_code, msg=resp.content)
        jobs = resp.json()
        self.assertTrue(util.contains_job_uuid(jobs, job_specs[0]['uuid']))
        self.assertFalse(util.contains_job_uuid(jobs, job_specs[1]['uuid']))

        # query just for job 2
        resp = util.list_jobs(self.cook_url, user=user, state='waiting+running+completed',
                              start_ms=submit_times[0] + 1, end_ms=submit_times[1] + 1)
        self.assertEqual(200, resp.status_code, msg=resp.content)
        jobs = resp.json()
        self.assertFalse(util.contains_job_uuid(jobs, job_specs[0]['uuid']))
        self.assertTrue(util.contains_job_uuid(jobs, job_specs[1]['uuid']))

        # query for neither
        resp = util.list_jobs(self.cook_url, user=user, state='waiting+running+completed',
                              start_ms=submit_times[0] + 1, end_ms=submit_times[1])
        self.assertEqual(200, resp.status_code, msg=resp.content)
        jobs = resp.json()
        self.assertFalse(util.contains_job_uuid(jobs, job_specs[0]['uuid']))
        self.assertFalse(util.contains_job_uuid(jobs, job_specs[1]['uuid']))

    def test_list_jobs_by_completion_state(self):
        name = str(uuid.uuid4())
        job_uuid_1, resp = util.submit_job(self.cook_url, command='true', name=name)
        self.assertEqual(201, resp.status_code)
        job_uuid_2, resp = util.submit_job(self.cook_url, command='false', name=name)
        self.assertEqual(201, resp.status_code)
        job_uuid_3, resp = util.submit_job(self.cook_url, command='true', name=name)
        self.assertEqual(201, resp.status_code)
        user = self.determine_user()
        start = util.wait_for_job(self.cook_url, job_uuid_1, 'completed')['submit_time']
        end = util.wait_for_job(self.cook_url, job_uuid_2, 'completed')['submit_time'] + 1

        # Test the various combinations of states
        resp = util.list_jobs(self.cook_url, user=user, state='completed', start_ms=start, end_ms=end)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='success+failed', start_ms=start, end_ms=end)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='completed+success+failed', start_ms=start, end_ms=end)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='completed+failed', start_ms=start, end_ms=end)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='completed+success', start_ms=start, end_ms=end)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='success', start_ms=start, end_ms=end)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='running+waiting+success', start_ms=start, end_ms=end)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='failed', start_ms=start, end_ms=end)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        resp = util.list_jobs(self.cook_url, user=user, state='running+waiting+failed', start_ms=start, end_ms=end)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)

        # Test with failed and a specified limit
        resp = util.list_jobs(self.cook_url, user=user, state='failed', start_ms=start, end_ms=end, limit=1, name=name)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)

        # Test with success and a specified limit
        start = end - 1
        end = util.wait_for_job(self.cook_url, job_uuid_3, 'completed')['submit_time'] + 1
        resp = util.list_jobs(self.cook_url, user=user, state='success', start_ms=start, end_ms=end, limit=1, name=name)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)

    def test_list_jobs_by_name(self):
        job_uuid_1, resp = util.submit_job(self.cook_url, name='foo-bar-baz_qux')
        self.assertEqual(201, resp.status_code)
        job_uuid_2, resp = util.submit_job(self.cook_url, name='')
        self.assertEqual(201, resp.status_code)
        job_uuid_3, resp = util.submit_job(self.cook_url, name='.')
        self.assertEqual(201, resp.status_code)
        job_uuid_4, resp = util.submit_job(self.cook_url, name='foo-bar-baz__')
        self.assertEqual(201, resp.status_code)
        job_uuid_5, resp = util.submit_job(self.cook_url, name='ff')
        self.assertEqual(201, resp.status_code)
        job_uuid_6, resp = util.submit_job(self.cook_url, name='a')
        self.assertEqual(201, resp.status_code)
        user = self.determine_user()
        any_state = 'running+waiting+completed'

        resp = util.list_jobs(self.cook_url, user=user, state=any_state)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='*')
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='foo-bar-baz_qux')
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='foo-bar-baz_*')
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='f*')
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='')
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='*.*')
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='foo-bar-baz__*')
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='ff*')
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='a*')
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertTrue(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='a-z0-9_-')
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_1), job_uuid_1)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_2), job_uuid_2)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_3), job_uuid_3)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_4), job_uuid_4)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_5), job_uuid_5)
        self.assertFalse(util.contains_job_uuid(resp.json(), job_uuid_6), job_uuid_6)

    def test_list_with_invalid_name_filters(self):
        user = self.determine_user()
        any_state = 'running+waiting+completed'
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='^[a-z0-9_-]{3,16}$')
        self.assertEqual(400, resp.status_code)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='[a-z0-9_-]{3,16}')
        self.assertEqual(400, resp.status_code)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='[a-z0-9_-]')
        self.assertEqual(400, resp.status_code)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='\d+')
        self.assertEqual(400, resp.status_code)
        resp = util.list_jobs(self.cook_url, user=user, state=any_state, name='a+')
        self.assertEqual(400, resp.status_code)

    def test_cancel_job(self):
        job_uuid, _ = util.submit_job(self.cook_url, command='sleep 300')
        util.wait_for_job(self.cook_url, job_uuid, 'running')
        util.kill_jobs(self.cook_url, [job_uuid])
        job = util.load_job(self.cook_url, job_uuid)
        self.assertEqual('failed', job['state'])

    def test_change_retries(self):
        job_uuid, _ = util.submit_job(self.cook_url, command='sleep 60')
        try:
            util.wait_for_job(self.cook_url, job_uuid, 'running')
            util.kill_jobs(self.cook_url, [job_uuid])

            def instance_query():
                return util.query_jobs(self.cook_url, True, uuid=[job_uuid])

            # wait for the job (and its instances) to die
            util.wait_until(instance_query, util.all_instances_killed)
            # retry the job
            resp = util.retry_jobs(self.cook_url, retries=2, jobs=[job_uuid])
            self.assertEqual(201, resp.status_code, resp.text)
            job = util.load_job(self.cook_url, job_uuid)
            details = f"Job details: {json.dumps(job, sort_keys=True)}"
            self.assertIn(job['status'], ['waiting', 'running'], details)
            self.assertEqual(1, job['retries_remaining'], details)
        finally:
            util.kill_jobs(self.cook_url, [job_uuid])

    def test_change_retries_deprecated_post(self):
        job_uuid, _ = util.submit_job(self.cook_url, command='sleep 60')
        try:
            util.wait_for_job(self.cook_url, job_uuid, 'running')
            util.kill_jobs(self.cook_url, [job_uuid])

            def instance_query():
                return util.query_jobs(self.cook_url, True, uuid=[job_uuid])

            # wait for the job (and its instances) to die
            util.wait_until(instance_query, util.all_instances_killed)
            # retry the job
            resp = util.retry_jobs(self.cook_url, use_deprecated_post=True, retries=2, jobs=[job_uuid])
            self.assertEqual(201, resp.status_code, resp.text)
            job = util.load_job(self.cook_url, job_uuid)
            details = f"Job details: {json.dumps(job, sort_keys=True)}"
            self.assertIn(job['status'], ['waiting', 'running'], details)
            self.assertEqual(1, job['retries_remaining'], details)
        finally:
            util.kill_jobs(self.cook_url, [job_uuid])

    def test_change_failed_retries(self):
        job_specs = util.minimal_jobs(2, max_retries=1, command='sleep 60')
        try:
            jobs, resp = util.submit_jobs(self.cook_url, job_specs)
            self.assertEqual(resp.status_code, 201)
            # wait for first job to start running, and kill it
            job_uuid = jobs[0]
            util.wait_for_job(self.cook_url, job_uuid, 'running')
            util.kill_jobs(self.cook_url, [job_uuid])

            def instance_query():
                return util.query_jobs(self.cook_url, True, uuid=[job_uuid])

            # Wait for the job (and its instances) to die
            util.wait_until(instance_query, util.all_instances_killed)
            job = util.load_job(self.cook_url, job_uuid)
            self.assertEqual('failed', job['state'])
            # retry both jobs, but with the failed_only=true flag
            resp = util.retry_jobs(self.cook_url, retries=4, failed_only=True, jobs=jobs)
            self.assertEqual(201, resp.status_code, resp.text)
            jobs = util.query_jobs(self.cook_url, True, uuid=jobs).json()
            # We expect both jobs to be running now.
            # The first job (which we killed and retried) should have 3 retries remaining
            # (the attempt before resetting the total retries count is still included).
            job_details = f"Job details: {json.dumps(jobs[0], sort_keys=True)}"
            self.assertIn(jobs[0]['status'], ['waiting', 'running'], job_details)
            self.assertEqual(3, jobs[0]['retries_remaining'], job_details)
            # The second job (which started with the default 1 retries)
            # should have 1 remaining since the failed_only flag was set.
            job_details = f"Job details: {json.dumps(jobs[1], sort_keys=True)}"
            self.assertIn(jobs[1]['status'], ['waiting', 'running'], job_details)
            self.assertEqual(1, jobs[1]['retries_remaining'], job_details)
        finally:
            util.kill_jobs(self.cook_url, job_specs)

    def test_cancel_instance(self):
        job_uuid, _ = util.submit_job(self.cook_url, command='sleep 10', max_retries=2)
        job = util.wait_for_job(self.cook_url, job_uuid, 'running')
        task_id = job['instances'][0]['task_id']
        resp = util.session.delete('%s/rawscheduler?instance=%s' % (self.cook_url, task_id))
        self.assertEqual(204, resp.status_code, msg=resp.content)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual('success', job['state'], 'Job details: %s' % (json.dumps(job, sort_keys=True)))

    def test_implicit_group(self):
        group_uuid = str(uuid.uuid4())
        job_spec = {'group': group_uuid}
        jobs, resp = util.submit_jobs(self.cook_url, job_spec, 2)
        self.assertEqual(resp.status_code, 201)
        job_data = util.query_jobs(self.cook_url, uuid=jobs)
        self.assertEqual(200, job_data.status_code)
        job_data = job_data.json()
        self.assertEqual(group_uuid, job_data[0]['groups'][0]['uuid'])
        self.assertEqual(group_uuid, job_data[1]['groups'][0]['uuid'])
        util.wait_for_job(self.cook_url, jobs[0], 'completed')
        util.wait_for_job(self.cook_url, jobs[1], 'completed')

    def test_explicit_group(self):
        group_spec = util.minimal_group()
        group_uuid = group_spec['uuid']
        job_spec = {'group': group_uuid}
        jobs, resp = util.submit_jobs(self.cook_url, job_spec, 2, groups=[group_spec])
        self.assertEqual(resp.status_code, 201)
        job_data = util.query_jobs(self.cook_url, uuid=jobs)
        self.assertEqual(200, job_data.status_code)
        job_data = job_data.json()
        self.assertEqual(group_uuid, job_data[0]['groups'][0]['uuid'])
        self.assertEqual(group_uuid, job_data[1]['groups'][0]['uuid'])
        util.wait_for_job(self.cook_url, jobs[0], 'completed')
        util.wait_for_job(self.cook_url, jobs[1], 'completed')

    def test_straggler_handling(self):
        straggler_handling = {
            'type': 'quantile-deviation',
            'parameters': {
                'quantile': 0.5,
                'multiplier': 2.0
            }
        }
        slow_job_wait_seconds = 1200
        group_spec = util.minimal_group(straggler_handling=straggler_handling)
        job_fast = util.minimal_job(group=group_spec["uuid"])
        job_slow = util.minimal_job(group=group_spec["uuid"],
                                    command='sleep %d' % slow_job_wait_seconds)
        data = {'jobs': [job_fast, job_slow], 'groups': [group_spec]}
        resp = util.session.post('%s/rawscheduler' % self.cook_url, json=data)
        self.assertEqual(resp.status_code, 201)
        util.wait_for_job(self.cook_url, job_fast['uuid'], 'completed')
        util.wait_for_job(self.cook_url, job_slow['uuid'], 'completed',
                          slow_job_wait_seconds * 1000)
        jobs = util.query_jobs(self.cook_url, True, uuid=[job_fast, job_slow]).json()
        self.logger.debug('Loaded jobs %s', jobs)
        self.assertEqual('success', jobs[0]['state'], 'Job details: %s' % (json.dumps(jobs[0], sort_keys=True)))
        self.assertEqual('failed', jobs[1]['state'])
        self.assertEqual(2004, jobs[1]['instances'][0]['reason_code'])

    def test_expected_runtime_field(self):
        # Should support expected_runtime
        expected_runtime = 1
        job_uuid, resp = util.submit_job(self.cook_url, expected_runtime=expected_runtime)
        self.assertEqual(resp.status_code, 201)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        instance = job['instances'][0]
        self.assertEqual('success', instance['status'], 'Instance details: %s' % (json.dumps(instance, sort_keys=True)))
        self.assertEqual(expected_runtime, job['expected_runtime'])

        # Should disallow expected_runtime > max_runtime
        expected_runtime = 2
        max_runtime = expected_runtime - 1
        job_uuid, resp = util.submit_job(self.cook_url, expected_runtime=expected_runtime, max_runtime=max_runtime)
        self.assertEqual(resp.status_code, 400)

    def test_application_field(self):
        # Should support application
        application = {'name': 'foo-app', 'version': '0.1.0'}
        job_uuid, resp = util.submit_job(self.cook_url, application=application)
        self.assertEqual(resp.status_code, 201)
        job = util.load_job(self.cook_url, job_uuid)
        self.assertEqual(application, job['application'])

        # Should require application name
        _, resp = util.submit_job(self.cook_url, application={'version': '0.1.0'})
        self.assertEqual(resp.status_code, 400)

        # Should require application version
        _, resp = util.submit_job(self.cook_url, application={'name': 'foo-app'})
        self.assertEqual(resp.status_code, 400)

    def test_error_while_creating_job(self):
        job1 = util.minimal_job()
        job2 = util.minimal_job(uuid=job1['uuid'])
        resp = util.session.post('%s/rawscheduler' % self.cook_url,
                                 json={'jobs': [job1, job2]})
        self.assertEqual(resp.status_code, 500)

    @attr('explicit')
    def test_constraints(self):
        """
        Marked as explicit due to:
        RuntimeError: Job ... had status running - expected completed
        """
        state = util.get_mesos_state(self.mesos_url)
        hosts = [agent['hostname'] for agent in state['slaves']][:10]

        bad_job_uuid, resp = util.submit_job(self.cook_url, constraints=[["HOSTNAME",
                                                                          "EQUALS",
                                                                          "lol won't get scheduled"]])
        self.assertEqual(resp.status_code, 201, resp.text)

        try:
            host_to_job_uuid = {}
            for hostname in hosts:
                constraints = [["HOSTNAME", "EQUALS", hostname]]
                job_uuid, resp = util.submit_job(self.cook_url, constraints=constraints)
                self.assertEqual(resp.status_code, 201, resp.text)
                host_to_job_uuid[hostname] = job_uuid

            for hostname, job_uuid in host_to_job_uuid.items():
                job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
                hostname_constrained = job['instances'][0]['hostname']
                self.assertEqual(hostname, hostname_constrained)
                self.assertEqual([["HOSTNAME", "EQUALS", hostname]], job['constraints'])
            # This job should have been scheduled since the job submitted after it has completed
            # however, its constraint means it won't get scheduled
            util.wait_for_job(self.cook_url, bad_job_uuid, 'waiting', max_delay=3000)
        finally:
            util.kill_jobs(self.cook_url, [bad_job_uuid])

    def test_allow_partial(self):
        def absent_uuids(response):
            return [part for part in response.json()['error'].split() if util.is_valid_uuid(part)]

        job_uuid_1, resp = util.submit_job(self.cook_url)
        self.assertEqual(201, resp.status_code, msg=resp.content)
        job_uuid_2, resp = util.submit_job(self.cook_url)
        self.assertEqual(201, resp.status_code, msg=resp.content)

        # Only valid job uuids
        resp = util.query_jobs(self.cook_url, uuid=[job_uuid_1, job_uuid_2])
        self.assertEqual(200, resp.status_code, msg=resp.content)

        # Mixed valid, invalid job uuids
        bogus_uuid = str(uuid.uuid4())
        resp = util.query_jobs(self.cook_url, uuid=[job_uuid_1, job_uuid_2, bogus_uuid])
        self.assertEqual(404, resp.status_code, msg=resp.content)
        self.assertEqual([bogus_uuid], absent_uuids(resp))
        resp = util.query_jobs(self.cook_url, uuid=[job_uuid_1, job_uuid_2, bogus_uuid], partial='false')
        self.assertEqual(404, resp.status_code, resp.json())
        self.assertEqual([bogus_uuid], absent_uuids(resp))

        # Partial results with mixed valid, invalid job uuids
        resp = util.query_jobs(self.cook_url, uuid=[job_uuid_1, job_uuid_2, bogus_uuid], partial='true')
        self.assertEqual(200, resp.status_code, resp.json())
        self.assertEqual(2, len(resp.json()))
        self.assertEqual([job_uuid_1, job_uuid_2].sort(), [job['uuid'] for job in resp.json()].sort())

        # Only valid instance uuids
        job = util.wait_for_job(self.cook_url, job_uuid_1, 'completed')
        instance_uuid_1 = job['instances'][0]['task_id']
        job = util.wait_for_job(self.cook_url, job_uuid_2, 'completed')
        instance_uuid_2 = job['instances'][0]['task_id']
        resp = util.query_jobs_via_rawscheduler_endpoint(self.cook_url, instance=[instance_uuid_1, instance_uuid_2])
        self.assertEqual(200, resp.status_code, msg=resp.content)

        # Mixed valid, invalid instance uuids
        instance_uuids = [instance_uuid_1, instance_uuid_2, bogus_uuid]
        resp = util.query_jobs_via_rawscheduler_endpoint(self.cook_url, instance=instance_uuids)
        self.assertEqual(404, resp.status_code, msg=resp.content)
        self.assertEqual([bogus_uuid], absent_uuids(resp))
        resp = util.query_jobs_via_rawscheduler_endpoint(self.cook_url, instance=instance_uuids, partial='false')
        self.assertEqual(404, resp.status_code, msg=resp.content)
        self.assertEqual([bogus_uuid], absent_uuids(resp))

        # Partial results with mixed valid, invalid instance uuids
        resp = util.query_jobs_via_rawscheduler_endpoint(self.cook_url, instance=instance_uuids, partial='true')
        self.assertEqual(200, resp.status_code, msg=resp.content)
        self.assertEqual(2, len(resp.json()))
        self.assertEqual([job_uuid_1, job_uuid_2].sort(), [job['uuid'] for job in resp.json()].sort())

    def test_ports(self):
        job_uuid, resp = util.submit_job(self.cook_url, ports=1)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(1, len(job['instances'][0]['ports']))
        job_uuid, resp = util.submit_job(self.cook_url, ports=10)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual(10, len(job['instances'][0]['ports']))

    def test_allow_partial_for_groups(self):
        def absent_uuids(response):
            return [part for part in response.json()['error'].split() if util.is_valid_uuid(part)]

        group_uuid_1 = str(uuid.uuid4())
        group_uuid_2 = str(uuid.uuid4())
        _, resp = util.submit_job(self.cook_url, group=group_uuid_1)
        self.assertEqual(201, resp.status_code)
        _, resp = util.submit_job(self.cook_url, group=group_uuid_2)
        self.assertEqual(201, resp.status_code)

        # Only valid group uuids
        resp = util.query_groups(self.cook_url, uuid=[group_uuid_1, group_uuid_2])
        self.assertEqual(200, resp.status_code)

        # Mixed valid, invalid group uuids
        bogus_uuid = str(uuid.uuid4())
        resp = util.query_groups(self.cook_url, uuid=[group_uuid_1, group_uuid_2, bogus_uuid])
        self.assertEqual(404, resp.status_code)
        self.assertEqual([bogus_uuid], absent_uuids(resp))
        resp = util.query_groups(self.cook_url, uuid=[group_uuid_1, group_uuid_2, bogus_uuid], partial='false')
        self.assertEqual(404, resp.status_code, resp.json())
        self.assertEqual([bogus_uuid], absent_uuids(resp))

        # Partial results with mixed valid, invalid job uuids
        resp = util.query_groups(self.cook_url, uuid=[group_uuid_1, group_uuid_2, bogus_uuid], partial='true')
        self.assertEqual(200, resp.status_code, resp.json())
        self.assertEqual(2, len(resp.json()))
        self.assertEqual([group_uuid_1, group_uuid_2].sort(), [group['uuid'] for group in resp.json()].sort())

    def test_detailed_for_groups(self):
        detail_keys = ('waiting', 'running', 'completed')

        group_uuid = str(uuid.uuid4())
        _, resp = util.submit_job(self.cook_url, group=group_uuid)
        self.assertEqual(201, resp.status_code)

        # Cases with no details (default or detailed=false)
        for params in ({}, {'detailed': 'false'}):
            resp = util.query_groups(self.cook_url, uuid=[group_uuid], **params)
            self.assertEqual(200, resp.status_code)
            group_info = resp.json()[0]
            for k in detail_keys:
                self.assertNotIn(k, group_info)

        # Case with details (detailed=true)
        resp = util.query_groups(self.cook_url, uuid=[group_uuid], detailed='true')
        self.assertEqual(200, resp.status_code)
        group_info = resp.json()[0]
        for k in detail_keys:
            self.assertIn(k, group_info)

    def test_group_kill_simple(self):
        # Create and submit jobs in group
        slow_job_wait_seconds = 1200
        group_spec = util.minimal_group()
        group_uuid = group_spec['uuid']
        try:
            job_fast = util.minimal_job(group=group_uuid, priority=99)
            job_slow = util.minimal_job(group=group_uuid, command=f'sleep {slow_job_wait_seconds}')
            _, resp = util.submit_jobs(self.cook_url, [job_fast, job_slow], groups=[group_spec])
            self.assertEqual(resp.status_code, 201, resp.text)
            # Wait for the fast job to finish, and the slow job to start
            util.wait_for_job(self.cook_url, job_fast, 'completed')
            util.wait_for_job(self.cook_url, job_slow, 'running')
            # Now try to cancel the group (just the long job)
            util.kill_groups(self.cook_url, [group_uuid])

            # Wait for the slow job (and its instance) to die
            def query():
                return util.query_jobs(self.cook_url, True, uuid=[job_slow])

            util.wait_until(query, util.all_instances_killed)
            # The fast job should have Success, slow job Failed (because we killed it)
            jobs = util.query_jobs(self.cook_url, True, uuid=[job_fast, job_slow]).json()
            self.assertEqual('success', jobs[0]['state'], f"Job details: {json.dumps(jobs[0], sort_keys=True)}")
            slow_job_details = f"Job details: {json.dumps(jobs[1], sort_keys=True)}"
            self.assertEqual('failed', jobs[1]['state'], slow_job_details)
            valid_reasons = [
                # cook killed the job, so it exits non-zero
                reasons.CMD_NON_ZERO_EXIT,
                # cook killed the job during setup, so the executor had an error
                reasons.EXECUTOR_UNREGISTERED]
            self.assertIn(jobs[1]['instances'][0]['reason_code'], valid_reasons, slow_job_details)
        finally:
            # Now try to kill the group again
            # (ensure it still works when there are no live jobs)
            util.kill_groups(self.cook_url, [group_uuid])

    def test_group_kill_multi(self):
        # Create and submit jobs in group
        slow_job_wait_seconds = 1200
        group_spec = util.minimal_group()
        group_uuid = group_spec['uuid']
        job_spec = {'group': group_uuid, 'command': f'sleep {slow_job_wait_seconds}'}
        try:
            jobs, resp = util.submit_jobs(self.cook_url, job_spec, 10, groups=[group_spec])
            self.assertEqual(resp.status_code, 201)

            # Wait for some job to start
            def some_job_started(group_response):
                group = group_response.json()[0]
                running_count = group['running']
                self.logger.info(f"Currently {running_count} jobs running in group {group_uuid}")
                return running_count > 0

            def group_detail_query():
                response = util.query_groups(self.cook_url, uuid=[group_uuid], detailed='true')
                self.assertEqual(200, response.status_code)
                return response

            util.wait_until(group_detail_query, some_job_started)
            # Now try to kill the whole group
            util.kill_groups(self.cook_url, [group_uuid])

            # Wait for all the jobs to die
            # Ensure that each job Failed (because we killed it)
            def query():
                return util.query_jobs(self.cook_url, True, uuid=jobs)

            util.wait_until(query, util.all_instances_killed)
        finally:
            # Now try to kill the group again
            # (ensure it still works when there are no live jobs)
            util.kill_groups(self.cook_url, [group_uuid])

    def test_group_change_killed_retries(self):
        jobs = util.group_submit_kill_retry(self.cook_url, retry_failed_jobs_only=False)
        # ensure none of the jobs are still in a failed state
        for job in jobs:
            self.assertNotEqual('failed', job['state'], f'Job details: {json.dumps(job, sort_keys=True)}')

    def test_group_change_killed_retries_failed_only(self):
        jobs = util.group_submit_kill_retry(self.cook_url, retry_failed_jobs_only=True)
        # ensure none of the jobs are still in a failed state
        for job in jobs:
            self.assertNotEqual('failed', job['state'], f'Job details: {json.dumps(job, sort_keys=True)}')

    def test_group_change_retries(self):
        group_spec = util.minimal_group()
        group_uuid = group_spec['uuid']
        job_spec = {'group': group_uuid, 'command': 'sleep 1'}

        def group_query():
            return util.group_detail_query(self.cook_url, group_uuid)

        try:
            jobs, resp = util.submit_jobs(self.cook_url, job_spec, 10, groups=[group_spec])
            self.assertEqual(resp.status_code, 201)
            # wait for some job to start
            util.wait_until(group_query, util.group_some_job_started)
            # When this wait condition succeeds, we expect at least one job to be completed,
            # a couple of jobs to be running (sleeping for 1 second should be long enough
            # to dominate any lag in the test code), and some still waiting.
            # If this is run in a larger mesos cluster with many jobs executing in parallel,
            # then all of the jobs may complete around the same time. However, we would
            # expect the behavior described above in our usual scaled-down testing environments
            # (i.e., Travis-CI VMs and minimesos using local docker instances).
            util.wait_until(group_query, util.group_some_job_done)
            job_data = util.query_jobs(self.cook_url, uuid=jobs).json()
            # retry all jobs in the group
            util.retry_jobs(self.cook_url, retries=12, groups=[group_uuid], failed_only=False)
            # wait for the previously-completed jobs to restart
            prev_completed_jobs = [j for j in job_data if j['status'] == 'completed']
            assert len(prev_completed_jobs) >= 1

            def jobs_query():
                return util.query_jobs(self.cook_url, True, uuid=prev_completed_jobs)

            def all_completed_restarted(response):
                for job in response.json():
                    instance_count = len(job['instances'])
                    if job['status'] == 'completed' and instance_count < 2:
                        self.logger.debug(f"Completed job {job['uuid']} has fewer than 2 instances: {instance_count}")
                        return False
                return True

            util.wait_until(jobs_query, all_completed_restarted)
            # ensure that all of the jobs have an updated retries count (set to 12 above)
            job_data = util.query_jobs(self.cook_url, uuid=jobs)
            self.assertEqual(200, job_data.status_code)
            for job in job_data.json():
                job_details = f'Job details: {json.dumps(job, sort_keys=True)}'
                # Jobs that ran twice will have 10 retries remaining,
                # Jobs that ran once will have 11 retries remaining.
                # Jobs that were running during the reset and are still running
                # will still have all 12 retries remaining.
                self.assertIn(job['retries_remaining'], [10, 11, 12], job_details)
        finally:
            # ensure that we don't leave a bunch of jobs running/waiting
            util.kill_groups(self.cook_url, [group_uuid])

    def test_group_failed_only_change_retries_all_active(self):
        statuses = ['running', 'waiting']
        jobs = util.group_submit_retry(self.cook_url, command='sleep 10', predicate_statuses=statuses)
        for job in jobs:
            job_details = f'Job details: {json.dumps(job, sort_keys=True)}'
            self.assertIn(job['status'], statuses, job_details)
            self.assertEqual(job['retries_remaining'], 1, job_details)
            self.assertLessEqual(len(job['instances']), 1, job_details)

    def test_group_failed_only_change_retries_all_success(self):
        statuses = ['completed']
        jobs = util.group_submit_retry(self.cook_url, command='exit 0', predicate_statuses=statuses)
        for job in jobs:
            job_details = f'Job details: {json.dumps(job, sort_keys=True)}'
            self.assertIn(job['status'], statuses, job_details)
            self.assertEqual(job['retries_remaining'], 0, job_details)
            self.assertEqual(len(job['instances']), 1, job_details)

    def test_group_failed_only_change_retries_all_failed(self):
        statuses = ['completed']
        jobs = util.group_submit_retry(self.cook_url, command='exit 1', predicate_statuses=statuses)
        for job in jobs:
            job_details = f'Job details: {json.dumps(job, sort_keys=True)}'
            self.assertIn(job['status'], statuses, job_details)
            self.assertEqual(job['retries_remaining'], 0, job_details)
            self.assertEqual(len(job['instances']), 2, job_details)

    def test_400_on_group_query_without_uuid(self):
        resp = util.query_groups(self.cook_url)
        self.assertEqual(400, resp.status_code)

    def test_queue_endpoint(self):
        constraints = [["HOSTNAME", "EQUALS", "lol won't get scheduled"]]
        group = {'uuid': str(uuid.uuid4())}
        job_spec = {'group': group['uuid'],
                    'constraints': constraints}
        uuids, resp = util.submit_jobs(self.cook_url, job_spec, 1, groups=[group])
        job_uuid = uuids[0]
        try:
            self.assertEqual(201, resp.status_code, resp.content)

            def query_queue():
                return util.session.get('%s/queue' % self.cook_url)

            def queue_predicate(resp):
                return any([job['job/uuid'] == job_uuid for job in resp.json()['normal']])

            resp = util.wait_until(query_queue, queue_predicate)
            self.assertEqual(200, resp.status_code, resp.content)
            job = [job for job in resp.json()['normal']
                   if job['job/uuid'] == job_uuid][0]
            self.assertTrue('group/_job' in job.keys())
            job_group = job['group/_job'][0]
            self.assertEqual(group['uuid'], job_group['group/uuid'])
            self.assertTrue('group/host-placement' in job_group.keys())
            self.assertFalse('group/job' in job_group.keys())
        finally:
            util.kill_jobs(self.cook_url, [job_uuid])

    def test_basic_docker_job(self):
        job_uuid, resp = util.submit_job(
            self.cook_url,
            name="check_alpine_version",
            command="cat /etc/alpine-release",
            container={"type": "DOCKER",
                       "docker": {'image': "alpine:latest"}})
        self.assertEqual(resp.status_code, 201)
        job = util.wait_for_job(self.cook_url, job_uuid, 'completed')
        self.assertEqual('success', job['instances'][0]['status'])

    def test_docker_port_mapping(self):
        job_uuid, resp = util.submit_job(self.cook_url,
                                         command='python -m http.server 8080',
                                         ports=2,
                                         container={'type': 'DOCKER',
                                                    'docker': {'image': 'python:3.6',
                                                               'network': 'BRIDGE',
                                                               'port-mapping': [{'host-port': 0,  # first assigned port
                                                                                 'container-port': 8080},
                                                                                {'host-port': 1,  # second assigned port
                                                                                 'container-port': 9090,
                                                                                 'protocol': 'udp'}]}})
        self.assertEqual(resp.status_code, 201, resp.content)
        try:
            job = util.wait_for_job(self.cook_url, job_uuid, 'running')
            instance = job['instances'][0]
            self.logger.debug('instance: %s' % instance)

            # Get agent host/port
            state = util.get_mesos_state(self.mesos_url)
            agent = [agent for agent in state['slaves']
                     if agent['hostname'] == instance['hostname']][0]
            if agent is None:
                self.logger.warning(f"Could not find agent for hostname {instance['hostname']}")
                self.logger.warning(f"slaves: {state['slaves']}")
            # Get the host and port of the agent API.
            # Example pid: "slave(1)@172.17.0.7:5051"
            agent_hostport = agent['pid'].split('@')[1]

            # Get container ID from agent
            def agent_query():
                return util.session.get('http://%s/state.json' % agent_hostport)

            def contains_executor_predicate(agent_response):
                agent_state = agent_response.json()
                executor = util.get_executor(agent_state, instance['executor_id'])
                if executor is None:
                    self.logger.warning(f"Could not find executor {instance['executor_id']} in agent state")
                    self.logger.warning(f"agent_state: {agent_state}")
                return executor is not None

            agent_state = util.wait_until(agent_query, contains_executor_predicate).json()
            executor = util.get_executor(agent_state, instance['executor_id'])

            container_id = 'mesos-%s.%s' % (agent['id'], executor['container'])
            self.logger.debug('container_id: %s' % container_id)

            @retry(stop_max_delay=60000, wait_fixed=1000)  # Wait for docker container to start
            def get_docker_info():
                self.logger.info('Running containers: %s' % subprocess.check_output(['docker', 'ps']).decode('utf-8'))
                return json.loads(subprocess.check_output(['docker', 'inspect', container_id]).decode('utf-8'))

            docker_info = get_docker_info()
            ports = docker_info[0]['HostConfig']['PortBindings']
            self.logger.debug('ports: %s' % ports)
            self.assertTrue('8080/tcp' in ports)
            self.assertEqual(instance['ports'][0], int(ports['8080/tcp'][0]['HostPort']))
            self.assertTrue('9090/udp' in ports)
            self.assertEqual(instance['ports'][1], int(ports['9090/udp'][0]['HostPort']))
        finally:
            util.session.delete('%s/rawscheduler?job=%s' % (self.cook_url, job_uuid))

    def test_unscheduled_jobs(self):
        unsatisfiable_constraint = ['HOSTNAME', 'EQUALS', 'fakehost']
        job_uuid_1, resp = util.submit_job(self.cook_url, command='ls', constraints=[unsatisfiable_constraint])
        self.assertEqual(resp.status_code, 201, resp.content)
        job_uuid_2, resp = util.submit_job(self.cook_url, command='ls', constraints=[unsatisfiable_constraint])
        self.assertEqual(resp.status_code, 201, resp.content)
        try:
            jobs, _ = util.unscheduled_jobs(self.cook_url, job_uuid_1, job_uuid_2)
            self.logger.info(f'Unscheduled jobs: {jobs}')
            # If the job from the test is submitted after another one, unscheduled_jobs will report "There are jobs
            # ahead of this in the queue" so we cannot assert that there is exactly one failure reason.
            self.assertTrue(any([reasons.UNDER_INVESTIGATION == reason['reason'] for reason in jobs[0]['reasons']]))
            self.assertTrue(any([reasons.UNDER_INVESTIGATION == reason['reason'] for reason in jobs[1]['reasons']]))
            self.assertEqual(job_uuid_1, jobs[0]['uuid'])
            self.assertEqual(job_uuid_2, jobs[1]['uuid'])

            @retry(stop_max_delay=60000, wait_fixed=1000)
            def check_unscheduled_reason():
                jobs, _ = util.unscheduled_jobs(self.cook_url, job_uuid_1, job_uuid_2)
                self.logger.info(f'Unscheduled jobs: {jobs}')
                # If the job from the test is submitted after another one, unscheduled_jobs will report "There are
                # jobs ahead of this in the queue" so we cannot assert that there is exactly one failure reason.
                self.assertTrue(any([reasons.COULD_NOT_PLACE_JOB == reason['reason'] for reason in jobs[0]['reasons']]))
                self.assertTrue(any([reasons.COULD_NOT_PLACE_JOB == reason['reason'] for reason in jobs[1]['reasons']]))
                self.assertEqual(job_uuid_1, jobs[0]['uuid'])
                self.assertEqual(job_uuid_2, jobs[1]['uuid'])

            check_unscheduled_reason()
        finally:
            util.kill_jobs(self.cook_url, [job_uuid_1, job_uuid_2])

    def test_unscheduled_jobs_partial(self):
        unsatisfiable_constraint = ['HOSTNAME', 'EQUALS', 'fakehost']
        job_uuid_1, resp = util.submit_job(self.cook_url, command='ls', constraints=[unsatisfiable_constraint])
        self.assertEqual(resp.status_code, 201, resp.content)
        try:
            job_uuid_2 = uuid.uuid4()
            _, resp = util.unscheduled_jobs(self.cook_url, job_uuid_1, job_uuid_2, partial=None)
            self.assertEqual(404, resp.status_code)
            _, resp = util.unscheduled_jobs(self.cook_url, job_uuid_1, job_uuid_2, partial='false')
            self.assertEqual(404, resp.status_code)
            jobs, resp = util.unscheduled_jobs(self.cook_url, job_uuid_1, job_uuid_2, partial='true')
            self.assertEqual(200, resp.status_code)
            self.assertEqual(1, len(jobs))
            self.assertEqual(job_uuid_1, jobs[0]['uuid'])
        finally:
            util.kill_jobs(self.cook_url, [job_uuid_1])

    def test_unique_host_constraint(self):
        state = util.get_mesos_state(self.mesos_url)
        num_hosts = len(state['slaves'])
        group = {'uuid': str(uuid.uuid4()),
                 'host-placement': {'type': 'unique'}}
        job_spec = {'group': group['uuid'], 'command': 'sleep 600'}
        # Don't submit too many jobs for the test. If the cluster is larger than 19 hosts, only submit 20 jobs.
        num_jobs = min(num_hosts + 1, 20)
        uuids, resp = util.submit_jobs(self.cook_url, job_spec, num_jobs, groups=[group])
        self.assertEqual(resp.status_code, 201, resp.content)
        try:
            def query():
                return util.query_jobs(self.cook_url, uuid=uuids).json()

            def num_running_predicate(response):
                num_jobs_total = len(response)
                num_running = len([j for j in response if j['status'] == 'running'])
                num_waiting = len([j for j in response if j['status'] == 'waiting'])
                if num_jobs_total == num_hosts + 1:
                    # One job should not be scheduled
                    return (num_running == num_jobs_total - 1) and (num_waiting == 1)
                else:
                    # All of the jobs should be running
                    return num_running == num_jobs_total

            jobs = util.wait_until(query, num_running_predicate, max_wait_ms=60000)
            hosts = [job['instances'][0]['hostname'] for job in jobs
                     if job['status'] == 'running']
            # Only one job should run on each host
            self.assertEqual(len(set(hosts)), len(hosts))
            # If one job was not running, check the output of unscheduled_jobs
            if num_jobs == num_hosts + 1:
                waiting_jobs = [j for j in jobs if j['status'] == 'waiting']
                self.assertEqual(1, len(waiting_jobs))
                waiting_job = waiting_jobs[0]

                def query():
                    unscheduled = util.unscheduled_jobs(self.cook_url, waiting_job['uuid'])[0][0]
                    self.logger.info(f"unscheduled_jobs response: {unscheduled}")
                    return unscheduled

                def check_unique_constraint(response):
                    self.logger.debug('unscheduled_jobs response: %s' % response)
                    return any([r['reason'] == reasons.COULD_NOT_PLACE_JOB for r in response['reasons']])

                unscheduled_jobs = util.wait_until(query, check_unique_constraint)
                unique_reason = [r for r in unscheduled_jobs['reasons'] if r['reason'] ==
                                 reasons.COULD_NOT_PLACE_JOB][0]
                self.assertEqual("unique-host-placement-group-constraint",
                                 unique_reason['data']['reasons'][0]['reason'],
                                 unique_reason)
        finally:
            util.kill_jobs(self.cook_url, uuids)

    def test_balanced_host_constraint_cannot_place(self):
        state = util.get_mesos_state(self.mesos_url)
        num_hosts = len(state['slaves'])
        if num_hosts > 10:
            # Skip this test on large clusters
            self.logger.info(f"Skipping test due to cluster size of {num_hosts} greater than 10")
            return
        minimum_hosts = num_hosts + 1
        group = {'uuid': str(uuid.uuid4()),
                 'host-placement': {'type': 'balanced',
                                    'parameters': {'attribute': 'HOSTNAME',
                                                   'minimum': minimum_hosts}}}
        job_spec = {'command': 'sleep 600',
                    'cpus': 0.1,
                    'group': group['uuid'],
                    'mem': 100}
        num_jobs = minimum_hosts
        uuids, resp = util.submit_jobs(self.cook_url, job_spec, num_jobs, groups=[group])
        try:
            def query_list():
                return util.query_jobs(self.cook_url, uuid=uuids).json()

            def num_running_predicate(response):
                num_running = len([j for j in response if j['status'] == 'running'])
                num_waiting = len([j for j in response if j['status'] == 'waiting'])
                return num_running == num_hosts and num_waiting == 1

            jobs = util.wait_until(query_list, num_running_predicate)
            waiting_jobs = [j for j in jobs if j['status'] == 'waiting']
            self.assertEqual(1, len(waiting_jobs), waiting_jobs)
            waiting_job = waiting_jobs[0]

            def query_unscheduled():
                resp = util.unscheduled_jobs(self.cook_url, waiting_job['uuid'])[0][0]
                placement_reasons = [reason for reason in resp['reasons']
                                     if reason['reason'] == reasons.COULD_NOT_PLACE_JOB]
                self.logger.info(f"unscheduled_jobs response: {resp}")
                return placement_reasons

            placement_reasons = util.wait_until(query_unscheduled, lambda r: len(r) > 0)
            self.assertEqual(1, len(placement_reasons), placement_reasons)
            reason = placement_reasons[0]
            balanced_reasons = [r for r in reason['data']['reasons']
                                if r['reason'] == 'balanced-host-placement-group-constraint']
            self.assertEqual(1, len(balanced_reasons), balanced_reasons)
        finally:
            util.kill_jobs(self.cook_url, uuids)

    def test_balanced_host_constraint(self):
        state = util.get_mesos_state(self.mesos_url)
        num_hosts = len(state['slaves'])
        minimum_hosts = min(10, num_hosts)
        group = {'uuid': str(uuid.uuid4()),
                 'host-placement': {'type': 'balanced',
                                    'parameters': {'attribute': 'HOSTNAME',
                                                   'minimum': minimum_hosts}}}
        job_spec = {'group': group['uuid'],
                    'command': 'sleep 600',
                    'mem': 100,
                    'cpus': 0.1}
        max_jobs_per_host = 3
        num_jobs = minimum_hosts * max_jobs_per_host
        uuids, resp = util.submit_jobs(self.cook_url, job_spec, num_jobs, groups=[group])
        self.assertEqual(201, resp.status_code, resp.content)
        try:
            jobs = util.wait_for_jobs(self.cook_url, uuids, 'running')
            hosts = [j['instances'][0]['hostname'] for j in jobs]
            host_count = Counter(hosts)
            self.assertGreaterEqual(len(host_count), minimum_hosts, hosts)
            self.assertLessEqual(max(host_count.values()), max_jobs_per_host, host_count)
        finally:
            util.kill_jobs(self.cook_url, uuids)

    def test_attribute_equals_hostname_constraint(self):
        slaves = util.get_mesos_state(self.mesos_url)['slaves']
        max_slave_cpus = max([s['resources']['cpus'] for s in slaves])
        task_constraint_cpus = util.settings(self.cook_url)['task-constraints']['cpus']
        # The largest job we can submit that actually fits on a slave
        max_cpus = min(max_slave_cpus, task_constraint_cpus)
        # The number of "big" jobs we need to submit before one will not be scheduled
        num_big_jobs = max(1, math.floor(max_cpus / task_constraint_cpus))
        # Use the rest of the machine, plus one half CPU so one of the large jobs won't fit
        canary_cpus = max_slave_cpus - (num_big_jobs * max_cpus) + 0.5
        group = {'uuid': str(uuid.uuid4()),
                 'host-placement': {'type': 'attribute-equals',
                                    'parameters': {'attribute': 'HOSTNAME'}}}
        # First job - a canary job with high priority which uses canary_cpus cpus
        canary = util.minimal_job(group=group['uuid'],
                                  priority=100,
                                  cpus=canary_cpus,
                                  command='sleep 600')
        # Second job - a big job with low priority using max_cpus cpus
        # Based on the priority, canary should be scheduled ahead of big_job, but because
        # of the host-placement constraint they have to be scheduled on the same host.
        # The canary should be able to be scheduled, but big_job should never be scheduled.
        jobs = [util.minimal_job(group=group['uuid'],
                                 priority=1,
                                 cpus=max_cpus)
                for _ in range(num_big_jobs)]
        jobs.append(canary)
        uuids, resp = util.submit_jobs(self.cook_url, jobs, groups=[group])
        self.assertEqual(201, resp.status_code, resp.content)
        try:
            util.wait_for_job(self.cook_url, canary['uuid'], 'running')

            def query():
                unscheduled_jobs, _ = util.unscheduled_jobs(self.cook_url, *[j['uuid'] for j in jobs])
                self.logger.info(f"unscheduled_jobs response: {unscheduled_jobs}")
                no_hosts = [reason for job in unscheduled_jobs for reason in job['reasons']
                            if reason['reason'] == "The job couldn't be placed on any available hosts."]
                for no_hosts_reason in no_hosts:
                    for sub_reason in no_hosts_reason['data']['reasons']:
                        if sub_reason['reason'] == "Host had a different attribute than other jobs in the group.":
                            return sub_reason
                return None

            reason = util.wait_until(query, lambda r: r is not None)
            self.assertEqual(reason['reason'],
                             "Host had a different attribute than other jobs in the group.")
        finally:
            util.kill_jobs(self.cook_url, uuids)
