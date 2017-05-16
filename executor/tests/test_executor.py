import json
import logging
import signal
import subprocess
import time
import unittest
from threading import Event

import os
from nose.tools import *
from pymesos import encode_data

import cook
import cook.config as cc
import cook.executor as ce
from tests.utils import assert_message, assert_status, cleanup_output, get_random_task_id, FakeMesosExecutorDriver


class ExecutorTest(unittest.TestCase):
    def test_get_task_id(self):
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id}}
        self.assertEqual(task_id, ce.get_task_id(task))

    def test_get_task_id_missing_value(self):
        with self.assertRaises(KeyError):
            ce.get_task_id({'task_id': {}})

    def test_get_task_id_empty_dictionary(self):
        with self.assertRaises(KeyError):
            ce.get_task_id({})

    def test_create_status_running(self):
        task_id = get_random_task_id()
        actual_status = ce.create_status(task_id, cook.TASK_RUNNING)
        expected_status = {'task_id': {'value': task_id},
                           'state': cook.TASK_RUNNING}
        assert_status(expected_status, actual_status)

    def test_update_status(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        task_state = "TEST_TASK_STATE"

        ce.update_status(driver, task_id, task_state)

        self.assertEqual(1, driver.method_call_count())
        self.assertEqual(1, len(driver.statuses))
        actual_status = driver.statuses[0]
        expected_status = {'task_id': {'value': task_id},
                           'state': task_state}
        assert_status(expected_status, actual_status)

    def test_send_message(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        expected_message = {'task-id': task_id, 'message': 'test-message'}
        message = json.dumps(expected_message)
        max_message_length = 512

        result = ce.send_message(driver, message, max_message_length)

        self.assertTrue(result)
        self.assertEqual(1, driver.method_call_count())
        self.assertEqual(1, len(driver.messages))
        actual_encoded_message = driver.messages[0]
        assert_message(expected_message, actual_encoded_message)

    def test_send_message_max_length_exceeded(self):
        driver = object()
        task_id = get_random_task_id()
        message = json.dumps({'task-id': task_id, 'message': 'test-message'})
        max_message_length = 1

        result = ce.send_message(driver, message, max_message_length)
        self.assertFalse(result)

    def test_launch_task(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        command = 'echo "Hello World"; echo "Error Message" >&2'
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': command}).encode('utf8'))}
        stdout_name = 'build/stdout.' + str(task_id)
        stderr_name = 'build/stderr.' + str(task_id)

        if not os.path.isdir("build"):
            os.mkdir("build")

        try:
            process, stdout, stderr = ce.launch_task(driver, task, stdout_name, stderr_name)

            self.assertIsNotNone(process)
            for i in range(100):
                if process.poll() is None:
                    time.sleep(0.01)

            stdout.close()
            stderr.close()

            if process.poll() is None:
                process.kill()

            self.assertEqual(1, driver.method_call_count())
            self.assertEqual(1, len(driver.statuses))
            actual_status = driver.statuses[0]
            expected_status = {'task_id': {'value': task_id},
                               'state': cook.TASK_RUNNING}
            assert_status(expected_status, actual_status)

            self.assertEqual(0, process.poll())

            with open(stdout_name) as f:
                stdout_content = f.read()
                self.assertEqual("Hello World\n", stdout_content)

            with open(stderr_name) as f:
                stderr_content = f.read()
                self.assertEqual("Error Message\n", stderr_content)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_launch_task_error(self):
        class FakeMesosExecutorDriver(object):
            def __init__(self):
                self.count_calls = 0
                self.statuses = []

            def sendStatusUpdate(self, status):
                self.count_calls += 1
                self.statuses.append(status)
                if self.count_calls == 1:
                    raise Exception('Thrown from test')

            def method_call_count(self):
                return self.count_calls

        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id}}
        stdout_name = None
        stderr_name = None

        result = ce.launch_task(driver, task, stdout_name, stderr_name)

        self.assertIsNone(result)
        self.assertEqual(2, driver.method_call_count())

        actual_status_0 = driver.statuses[0]
        expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
        assert_status(expected_status_0, actual_status_0)

        actual_status_1 = driver.statuses[1]
        expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_FAILED}
        assert_status(expected_status_1, actual_status_1)

    def test_cleanup_process(self):
        task_id = get_random_task_id()
        stdout_name = 'build/stdout.' + str(task_id)
        stderr_name = 'build/stderr.' + str(task_id)

        stdout = open(stdout_name, 'w+')
        stderr = open(stderr_name, 'w+')

        try:
            process_info = None, stdout, stderr
            ce.cleanup_process(process_info)

            self.assertTrue(stdout.closed)
            self.assertTrue(stderr.closed)
        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_kill_task_terminate(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id}}

        stdout_name = 'build/stdout.' + str(task_id)
        stderr_name = 'build/stderr.' + str(task_id)

        stdout = open(stdout_name, 'w+')
        stderr = open(stderr_name, 'w+')

        try:
            command = 'sleep 100'
            process = subprocess.Popen(command, shell=True, stdout=stdout, stderr=stderr)
            process_info = process, stdout, stderr
            shutdown_grace_period_ms = 2000
            ce.kill_task(driver, task, process_info, shutdown_grace_period_ms)

            # await process termination
            while process.poll() is None:
                time.sleep(0.01)
            self.assertEqual(-1 * signal.SIGTERM, process.poll())

            self.assertEqual(1, driver.method_call_count())
            self.assertEqual(1, len(driver.statuses))
            actual_status = driver.statuses[0]
            expected_status = {'task_id': {'value': task_id},
                               'state': cook.TASK_KILLED}
            assert_status(expected_status, actual_status)

            self.assertTrue(stdout.closed)
            self.assertTrue(stderr.closed)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_await_process_completion(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id}}

        stdout_name = 'build/stdout.' + str(task_id)
        stderr_name = 'build/stderr.' + str(task_id)

        stdout = open(stdout_name, 'w+')
        stderr = open(stderr_name, 'w+')

        try:
            command = 'sleep 100'
            process = subprocess.Popen(command, shell=True, stdout=stdout, stderr=stderr)
            process_info = process, stdout, stderr
            shutdown_grace_period_ms = 2000

            stop_signal = Event()
            stop_signal.set()

            result = ce.await_process_completion(driver, task, stop_signal, process_info, shutdown_grace_period_ms)

            self.assertTrue(result)

            self.assertEqual(1, driver.method_call_count())
            self.assertEqual(1, len(driver.statuses))
            actual_status = driver.statuses[0]
            expected_status = {'task_id': {'value': task_id},
                               'state': cook.TASK_KILLED}
            assert_status(expected_status, actual_status)

            self.assertTrue(stdout.closed)
            self.assertTrue(stderr.closed)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def manage_task_runner(self, command, assertions_fn):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = 'build/stdout.' + str(task_id)
        stderr_name = 'build/stderr.' + str(task_id)

        stop_signal = Event()
        completed_signal = Event()
        max_message_length = 300
        progress_sample_interval_ms = 100
        sandbox_location = '/location/to/task/sandbox/{}'.format(task_id)
        progress_output_name = stdout_name
        progress_regex_string = '\^\^\^\^JOB-PROGRESS: (\d*)(?: )?(.*)'
        config = cc.ExecutorConfig(max_message_length=max_message_length,
                                   progress_output_name=progress_output_name,
                                   progress_regex_string=progress_regex_string,
                                   progress_sample_interval_ms=progress_sample_interval_ms,
                                   sandbox_location=sandbox_location)

        try:

            ce.manage_task(driver, task, stop_signal, completed_signal, config, stdout_name, stderr_name)

            self.assertTrue(completed_signal.isSet())
            assertions_fn(driver, task_id, sandbox_location)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_manage_task_successful_exit(self):
        def assertions(driver, task_id, sandbox_location):
            logging.info('driver.method_call_count() = {}'.format(driver.method_call_count()))
            self.assertEqual(4, driver.method_call_count())

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(2, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}
            assert_status(expected_status_1, actual_status_1)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-location': sandbox_location, 'task-id': task_id}
            assert_message(expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            assert_message(expected_message_1, actual_encoded_message_1)

        command = 'echo "Hello World"'
        self.manage_task_runner(command, assertions)

    def test_manage_task_involved_command_successful_exit(self):
        def assertions(driver, task_id, sandbox_location):
            logging.info('driver.method_call_count() = {}'.format(driver.method_call_count()))
            self.assertEqual(5, driver.method_call_count())

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(2, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}
            assert_status(expected_status_1, actual_status_1)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(3, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-location': sandbox_location, 'task-id': task_id}
            assert_message(expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'progress-message': 'line count is 20', 'progress-percent': 90, 'task-id': task_id}
            assert_message(expected_message_1, actual_encoded_message_1)

            actual_encoded_message_2 = driver.messages[2]
            expected_message_2 = {'exit-code': 0, 'task-id': task_id}
            assert_message(expected_message_2, actual_encoded_message_2)

        test_file_name = 'build/file.' + get_random_task_id()
        command = ('mkdir -p build; touch {0}; for i in $(seq 20); do echo $i >> {0}; done; '
                   'LINE_COUNT=`wc -l < {0} | tr -d \'[:space:]\'`; cat  {0}; rm -rfv {0}; '
                   'echo "^^^^JOB-PROGRESS: 90 line count is $LINE_COUNT"'.format(test_file_name))
        self.manage_task_runner(command, assertions)

    def test_manage_task_successful_exit_with_progress_message(self):
        def assertions(driver, task_id, sandbox_location):
            self.assertEqual(6, driver.method_call_count())

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(2, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}
            assert_status(expected_status_1, actual_status_1)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(4, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-location': sandbox_location, 'task-id': task_id}
            assert_message(expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'progress-message': 'Fifty percent', 'progress-percent': 50, 'task-id': task_id}
            assert_message(expected_message_1, actual_encoded_message_1)

            actual_encoded_message_2 = driver.messages[2]
            expected_message_2 = {'progress-message': 'Fifty-five percent', 'progress-percent': 55, 'task-id': task_id}
            assert_message(expected_message_2, actual_encoded_message_2)

            actual_encoded_message_3 = driver.messages[3]
            expected_message_3 = {'exit-code': 0, 'task-id': task_id}
            assert_message(expected_message_3, actual_encoded_message_3)

        command = 'echo "Hello World"; ' \
                  'echo "^^^^JOB-PROGRESS: 50 Fifty percent"; ' \
                  'sleep 1; ' \
                  'echo "^^^^JOB-PROGRESS: 55 Fifty-five percent"; ' \
                  'sleep 1; ' \
                  'echo "Exiting..."; ' \
                  'exit 0'
        self.manage_task_runner(command, assertions)

    def test_manage_task_erroneous_exit(self):
        def assertions(driver, task_id, sandbox_location):
            self.assertEqual(4, driver.method_call_count())

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(2, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_FAILED}
            assert_status(expected_status_1, actual_status_1)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-location': sandbox_location, 'task-id': task_id}
            assert_message(expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': 1, 'task-id': task_id}
            assert_message(expected_message_1, actual_encoded_message_1)

        command = 'echo "Hello World"; exit 1'
        self.manage_task_runner(command, assertions)
