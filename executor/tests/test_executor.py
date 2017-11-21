import collections
import json
import logging
import os
import signal
import subprocess
import time
import unittest
from threading import Event, Thread

from nose.tools import *
from pymesos import encode_data

import cook
import cook.config as cc
import cook.executor as ce
import cook.io_helper as cio
from tests.utils import assert_message, assert_status, cleanup_output, close_sys_outputs, ensure_directory, \
    get_random_task_id, parse_message, redirect_stderr_to_file, redirect_stdout_to_file, wait_for, \
    FakeMesosExecutorDriver


def find_process_ids_in_group(group_id):
    group_id_to_process_ids = collections.defaultdict(set)
    process_id_to_command = collections.defaultdict(lambda: '')

    p = subprocess.Popen('ps -eo pid,pgid,command',
                         close_fds=True, shell=True,
                         stderr=subprocess.STDOUT, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    ps_output = p.stdout.read().decode('utf8')

    for line in ps_output.splitlines():
        line_split = line.split()
        pid = line_split[0]
        pgid = line_split[1]
        command = str.join(' ', line_split[2:])
        group_id_to_process_ids[pgid].add(pid)
        process_id_to_command[pid] = command

    group_id_str = str(group_id)
    logging.info("group_id_to_process_ids: {}".format(group_id_to_process_ids))
    logging.info("group_id_to_process_ids[{}]: {}".format(group_id, group_id_to_process_ids[group_id_str]))
    for pid in group_id_to_process_ids[group_id_str]:
        logging.info("process (pid: {}) command is {}".format(pid, process_id_to_command[pid]))
    return group_id_to_process_ids[group_id_str]


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
        assert_status(self, expected_status, actual_status)

    def test_update_status(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        task_state = "TEST_TASK_STATE"

        ce.update_status(driver, task_id, task_state)

        self.assertEqual(1, len(driver.statuses))
        actual_status = driver.statuses[0]
        expected_status = {'task_id': {'value': task_id},
                           'state': task_state}
        assert_status(self, expected_status, actual_status)

    def test_send_message(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        expected_message = {'task-id': task_id, 'message': 'test-message'}
        message = json.dumps(expected_message)
        max_message_length = 512

        result = ce.send_message(driver, message, max_message_length)

        self.assertTrue(result)
        self.assertEqual(1, len(driver.messages))
        actual_encoded_message = driver.messages[0]
        assert_message(self, expected_message, actual_encoded_message)

    def test_send_message_max_length_exceeded(self):
        driver = object()
        task_id = get_random_task_id()
        message = json.dumps({'task-id': task_id, 'message': 'test-message'})
        max_message_length = 1

        result = ce.send_message(driver, message, max_message_length)
        self.assertFalse(result)

    def test_progress_group_assignment_and_killing(self):

        start_time = time.time()

        task_id = get_random_task_id()
        command = 'echo "A.$(sleep 30)" & echo "B.$(sleep 30)" & echo "C.$(sleep 30)" &'
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': command}).encode('utf8'))}
        environment = {}
        process = ce.launch_task(task, environment)

        group_id = ce.find_process_group(process.pid)
        self.assertGreater(group_id, 0)

        child_process_ids = wait_for(lambda: find_process_ids_in_group(group_id),
                                     lambda data: len(data) >= 7,
                                     default_value=[])
        self.assertGreaterEqual(len(child_process_ids), 7)
        self.assertLessEqual(len(child_process_ids), 10)

        ce.kill_process_group(group_id)

        child_process_ids = wait_for(lambda: find_process_ids_in_group(group_id),
                                     lambda data: len(data) == 1,
                                     default_value=[])
        self.assertEqual(len(child_process_ids), 1)

        self.assertLess(time.time() - start_time, 30)

    def test_launch_task(self):
        task_id = get_random_task_id()
        command = 'echo "Hello World"; echo "Error Message" >&2'
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            max_bytes_read_per_line = 4096
            process = ce.launch_task(task, os.environ)
            stdout_thread, stderr_thread = cio.track_outputs(task_id, process, max_bytes_read_per_line)
            process_info = process, stdout_thread, stderr_thread

            self.assertIsNotNone(process)

            for _ in range(100):
                if ce.is_running(process_info):
                    time.sleep(0.01)

            if process.poll() is None:
                process.kill()
            close_sys_outputs()

            self.assertEqual(0, process.poll())

            with open(stdout_name) as f:
                stdout_content = f.read()
                self.assertTrue("Hello World\n" in stdout_content)

            with open(stderr_name) as f:
                stderr_content = f.read()
                self.assertTrue("Error Message\n" in stderr_content)
        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_launch_task_no_command(self):
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': ''}).encode('utf8'))}

        process = ce.launch_task(task, os.environ)

        self.assertIsNone(process)

    def test_launch_task_handle_exception(self):
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id}}

        process = ce.launch_task(task, os.environ)

        self.assertIsNone(process)

    def test_kill_task_terminate(self):
        task_id = get_random_task_id()

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            command = 'sleep 100'
            process = subprocess.Popen(command, shell=True)
            shutdown_grace_period_ms = 2000
            ce.kill_task(process, shutdown_grace_period_ms)

            # await process termination
            while process.poll() is None:
                time.sleep(0.01)
            self.assertEqual(-1 * signal.SIGTERM, process.poll())

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_await_process_completion_normal(self):
        task_id = get_random_task_id()

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            command = 'sleep 2'
            process = subprocess.Popen(command, shell=True)
            stdout_thread = Thread()
            stderr_thread = Thread()
            process_info = process, stdout_thread, stderr_thread
            shutdown_grace_period_ms = 1000

            stop_signal = Event()

            ce.await_process_completion(stop_signal, process_info, shutdown_grace_period_ms)

            self.assertFalse(stop_signal.isSet())
            self.assertEqual(0, process.returncode)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_await_process_completion_killed(self):
        task_id = get_random_task_id()

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            command = 'sleep 100'
            process = subprocess.Popen(command, shell=True)
            stdout_thread = Thread()
            stderr_thread = Thread()
            process_info = process, stdout_thread, stderr_thread
            shutdown_grace_period_ms = 2000

            stop_signal = Event()

            def sleep_and_set_stop_signal():
                time.sleep(2 * cook.RUNNING_POLL_INTERVAL_SECS)
                stop_signal.set()
            thread = Thread(target=sleep_and_set_stop_signal, args=())
            thread.start()

            ce.await_process_completion(stop_signal, process_info, shutdown_grace_period_ms)

            self.assertTrue(process.returncode < 0)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_get_task_state(self):
        self.assertEqual(cook.TASK_FINISHED, ce.get_task_state(0))
        self.assertEqual(cook.TASK_FAILED, ce.get_task_state(1))
        self.assertEqual(cook.TASK_KILLED, ce.get_task_state(-1))

    def test_retrieve_process_environment(self):
        self.assertEqual({'EXECUTOR_PROGRESS_OUTPUT_FILE': 'stdout'},
                         ce.retrieve_process_environment(cc.ExecutorConfig(), {}))
        self.assertEqual({'CUSTOM_PROGRESS_OUTPUT_FILE': 'stdout',
                          'FOO': 'BAR',
                          'MESOS_SANDBOX': '/path/to/sandbox',
                          'PROGRESS_OUTPUT_FILE': 'executor.progress'},
                         ce.retrieve_process_environment(
                             cc.ExecutorConfig(progress_output_env_variable='CUSTOM_PROGRESS_OUTPUT_FILE'),
                             {'FOO': 'BAR',
                              'MESOS_SANDBOX': '/path/to/sandbox',
                              'PROGRESS_OUTPUT_FILE': 'executor.progress'}))
        self.assertEqual({'CUSTOM_PROGRESS_OUTPUT_FILE': 'custom.progress',
                          'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'CUSTOM_PROGRESS_OUTPUT_FILE'},
                         ce.retrieve_process_environment(
                             cc.ExecutorConfig(progress_output_env_variable='CUSTOM_PROGRESS_OUTPUT_FILE',
                                               progress_output_name='custom.progress'),
                             {'CUSTOM_PROGRESS_OUTPUT_FILE': 'executor.progress',
                              'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'CUSTOM_PROGRESS_OUTPUT_FILE'}))
        self.assertEqual({'CUSTOM_PROGRESS_OUTPUT_FILE': 'custom.progress',
                          'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'CUSTOM_PROGRESS_OUTPUT_FILE',
                          'PROGRESS_OUTPUT_FILE': 'stdout'},
                         ce.retrieve_process_environment(
                             cc.ExecutorConfig(progress_output_env_variable='CUSTOM_PROGRESS_OUTPUT_FILE',
                                               progress_output_name='custom.progress'),
                             {'CUSTOM_PROGRESS_OUTPUT_FILE': 'executor.progress',
                              'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'CUSTOM_PROGRESS_OUTPUT_FILE',
                              'PROGRESS_OUTPUT_FILE': 'stdout'}))

    def manage_task_runner(self, command, assertions_fn, stop_signal=Event()):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        completed_signal = Event()
        max_message_length = 300
        progress_sample_interval_ms = 100
        sandbox_directory = '/location/to/task/sandbox/{}'.format(task_id)
        progress_output_name = stdout_name
        progress_regex_string = '\^\^\^\^JOB-PROGRESS: (\d*)(?: )?(.*)'
        config = cc.ExecutorConfig(max_message_length=max_message_length,
                                   progress_output_name=progress_output_name,
                                   progress_regex_string=progress_regex_string,
                                   progress_sample_interval_ms=progress_sample_interval_ms,
                                   sandbox_directory=sandbox_directory)

        try:

            ce.manage_task(driver, task, stop_signal, completed_signal, config)

            self.assertTrue(completed_signal.isSet())
            assertions_fn(driver, task_id, sandbox_directory)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_manage_task_environment_output(self):
        def assertions(driver, task_id, _):
            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(2, len(driver.messages))

            stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
            with open(stdout_name) as f:
                file_contents = f.read()
                self.assertTrue('FEE=FIE' in file_contents)
                self.assertTrue('FOO=BAR' in file_contents)
                self.assertTrue('PROGRESS_OUTPUT_FILE=foobar' in file_contents)

        command = 'env | sort'
        current_environ = os.environ
        try:
            os.environ = {'FOO': 'BAR', 'FEE': 'FIE', 'PROGRESS_OUTPUT_FILE': 'foobar'}
            self.manage_task_runner(command, assertions)
        finally:
            os.environ = current_environ

    def test_manage_task_successful_exit(self):
        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_STARTING}
            assert_status(self, expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(self, expected_status_1, actual_status_1)

            actual_status_2 = driver.statuses[2]
            expected_status_2 = {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}
            assert_status(self, expected_status_2, actual_status_2)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            assert_message(self, expected_message_1, actual_encoded_message_1)

        command = 'echo "Hello World"'
        self.manage_task_runner(command, assertions)

    def test_manage_task_empty_command(self):
        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(2, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_STARTING}
            assert_status(self, expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_ERROR}
            assert_status(self, expected_status_1, actual_status_1)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(1, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

        command = ''
        self.manage_task_runner(command, assertions)

    def test_manage_task_involved_command_successful_exit(self):
        def assertions(driver, task_id, sandbox_directory):
            
            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_STARTING}
            assert_status(self, expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(self, expected_status_1, actual_status_1)

            actual_status_2 = driver.statuses[2]
            expected_status_2 = {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}
            assert_status(self, expected_status_2, actual_status_2)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(3, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            expected_exit_messages = [{'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'line count is 20',
                                           'progress-percent': 90,
                                           'progress-sequence': 1,
                                           'task-id': task_id}]

            for i in range(1, len(driver.messages)):
                actual_message = parse_message(driver.messages[i])
                if 'exit-code' in actual_message and len(expected_exit_messages) > 0:
                    expected_message = expected_exit_messages.pop(0)
                    self.assertEquals(expected_message, actual_message)
                elif 'progress-sequence' in actual_message and len(expected_progress_messages) > 0:
                    expected_message = expected_progress_messages.pop(0)
                    self.assertEquals(expected_message, actual_message)
                else:
                    self.assertTrue(False, 'Unexpected message: {}'.format(actual_message))

        test_file_name = ensure_directory('build/file.' + get_random_task_id())
        command = ('mkdir -p build; touch {0}; for i in $(seq 20); do echo $i >> {0}; done; '
                   'LINE_COUNT=`wc -l < {0} | tr -d \'[:space:]\'`; cat  {0}; rm -rfv {0}; '
                   'echo "^^^^JOB-PROGRESS: 90 line count is $LINE_COUNT"'.format(test_file_name))
        self.manage_task_runner(command, assertions)

    def test_manage_task_successful_exit_with_progress_message(self):
        def assertions(driver, task_id, sandbox_directory):
            
            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_STARTING}
            assert_status(self, expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(self, expected_status_1, actual_status_1)

            actual_status_2 = driver.statuses[2]
            expected_status_2 = {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}
            assert_status(self, expected_status_2, actual_status_2)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(4, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            expected_exit_messages = [{'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'Fifty percent',
                                           'progress-percent': 50,
                                           'progress-sequence': 1,
                                           'task-id': task_id},
                                          {'progress-message': 'Fifty-five percent',
                                           'progress-percent': 55,
                                           'progress-sequence': 2,
                                           'task-id': task_id}]

            for i in range(1, len(driver.messages)):
                actual_message = parse_message(driver.messages[i])
                if 'exit-code' in actual_message and len(expected_exit_messages) > 0:
                    expected_message = expected_exit_messages.pop(0)
                    self.assertEquals(expected_message, actual_message)
                elif 'progress-sequence' in actual_message and len(expected_progress_messages) > 0:
                    expected_message = expected_progress_messages.pop(0)
                    self.assertEquals(expected_message, actual_message)
                else:
                    self.fail('Unexpected message: {}'.format(actual_message))

        command = 'echo "Hello World"; ' \
                  'echo "^^^^JOB-PROGRESS: 50 Fifty percent"; ' \
                  'sleep 1; ' \
                  'echo "^^^^JOB-PROGRESS: 55 Fifty-five percent"; ' \
                  'sleep 1; ' \
                  'echo "Exiting..."; ' \
                  'exit 0'
        self.manage_task_runner(command, assertions)

    def test_manage_task_erroneous_exit(self):
        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_STARTING}
            assert_status(self, expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(self, expected_status_1, actual_status_1)

            actual_status_2 = driver.statuses[2]
            expected_status_2 = {'task_id': {'value': task_id}, 'state': cook.TASK_FAILED}
            assert_status(self, expected_status_2, actual_status_2)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': 1, 'task-id': task_id}
            assert_message(self, expected_message_1, actual_encoded_message_1)

        command = 'echo "Hello World"; exit 1'
        self.manage_task_runner(command, assertions)

    def test_manage_task_terminated(self):
        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            actual_status_0 = driver.statuses[0]
            expected_status_0 = {'task_id': {'value': task_id}, 'state': cook.TASK_STARTING}
            assert_status(self, expected_status_0, actual_status_0)

            actual_status_1 = driver.statuses[1]
            expected_status_1 = {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING}
            assert_status(self, expected_status_1, actual_status_1)

            actual_status_2 = driver.statuses[2]
            expected_status_2 = {'task_id': {'value': task_id}, 'state': cook.TASK_KILLED}
            assert_status(self, expected_status_2, actual_status_2)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': -15, 'task-id': task_id}
            assert_message(self, expected_message_1, actual_encoded_message_1)

        stop_signal = Event()

        def sleep_and_set_stop_signal():
            time.sleep(2 * cook.RUNNING_POLL_INTERVAL_SECS)
            stop_signal.set()
        thread = Thread(target=sleep_and_set_stop_signal, args=())
        thread.start()

        command = 'sleep 100'
        self.manage_task_runner(command, assertions, stop_signal=stop_signal)

    def test_manage_task_long_output(self):
        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            assert_message(self, expected_message_1, actual_encoded_message_1)

            stdout_name = ensure_directory('build/stdout.' + str(task_id))
            if os.path.isfile(stdout_name):
                with open(stdout_name) as f:
                    file_contents = f.read()
                    expected_string = 'Hello' * 1000000
                    self.assertTrue(expected_string in file_contents)
            else:
                self.fail('{} does not exist.'.format(stdout_name))


        stop_signal = Event()

        def sleep_and_set_stop_signal():
            # wait upto 1 minute for the task to complete
            for _ in range(60):
                if not stop_signal.isSet():
                    time.sleep(1)
            stop_signal.set()
        thread = Thread(target=sleep_and_set_stop_signal, args=())
        thread.start()

        command = 'for i in `seq 1000000`; do printf "Hello"; done; echo "World"'
        self.manage_task_runner(command, assertions, stop_signal=stop_signal)
        stop_signal.set()
