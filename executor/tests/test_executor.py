import collections
import json
import logging
import os
import signal
import subprocess
import time
import unittest
from threading import Event, Thread, Timer

from nose.tools import *
from pymesos import encode_data

import cook
import cook.config as cc
import cook.executor as ce
import cook.io_helper as cio
from tests.utils import assert_message, assert_messages, assert_status, cleanup_output, close_sys_outputs, \
    ensure_directory, get_random_task_id, parse_message, redirect_stderr_to_file, redirect_stdout_to_file, wait_for, \
    FakeExecutorConfig, FakeMesosExecutorDriver


def sleep_and_set_stop_signal_task(stop_signal, wait_seconds):
    """Waits for wait_seconds seconds before setting stop_signal."""
    timer = Timer(wait_seconds, stop_signal.set)
    timer.daemon = True
    timer.start()


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

        ce.kill_process_group(group_id, signal.SIGKILL)

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
            process = ce.launch_task(task, os.environ)
            stdout_thread, stderr_thread = cio.track_outputs(task_id, process, 2, 4096)
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

    def test_launch_task_interactive_output(self):
        task_id = get_random_task_id()
        command = 'echo "Start"; echo "Hello"; sleep 100; echo "World"; echo "Done"; '
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            process = ce.launch_task(task, os.environ)
            stdout_thread, stderr_thread = cio.track_outputs(task_id, process, 2, 4096)
            process_info = process, stdout_thread, stderr_thread

            self.assertIsNotNone(process)

            # let the process run for up to 50 seconds
            for _ in range(5000):
                if ce.is_running(process_info):
                    time.sleep(0.01)
                    with open(stdout_name) as f:
                        stdout_content = f.read()
                        if 'Start' in stdout_content and 'Hello' in stdout_content:
                            break

            try:
                with open(stdout_name) as f:
                    stdout_content = f.read()
                    logging.info ('Contents of stdout: {}'.format(stdout_content))
                    self.assertTrue("Start" in stdout_content)
                    self.assertTrue("Hello" in stdout_content)
                    self.assertFalse("World" in stdout_content)
                    self.assertFalse("Done" in stdout_content)
            finally:
                if process.poll() is None:
                    logging.info ('Killing launched process')
                    process.kill()

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

    def test_kill_task_terminate_with_sigterm(self):
        task_id = get_random_task_id()

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            command = "bash -c 'function handle_term { echo GOT TERM; }; trap handle_term SIGTERM TERM; sleep 100'"
            process = subprocess.Popen(command, preexec_fn=os.setpgrp, shell=True,
                                       stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout_thread, stderr_thread = cio.track_outputs(task_id, process, 2, 4096)
            shutdown_grace_period_ms = 1000

            group_id = ce.find_process_group(process.pid)
            self.assertGreater(len(find_process_ids_in_group(group_id)), 0)

            ce.kill_task(process, shutdown_grace_period_ms)

            # await process termination
            while process.poll() is None:
                time.sleep(0.01)
            stderr_thread.join()
            stdout_thread.join()

            self.assertTrue(((-1 * signal.SIGTERM) == process.poll()) or ((128 + signal.SIGTERM) == process.poll()),
                            'Process exited with code {}'.format(process.poll()))
            self.assertEqual(0, len(find_process_ids_in_group(group_id)))

            with open(stdout_name) as f:
                file_contents = f.read()
                self.assertTrue('GOT TERM' in file_contents)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_kill_task_terminate_with_sigkill(self):
        task_id = get_random_task_id()

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            command = "trap '' TERM SIGTERM; sleep 100"
            process = subprocess.Popen(command, preexec_fn=os.setpgrp, shell=True,
                                       stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout_thread, stderr_thread = cio.track_outputs(task_id, process, 2, 4096)
            shutdown_grace_period_ms = 1000

            group_id = ce.find_process_group(process.pid)
            self.assertGreater(len(find_process_ids_in_group(group_id)), 0)

            ce.kill_task(process, shutdown_grace_period_ms)

            # await process termination
            while process.poll() is None:
                time.sleep(0.01)
            stderr_thread.join()
            stdout_thread.join()

            self.assertTrue(((-1 * signal.SIGKILL) == process.poll()) or ((128 + signal.SIGKILL) == process.poll()),
                            'Process exited with code {}'.format(process.poll()))
            self.assertEqual(len(find_process_ids_in_group(group_id)), 0)

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
            process = subprocess.Popen(command, preexec_fn=os.setpgrp, shell=True)
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
            process = subprocess.Popen(command, preexec_fn=os.setpgrp, shell=True)
            stdout_thread = Thread()
            stderr_thread = Thread()
            process_info = process, stdout_thread, stderr_thread
            shutdown_grace_period_ms = 2000

            stop_signal = Event()
            sleep_and_set_stop_signal_task(stop_signal, 2 * cook.RUNNING_POLL_INTERVAL_SECS)

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

    def manage_task_runner(self, command, assertions_fn, stop_signal=Event(), task_id=get_random_task_id(), config=None):
        driver = FakeMesosExecutorDriver()
        task = {'task_id': {'value': task_id},
                'data': encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        completed_signal = Event()
        if config is None:
            sandbox_directory = '/location/to/task/sandbox/{}'.format(task_id)
            config = cc.ExecutorConfig(max_message_length=300,
                                       progress_output_name=stdout_name,
                                       progress_regex_string='\^\^\^\^JOB-PROGRESS: ([0-9]*\.?[0-9]+)(?: )?(.*)',
                                       progress_sample_interval_ms=100,
                                       sandbox_directory=sandbox_directory)
        else:
            sandbox_directory = config.sandbox_directory

        try:

            ce.manage_task(driver, task, stop_signal, completed_signal, config)

            self.assertTrue(completed_signal.isSet())
            assertions_fn(driver, task_id, sandbox_directory)

        finally:
            cleanup_output(stdout_name, stderr_name)

    def run_command_in_manage_task_runner(self, command, assertions, wait_time_secs):
        stop_signal = Event()
        sleep_and_set_stop_signal_task(stop_signal, wait_time_secs)
        self.manage_task_runner(command, assertions, stop_signal=stop_signal)
        stop_signal.set()

    def test_manage_task_environment_output(self):
        def assertions(driver, task_id, _):
            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
            self.assertEqual(3, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            expected_exit_messages = [{'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'line count is 20',
                                           'progress-percent': 90,
                                           'progress-sequence': 1,
                                           'task-id': task_id}]
            assert_messages(self, driver.messages, expected_exit_messages, expected_progress_messages)

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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
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
            assert_messages(self, driver.messages, expected_exit_messages, expected_progress_messages)

        command = 'echo "Hello World"; ' \
                  'echo "^^^^JOB-PROGRESS: 50 Fifty percent"; ' \
                  'sleep 0.1; ' \
                  'echo "^^^^JOB-PROGRESS: 54.8 Fifty-five percent"; ' \
                  'sleep 0.1; ' \
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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': -15, 'task-id': task_id}
            assert_message(self, expected_message_1, actual_encoded_message_1)

        command = 'sleep 100'
        self.run_command_in_manage_task_runner(command, assertions, 2 * cook.RUNNING_POLL_INTERVAL_SECS)

    def test_manage_task_random_binary_output(self):
        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            logging.info('Messages: {}'.format(driver.messages))
            self.assertEqual(5, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            expected_exit_messages = [{'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'Twenty-five percent in stdout',
                                           'progress-percent': 25,
                                           'progress-sequence': 1,
                                           'task-id': task_id},
                                          {'progress-message': '',
                                           'progress-percent': 50,
                                           'progress-sequence': 2,
                                           'task-id': task_id},
                                          {'progress-message': 'Fifty-five percent in stdout',
                                           'progress-percent': 55,
                                           'progress-sequence': 3,
                                           'task-id': task_id}]
            assert_messages(self, driver.messages, expected_exit_messages, expected_progress_messages)

            stdout_name = ensure_directory('build/stdout.' + str(task_id))
            if not os.path.isfile(stdout_name):
                self.fail('{} does not exist.'.format(stdout_name))

        stop_signal = Event()
        sleep_and_set_stop_signal_task(stop_signal, 60)

        # progress string in file with binary data will be ignored
        command = 'echo "Hello"; ' \
                  'echo "^^^^JOB-PROGRESS: 25 Twenty-five percent in stdout"; ' \
                  'head -c 1000 /dev/random; ' \
                  'echo "force newline"; ' \
                  'sleep 2; ' \
                  'echo "^^^^JOB-PROGRESS: 50 `head -c 100 /dev/random`"; ' \
                  'echo "force newline"; ' \
                  'sleep 2; ' \
                  'head -c 1000 /dev/random; ' \
                  'echo "force newline"; ' \
                  'echo "^^^^JOB-PROGRESS: 55 Fifty-five percent in stdout"; ' \
                  'echo "Done"'
        self.manage_task_runner(command, assertions, stop_signal=stop_signal)
        stop_signal.set()

    def test_manage_task_long_output_single_line(self):
        num_iterations = 100000

        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
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
                    self.assertEqual(num_iterations * 25, file_contents.count('X'))
            else:
                self.fail('{} does not exist.'.format(stdout_name))

            stderr_name = ensure_directory('build/stderr.' + str(task_id))
            if os.path.isfile(stderr_name):
                with open(stderr_name) as f:
                    file_contents = f.read()
                    self.assertEqual(num_iterations * 25, file_contents.count('X'))
            else:
                self.fail('{} does not exist.'.format(stderr_name))

        command = 'for i in `seq {}`; ' \
                  'do printf "XXXXXXXXXXXXXXXXXXXXXXXXX"; printf "XXXXXXXXXXXXXXXXXXXXXXXXX" >&2; done; ' \
                  'echo "Done."'.format(num_iterations)
        self.run_command_in_manage_task_runner(command, assertions, 60)

    def test_manage_task_long_output_multiple_lines(self):
        num_iterations = 100000

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
                    self.assertEqual(num_iterations * 25, file_contents.count('X'))
            else:
                self.fail('{} does not exist.'.format(stdout_name))

            stderr_name = ensure_directory('build/stderr.' + str(task_id))
            if os.path.isfile(stderr_name):
                with open(stderr_name) as f:
                    file_contents = f.read()
                    self.assertEqual(num_iterations * 25, file_contents.count('X'))
            else:
                self.fail('{} does not exist.'.format(stderr_name))

        command = 'for i in `seq {}`; ' \
                  'do printf "XXXXXXXXXXXXXXXXXXXXXXXXX\\n"; printf "XXXXXXXXXXXXXXXXXXXXXXXXX\\n" >&2; done; ' \
                  'echo "Done."'.format(num_iterations)
        self.run_command_in_manage_task_runner(command, assertions, 60)

    def test_manage_task_long_progress_output(self):
        num_iterations = 100000

        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            logging.info('Messages: {}'.format(driver.messages))
            self.assertLess(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            found_exit_message = False
            for index in range(1, len(driver.messages)):
                actual_encoded_message = driver.messages[index]
                actual_message = parse_message(actual_encoded_message)
                if 'exit-code' in actual_message:
                    found_exit_message = True
                    expected_message = {'exit-code': 0, 'task-id': task_id}
                    assert_message(self, expected_message, actual_encoded_message)
                    break
            self.assertTrue(found_exit_message)

            stderr_name = ensure_directory('build/stderr.' + str(task_id))
            if os.path.isfile(stderr_name):
                with open(stderr_name) as f:
                    file_contents = f.read()
                    self.assertEqual(num_iterations * 25, file_contents.count('X'))
            else:
                self.fail('{} does not exist.'.format(stderr_name))

        stop_signal = Event()
        sleep_and_set_stop_signal_task(stop_signal, 60)

        # one order magnitude smaller than non-progress long tests above
        command = 'for i in `seq {}`; ' \
                  'do printf "^^^^JOB-PROGRESS: 50 Fifty\\n"; printf "XXXXXXXXXXXXXXXXXXXXXXXXX\\n" >&2; done; ' \
                  'echo "Done."'.format(num_iterations)
        self.run_command_in_manage_task_runner(command, assertions, 60)

    def test_manage_task_progress_in_progress_stderr_and_stdout_progress(self):
        max_message_length = 130

        def assertions(driver, task_id, sandbox_directory):

            logging.info('Statuses: {}'.format(driver.statuses))
            self.assertEqual(3, len(driver.statuses))

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
            self.assertEqual(6, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            expected_exit_messages = [{'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'Fifty percent in progress file',
                                           'progress-percent': 50,
                                           'progress-sequence': 1,
                                           'task-id': task_id},
                                          {'progress-message': 'Fifty-five percent in stdout',
                                           'progress-percent': 55,
                                           'progress-sequence': 2,
                                           'task-id': task_id},
                                          {'progress-message': 'Sixty percent in stderr',
                                           'progress-percent': 60,
                                           'progress-sequence': 3,
                                           'task-id': task_id},
                                          {'progress-message': 'Sixty-five percent in stdout with...',
                                           'progress-percent': 65,
                                           'progress-sequence': 4,
                                           'task-id': task_id}]
            for i in range(0, len(driver.messages)):
                actual_message = parse_message(driver.messages[i])
                actual_message_string = str(actual_message).encode('utf8')
                self.assertLessEqual(len(actual_message_string), max_message_length)

            assert_messages(self, driver.messages, expected_exit_messages, expected_progress_messages)

        stop_signal = Event()
        sleep_and_set_stop_signal_task(stop_signal, 60)

        task_id=get_random_task_id()
        progress_name = ensure_directory('build/progress.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))
        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))

        config = FakeExecutorConfig({'flush_interval_secs': 0.5,
                                     'max_bytes_read_per_line': 1024,
                                     'max_message_length': max_message_length,
                                     'progress_output_env_variable': 'DEFAULT_PROGRESS_FILE_ENV_VARIABLE',
                                     'progress_output_name': progress_name,
                                     'progress_regex_string': '\^\^\^\^JOB-PROGRESS: ([0-9]*\.?[0-9]+)(?: )?(.*)',
                                     'progress_sample_interval_ms': 10,
                                     'sandbox_directory': '/sandbox/directory/for/{}'.format(task_id),
                                     'shutdown_grace_period_ms': 60000,
                                     'stderr_file': stderr_name,
                                     'stdout_file': stdout_name})

        command = 'echo "Hello World"; ' \
                  'echo "^^^^JOB-PROGRESS: 50 Fifty percent in progress file" >> {}; ' \
                  'sleep 0.1; ' \
                  'echo "^^^^JOB-PROGRESS: 55 Fifty-five percent in stdout" >> {}; ' \
                  'sleep 0.1; ' \
                  'echo "^^^^JOB-PROGRESS: 60 Sixty percent in stderr" >> {}; ' \
                  'sleep 0.1; ' \
                  'echo "^^^^JOB-PROGRESS: 65 Sixty-five percent in stdout with a long message" >> {}; ' \
                  'sleep 0.1; ' \
                  'echo "Exiting..."; ' \
                  'exit 0'.format(progress_name, stdout_name, stderr_name, stdout_name)

        self.manage_task_runner(command, assertions, stop_signal=stop_signal, task_id=task_id, config=config)
        stop_signal.set()

    def test_executor_launch_task(self):

        task_id = get_random_task_id()
        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            config = cc.ExecutorConfig()
            stop_signal = Event()
            executor = ce.CookExecutor(stop_signal, config)

            driver = FakeMesosExecutorDriver()
            output_name = ensure_directory('build/output.' + str(task_id))
            command = 'echo "Start" >> {}; sleep 0.1; echo "Done." >> {}; '.format(output_name, output_name)
            task = {'task_id': {'value': task_id},
                    'data': encode_data(json.dumps({'command': command}).encode('utf8'))}

            executor.launchTask(driver, task)
            executor.await_completion()
            logging.info('Task completed')

            if os.path.isfile(output_name):
                with open(output_name) as f:
                    file_contents = f.read()
                    self.assertTrue('Start' in file_contents)
                    self.assertTrue('Done' in file_contents)
            else:
                self.fail('{} does not exist.'.format(stderr_name))

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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': '', 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            assert_message(self, expected_message_1, actual_encoded_message_1)
        finally:
            cleanup_output(stdout_name, stderr_name)

    def test_executor_launch_task_and_disconnect(self):

        task_id = get_random_task_id()
        stdout_name = ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = ensure_directory('build/stderr.{}'.format(task_id))

        redirect_stdout_to_file(stdout_name)
        redirect_stderr_to_file(stderr_name)

        try:
            config = cc.ExecutorConfig()
            stop_signal = Event()
            executor = ce.CookExecutor(stop_signal, config)

            driver = FakeMesosExecutorDriver()
            output_name = ensure_directory('build/output.' + str(task_id))
            command = 'echo "Start" >> {}; sleep 100; echo "Done." >> {}; '.format(output_name, output_name)
            task = {'task_id': {'value': task_id},
                    'data': encode_data(json.dumps({'command': command}).encode('utf8'))}

            executor.launchTask(driver, task)

            # let the process run for up to 10 seconds
            for _ in range(1000):
                time.sleep(0.01)
                if os.path.isfile(output_name):
                    with open(output_name) as f:
                        content = f.read()
                        if 'Start' in content:
                            break

            executor.disconnected(driver)
            self.assertTrue(executor.disconnect_signal.isSet())
            self.assertTrue(executor.stop_signal.isSet())

            executor.await_completion()
            logging.info('Task completed')

            if os.path.isfile(output_name):
                with open(output_name) as f:
                    file_contents = f.read()
                    self.assertTrue('Start' in file_contents)
                    self.assertTrue('Done' not in file_contents)
            else:
                self.fail('{} does not exist.'.format(stderr_name))

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

            logging.info('Messages: {}'.format(list(map(lambda m: parse_message(m), driver.messages))))
            self.assertEqual(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': '', 'task-id': task_id, 'type': 'directory'}
            assert_message(self, expected_message_0, actual_encoded_message_0)

            actual_encoded_message_1 = driver.messages[1]
            expected_message_1 = {'exit-code': -15, 'task-id': task_id}
            assert_message(self, expected_message_1, actual_encoded_message_1)
        finally:
            cleanup_output(stdout_name, stderr_name)

