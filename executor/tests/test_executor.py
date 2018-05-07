import errno
import functools
import json
import logging
import subprocess
import time
import unittest
from unittest.mock import patch
from threading import Event, Timer

import os
import pymesos as pm
import pytest

import cook
import cook.config as cc
import cook.executor as ce
import cook.subprocess as cs
import tests.utils as tu


def sleep_and_set_stop_signal_task(stop_signal, wait_seconds):
    """Waits for wait_seconds seconds before setting stop_signal."""
    timer = Timer(wait_seconds, stop_signal.set)
    timer.daemon = True
    timer.start()


class ExecutorTest(unittest.TestCase):
    def test_get_task_id(self):
        task_id = tu.get_random_task_id()
        task = {'task_id': {'value': task_id}}
        self.assertEqual(task_id, ce.get_task_id(task))

    def test_get_task_id_missing_value(self):
        with self.assertRaises(KeyError):
            ce.get_task_id({'task_id': {}})

    def test_get_task_id_empty_dictionary(self):
        with self.assertRaises(KeyError):
            ce.get_task_id({})

    def test_create_status_running(self):
        driver = tu.FakeMesosExecutorDriver()
        task_id = tu.get_random_task_id()
        status_updater = ce.StatusUpdater(driver, task_id)
        actual_status = status_updater.create_status(cook.TASK_RUNNING)
        expected_status = {'task_id': {'value': task_id},
                           'state': cook.TASK_RUNNING}
        tu.assert_status(self, expected_status, actual_status)

    def test_update_status(self):
        driver = tu.FakeMesosExecutorDriver()
        task_id = tu.get_random_task_id()
        status_updater = ce.StatusUpdater(driver, task_id)
        task_state = "TEST_TASK_STATE"

        self.assertTrue(status_updater.update_status(cook.TASK_STARTING))
        self.assertTrue(status_updater.update_status(task_state))
        self.assertTrue(status_updater.update_status(cook.TASK_RUNNING, reason='Running'))
        self.assertTrue(status_updater.update_status(cook.TASK_FAILED, reason='Termination'))
        self.assertFalse(status_updater.update_status(cook.TASK_FINISHED))

        expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                             {'task_id': {'value': task_id}, 'state': task_state},
                             {'task_id': {'value': task_id}, 'reason': 'Running', 'state': cook.TASK_RUNNING},
                             {'task_id': {'value': task_id}, 'reason': 'Termination', 'state': cook.TASK_FAILED}]
        tu.assert_statuses(self, expected_statuses, driver.statuses)

    def test_send_message(self):
        driver = tu.FakeMesosExecutorDriver()
        task_id = tu.get_random_task_id()
        expected_message = {'task-id': task_id, 'message': 'test-message'}

        result = ce.send_message(driver, tu.fake_os_error_handler, expected_message)

        self.assertTrue(result)
        self.assertEqual(1, len(driver.messages))
        actual_encoded_message = driver.messages[0]
        tu.assert_message(self, expected_message, actual_encoded_message)

    def test_send_message_handles_os_error_memory(self):
        exception_handler_calls = []
        exception_handler = functools.partial(tu.store_exception_handler, exception_handler_calls)
        exception = OSError(errno.ENOMEM, 'Out of memory')
        driver = tu.ErrorMesosExecutorDriver(exception)
        task_id = tu.get_random_task_id()
        message = {'task-id': task_id, 'message': 'test-message'}

        result = ce.send_message(driver, exception_handler, message)

        self.assertEqual([exception], exception_handler_calls)
        self.assertFalse(result)
        self.assertEqual(1, len(driver.messages))

    def test_send_message_handles_os_error_non_memory(self):
        exception_handler_calls = []
        exception_handler = functools.partial(tu.store_exception_handler, exception_handler_calls)
        exception = OSError(errno.EACCES, 'Permission denied')
        driver = tu.ErrorMesosExecutorDriver(exception)
        task_id = tu.get_random_task_id()
        message = {'task-id': task_id, 'message': 'test-message'}

        result = ce.send_message(driver, exception_handler, message)

        self.assertEqual([], exception_handler_calls)
        self.assertFalse(result)
        self.assertEqual(1, len(driver.messages))

    def test_send_message_handles_generic_exception(self):
        exception_handler_calls = []
        exception_handler = functools.partial(tu.store_exception_handler, exception_handler_calls)
        exception = Exception('Generic Exception')
        driver = tu.ErrorMesosExecutorDriver(exception)
        task_id = tu.get_random_task_id()
        message = {'task-id': task_id, 'message': 'test-message'}

        result = ce.send_message(driver, exception_handler, message)

        self.assertEqual([], exception_handler_calls)
        self.assertFalse(result)
        self.assertEqual(1, len(driver.messages))

    def test_os_error_handler_functools_partial(self):
        driver = tu.FakeMesosExecutorDriver()
        task_id = tu.get_random_task_id()
        status_updater = ce.StatusUpdater(driver, task_id)
        stop_signal = Event()
        inner_os_error_handler = functools.partial(ce.os_error_handler, stop_signal, status_updater)

        inner_os_error_handler(OSError(errno.ENOMEM, 'No Memory'))

        self.assertTrue(stop_signal.isSet())
        expected_statuses = [{'task_id': {'value': task_id},
                              'reason': cook.REASON_CONTAINER_LIMITATION_MEMORY,
                              'state': cook.TASK_FAILED}]
        tu.assert_statuses(self, expected_statuses, driver.statuses)

    def test_os_error_handler_no_memory(self):
        driver = tu.FakeMesosExecutorDriver()
        task_id = tu.get_random_task_id()
        status_updater = ce.StatusUpdater(driver, task_id)
        stop_signal = Event()
        os_error = OSError(errno.ENOMEM, 'No Memory')

        ce.os_error_handler(stop_signal, status_updater, os_error)

        self.assertTrue(stop_signal.isSet())
        expected_statuses = [{'task_id': {'value': task_id},
                              'reason': cook.REASON_CONTAINER_LIMITATION_MEMORY,
                              'state': cook.TASK_FAILED}]
        tu.assert_statuses(self, expected_statuses, driver.statuses)

    def test_os_error_handler_no_permission(self):
        driver = tu.FakeMesosExecutorDriver()
        task_id = tu.get_random_task_id()
        status_updater = ce.StatusUpdater(driver, task_id)
        stop_signal = Event()
        os_error = OSError(errno.EPERM, 'No Permission')

        ce.os_error_handler(stop_signal, status_updater, os_error)

        self.assertTrue(stop_signal.isSet())
        expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_FAILED}]
        tu.assert_statuses(self, expected_statuses, driver.statuses)

    def test_launch_task(self):
        task_id = tu.get_random_task_id()
        command = 'echo "Hello World"; echo "Error Message" >&2'
        task = {'task_id': {'value': task_id},
                'data': pm.encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            process = ce.launch_task(task, os.environ)

            self.assertIsNotNone(process)

            for _ in range(100):
                if cs.is_process_running(process):
                    time.sleep(0.01)

            if process.poll() is None:
                process.kill()
            tu.close_sys_outputs()

            self.assertEqual(0, process.poll())

            with open(stdout_name) as f:
                stdout_content = f.read()
                self.assertTrue("Hello World\n" in stdout_content)

            with open(stderr_name) as f:
                stderr_content = f.read()
                self.assertTrue("Error Message\n" in stderr_content)
        finally:
            tu.cleanup_output(stdout_name, stderr_name)

    def test_launch_task_interactive_output(self):
        task_id = tu.get_random_task_id()
        command = 'echo "Start"; echo "Hello"; sleep 100; echo "World"; echo "Done"; '
        task = {'task_id': {'value': task_id},
                'data': pm.encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            process = ce.launch_task(task, os.environ)

            self.assertIsNotNone(process)

            # let the process run for up to 50 seconds
            for _ in range(5000):
                if cs.is_process_running(process):
                    time.sleep(0.01)
                    with open(stdout_name) as f:
                        stdout_content = f.read()
                        if 'Start' in stdout_content and 'Hello' in stdout_content:
                            break

            try:
                with open(stdout_name) as f:
                    stdout_content = f.read()
                    logging.info('Contents of stdout: {}'.format(stdout_content))
                    self.assertTrue("Start" in stdout_content)
                    self.assertTrue("Hello" in stdout_content)
                    self.assertFalse("World" in stdout_content)
                    self.assertFalse("Done" in stdout_content)
            finally:
                if process.poll() is None:
                    logging.info('Killing launched process')
                    process.kill()

        finally:
            tu.cleanup_output(stdout_name, stderr_name)

    def test_launch_task_no_command(self):
        task_id = tu.get_random_task_id()
        task = {'task_id': {'value': task_id},
                'data': pm.encode_data(json.dumps({'command': ''}).encode('utf8'))}

        process = ce.launch_task(task, os.environ)

        self.assertIsNone(process)

    def test_launch_task_handle_exception(self):
        task_id = tu.get_random_task_id()
        task = {'task_id': {'value': task_id}}

        process = ce.launch_task(task, os.environ)

        self.assertIsNone(process)

    def test_await_process_completion_normal(self):
        task_id = tu.get_random_task_id()

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            command = 'sleep 2'
            process = subprocess.Popen(command, preexec_fn=os.setpgrp, shell=True)
            shutdown_grace_period_ms = 1000

            stop_signal = Event()

            ce.await_process_completion(process, stop_signal, shutdown_grace_period_ms)

            self.assertFalse(stop_signal.isSet())
            self.assertEqual(0, process.returncode)

        finally:
            tu.cleanup_output(stdout_name, stderr_name)

    def test_await_process_completion_killed(self):
        task_id = tu.get_random_task_id()

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            command = 'sleep 100'
            process = subprocess.Popen(command, preexec_fn=os.setpgrp, shell=True)
            shutdown_grace_period_ms = 2000

            stop_signal = Event()
            sleep_and_set_stop_signal_task(stop_signal, 2)

            ce.await_process_completion(process, stop_signal, shutdown_grace_period_ms)

            self.assertTrue(process.returncode < 0)

        finally:
            tu.cleanup_output(stdout_name, stderr_name)

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

    def manage_task_runner(self, command, assertions_fn, stop_signal=None, task_id=None, config=None, driver=None):

        if driver is None:
            driver = tu.FakeMesosExecutorDriver()
        if stop_signal is None:
            stop_signal = Event()
        if task_id is None:
            task_id = tu.get_random_task_id()

        task = {'task_id': {'value': task_id},
                'data': pm.encode_data(json.dumps({'command': command}).encode('utf8'))}

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        completed_signal = Event()
        if config is None:
            sandbox_directory = '/location/to/task/sandbox/{}'.format(task_id)
            config = cc.ExecutorConfig(max_message_length=300,
                                       progress_output_name=stdout_name,
                                       progress_regex_string='\^\^\^\^JOB-PROGRESS:\s+([0-9]*\.?[0-9]+)($|\s+.*)',
                                       progress_sample_interval_ms=100,
                                       sandbox_directory=sandbox_directory)
        else:
            sandbox_directory = config.sandbox_directory

        try:

            ce.manage_task(driver, task, stop_signal, completed_signal, config)

            self.assertTrue(completed_signal.isSet())
            assertions_fn(driver, task_id, sandbox_directory)

        finally:
            tu.cleanup_output(stdout_name, stderr_name)

    def run_command_in_manage_task_runner(self, command, assertions, wait_time_secs):
        stop_signal = Event()
        sleep_and_set_stop_signal_task(stop_signal, wait_time_secs)
        self.manage_task_runner(command, assertions, stop_signal=stop_signal)
        stop_signal.set()

    def test_manage_task_environment_output(self):
        def assertions(driver, task_id, _):
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': '/location/to/task/sandbox/{}'.format(task_id),
                                  'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)

            stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
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
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)

        command = 'echo "Hello World"'
        self.manage_task_runner(command, assertions)

    def test_manage_task_empty_command(self):
        def assertions(driver, task_id, sandbox_directory):
            expected_statuses = [{'task_id': {'value': task_id},
                                  'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id},
                                  'reason': cook.REASON_TASK_INVALID,
                                  'state': cook.TASK_ERROR}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            tu.assert_messages(self, [expected_message_0], [], driver.messages)

        command = ''
        self.manage_task_runner(command, assertions)

    def test_manage_task_involved_command_successful_exit(self):
        def assertions(driver, task_id, sandbox_directory):
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_core_messages = [{'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'},
                                      {'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'line count is 20',
                                           'progress-percent': 90, 'progress-sequence': 1, 'task-id': task_id}]
            tu.assert_messages(self, expected_core_messages, expected_progress_messages, driver.messages)

        test_file_name = tu.ensure_directory('build/file.' + tu.get_random_task_id())
        command = ('mkdir -p build; touch {0}; for i in $(seq 20); do echo $i >> {0}; done; '
                   'LINE_COUNT=`wc -l < {0} | tr -d \'[:space:]\'`; cat  {0}; rm -rfv {0}; '
                   'echo "^^^^JOB-PROGRESS: 90 line count is $LINE_COUNT"'.format(test_file_name))
        self.manage_task_runner(command, assertions)

    def test_manage_task_successful_exit_despite_faulty_driver(self):
        def assertions(driver, task_id, sandbox_directory):
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_core_messages = [{'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'},
                                      {'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'ninety percent',
                                           'progress-percent': 90, 'progress-sequence': 1, 'task-id': task_id},
                                          # retried because the previous send fails
                                          {'progress-message': 'ninety percent',
                                           'progress-percent': 90, 'progress-sequence': 1, 'task-id': task_id}]
            tu.assert_messages(self, expected_core_messages, expected_progress_messages, driver.messages)

        test_file_name = tu.ensure_directory('build/file.' + tu.get_random_task_id())
        command = ('echo "^^^^JOB-PROGRESS: 90 ninety percent"'.format(test_file_name))
        socket_error = OSError('socket.error') # socket.error is an alias of OSError
        self.manage_task_runner(command, assertions, driver=tu.ErrorMesosExecutorDriver(socket_error))

    def test_manage_task_successful_exit_with_progress_message(self):
        def assertions(driver, task_id, sandbox_directory):
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_core_messages = [{'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'},
                                      {'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'Fifty percent',
                                           'progress-percent': 50, 'progress-sequence': 1, 'task-id': task_id},
                                          {'progress-message': 'Fifty-five percent',
                                           'progress-percent': 55, 'progress-sequence': 2, 'task-id': task_id}]
            tu.assert_messages(self, expected_core_messages, expected_progress_messages, driver.messages)

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
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FAILED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': 1, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)

        command = 'echo "Hello World"; exit 1'
        self.manage_task_runner(command, assertions)

    # FIXME - remove the xfail mark once the issue with this test crashing is resolved:
    # https://github.com/twosigma/Cook/issues/678
    @pytest.mark.xfail
    @unittest.skip('This test crashes occasionally')
    def test_manage_task_terminated(self):
        def assertions(driver, task_id, sandbox_directory):
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_KILLED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': -15, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)

        command = 'sleep 100'
        self.run_command_in_manage_task_runner(command, assertions, 2)

    def test_manage_task_random_binary_output(self):
        def assertions(driver, task_id, sandbox_directory):
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_core_messages = [{'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'},
                                      {'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': '',
                                           'progress-percent': 50, 'progress-sequence': 1, 'task-id': task_id}]
            tu.assert_messages(self, expected_core_messages, expected_progress_messages, driver.messages)

            stdout_name = tu.ensure_directory('build/stdout.' + str(task_id))
            if not os.path.isfile(stdout_name):
                self.fail('{} does not exist.'.format(stdout_name))

        stop_signal = Event()
        sleep_and_set_stop_signal_task(stop_signal, 60)

        # progress string in file with binary data will be ignored
        command = 'echo "Hello"; ' \
                  'head -c 1000 /dev/random; ' \
                  'echo "force newline stage-1"; ' \
                  'echo "^^^^JOB-PROGRESS: 50 `head -c 50 /dev/random`"; ' \
                  'echo "force newline stage-2"; ' \
                  'echo "Done"'
        self.manage_task_runner(command, assertions, stop_signal=stop_signal)
        stop_signal.set()

    def test_manage_task_long_output_single_line(self):
        num_iterations = 100000

        def assertions(driver, task_id, sandbox_directory):

            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)

            stdout_name = tu.ensure_directory('build/stdout.' + str(task_id))
            if os.path.isfile(stdout_name):
                with open(stdout_name) as f:
                    file_contents = f.read()
                    self.assertEqual(num_iterations * 25, file_contents.count('X'))
            else:
                self.fail('{} does not exist.'.format(stdout_name))

            stderr_name = tu.ensure_directory('build/stderr.' + str(task_id))
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

            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)

            stdout_name = tu.ensure_directory('build/stdout.' + str(task_id))
            if os.path.isfile(stdout_name):
                with open(stdout_name) as f:
                    file_contents = f.read()
                    self.assertEqual(num_iterations * 25, file_contents.count('X'))
            else:
                self.fail('{} does not exist.'.format(stdout_name))

            stderr_name = tu.ensure_directory('build/stderr.' + str(task_id))
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

            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            logging.info('Messages: {}'.format(driver.messages))
            self.assertLess(2, len(driver.messages))

            actual_encoded_message_0 = driver.messages[0]
            expected_message_0 = {'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'}
            tu.assert_message(self, expected_message_0, actual_encoded_message_0)

            found_exit_message = False
            for index in range(1, len(driver.messages)):
                actual_encoded_message = driver.messages[index]
                actual_message = tu.parse_message(actual_encoded_message)
                if 'exit-code' in actual_message:
                    found_exit_message = True
                    expected_message = {'exit-code': 0, 'task-id': task_id}
                    tu.assert_message(self, expected_message, actual_encoded_message)
                    break
            self.assertTrue(found_exit_message)

            stderr_name = tu.ensure_directory('build/stderr.' + str(task_id))
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
        max_message_length = 35

        def assertions(driver, task_id, sandbox_directory):
            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_core_messages = [{'sandbox-directory': sandbox_directory, 'task-id': task_id, 'type': 'directory'},
                                      {'exit-code': 0, 'task-id': task_id}]
            expected_progress_messages = [{'progress-message': 'Fifty percent in progress file',
                                           'progress-percent': 50, 'progress-sequence': 1, 'task-id': task_id},
                                          {'progress-message': 'Fifty-five percent in stdout',
                                           'progress-percent': 55, 'progress-sequence': 2, 'task-id': task_id},
                                          {'progress-message': 'Sixty percent in stderr',
                                           'progress-percent': 60, 'progress-sequence': 3, 'task-id': task_id},
                                          {'progress-message': 'Sixty-five percent in stdout wit...',
                                           'progress-percent': 65, 'progress-sequence': 4, 'task-id': task_id}]
            tu.assert_messages(self, expected_core_messages, expected_progress_messages, driver.messages)

        stop_signal = Event()
        sleep_and_set_stop_signal_task(stop_signal, 60)

        task_id = tu.get_random_task_id()
        progress_name = tu.ensure_directory('build/progress.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))
        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))

        config = tu.FakeExecutorConfig({'max_bytes_read_per_line': 1024,
                                        'max_message_length': max_message_length,
                                        'progress_output_env_variable': 'DEFAULT_PROGRESS_FILE_ENV_VARIABLE',
                                        'progress_output_name': progress_name,
                                        'progress_regex_string': '\^\^\^\^JOB-PROGRESS:\s+([0-9]*\.?[0-9]+)($|\s+.*)',
                                        'progress_sample_interval_ms': 10,
                                        'sandbox_directory': '/sandbox/directory/for/{}'.format(task_id),
                                        'shutdown_grace_period_ms': 60000,
                                        'stderr_file': stderr_name,
                                        'stdout_file': stdout_name})

        command = 'echo "Hello World"; ' \
                  'echo "^^^^JOB-PROGRESS: 50 Fifty percent in progress file" >> {}; ' \
                  'sleep 0.25; ' \
                  'echo "^^^^JOB-PROGRESS: 55 Fifty-five percent in stdout" >> {}; ' \
                  'sleep 0.25; ' \
                  'echo "^^^^JOB-PROGRESS: 60 Sixty percent in stderr" >> {}; ' \
                  'sleep 0.25; ' \
                  'echo "^^^^JOB-PROGRESS: 65 Sixty-five percent in stdout with a long message" >> {}; ' \
                  'sleep 0.25; ' \
                  'echo "Exiting..."; ' \
                  'exit 0'.format(progress_name, stdout_name, stderr_name, stdout_name)

        try:
            self.manage_task_runner(command, assertions, stop_signal=stop_signal, task_id=task_id, config=config)
            stop_signal.set()
        finally:
            tu.cleanup_file(progress_name)

    def test_executor_launch_task(self):

        task_id = tu.get_random_task_id()
        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))
        output_name = tu.ensure_directory('build/output.' + str(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            config = cc.ExecutorConfig()
            stop_signal = Event()
            executor = ce.CookExecutor(stop_signal, config)

            driver = tu.FakeMesosExecutorDriver()
            command = 'echo "Start" >> {}; sleep 0.1; echo "Done." >> {}; '.format(output_name, output_name)
            task = {'task_id': {'value': task_id},
                    'data': pm.encode_data(json.dumps({'command': command}).encode('utf8'))}

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

            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_FINISHED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': '', 'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': 0, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)
        finally:
            tu.cleanup_output(stdout_name, stderr_name)
            tu.cleanup_file(output_name)

    def test_executor_launch_task_and_disconnect(self):

        task_id = tu.get_random_task_id()
        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))
        output_name = tu.ensure_directory('build/output.' + str(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            config = cc.ExecutorConfig()
            stop_signal = Event()
            executor = ce.CookExecutor(stop_signal, config)

            driver = tu.FakeMesosExecutorDriver()
            command = 'echo "Start" >> {}; sleep 100; echo "Done." >> {}; '.format(output_name, output_name)
            task = {'task_id': {'value': task_id},
                    'data': pm.encode_data(json.dumps({'command': command}).encode('utf8'))}

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

            expected_statuses = [{'task_id': {'value': task_id}, 'state': cook.TASK_STARTING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_RUNNING},
                                 {'task_id': {'value': task_id}, 'state': cook.TASK_KILLED}]
            tu.assert_statuses(self, expected_statuses, driver.statuses)

            expected_message_0 = {'sandbox-directory': '', 'task-id': task_id, 'type': 'directory'}
            expected_message_1 = {'exit-code': -15, 'task-id': task_id}
            tu.assert_messages(self, [expected_message_0, expected_message_1], [], driver.messages)
        finally:
            tu.cleanup_output(stdout_name, stderr_name)
            tu.cleanup_file(output_name)

    @patch('os._exit')
    def test_executor_exit_env_variable(self, mock_exit):
        os.environ['EXECUTOR_TEST_EXIT'] = '5'
        try:
            config = cc.ExecutorConfig()
            stop_signal = Event()
            executor = ce.CookExecutor(stop_signal, config)
            driver = tu.FakeMesosExecutorDriver()
            executor_info = {'executor_id': {'value': 'test'}}
            framework_info = {'id': 'framework'}
            agent_info = {'id': {'value': 'agent'}}
            executor.registered(driver, executor_info, framework_info, agent_info)
            mock_exit.assert_called_with(5)
        finally:
            del os.environ['EXECUTOR_TEST_EXIT']
