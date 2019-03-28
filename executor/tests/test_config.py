import logging
import unittest

import os

import cook.config as cc


class ConfigTest(unittest.TestCase):
    def test_parse_time_ms(self):
        self.assertEqual(1000, cc.ExecutorConfig.parse_time_ms('1secs'))
        self.assertEqual(2000, cc.ExecutorConfig.parse_time_ms('2secs'))
        self.assertEqual(5000, cc.ExecutorConfig.parse_time_ms('5secs'))
        logging.info('1 hrs = {}'.format(cc.ExecutorConfig.parse_time_ms('1hrs')))
        self.assertEqual(3600000, cc.ExecutorConfig.parse_time_ms('1hrs'))
        # default value tests
        self.assertEqual(1000, cc.ExecutorConfig.parse_time_ms('1sec'))
        self.assertEqual(1000, cc.ExecutorConfig.parse_time_ms('5'))
        self.assertEqual(1000, cc.ExecutorConfig.parse_time_ms('corrupt-value'))

    def test_executor_config(self):
        checkpoint = 1
        max_bytes_read_per_line = 16 * 1024
        max_message_length = 300
        memory_usage_interval_secs = 150
        progress_output_env_variable = 'PROGRESS_OUTPUT_ENV_VARIABLE'
        progress_output_name = 'stdout_name'
        progress_regex_string = 'some-regex-string'
        progress_sample_interval_ms = 100
        recovery_timeout = '5mins'
        sandbox_directory = '/location/to/task/sandbox/task_id'
        shutdown_grace_period_secs = '5secs'
        config = cc.ExecutorConfig(checkpoint=checkpoint,
                                   max_bytes_read_per_line=max_bytes_read_per_line,
                                   max_message_length=max_message_length,
                                   memory_usage_interval_secs=memory_usage_interval_secs,
                                   progress_output_env_variable=progress_output_env_variable,
                                   progress_output_name=progress_output_name,
                                   progress_regex_string=progress_regex_string,
                                   progress_sample_interval_ms=progress_sample_interval_ms,
                                   recovery_timeout=recovery_timeout,
                                   sandbox_directory=sandbox_directory,
                                   shutdown_grace_period=shutdown_grace_period_secs)

        self.assertEqual(checkpoint, True)
        self.assertEqual(max_bytes_read_per_line, config.max_bytes_read_per_line)
        self.assertEqual(max_message_length, config.max_message_length)
        self.assertEqual(memory_usage_interval_secs, config.memory_usage_interval_secs)
        self.assertEqual(progress_output_env_variable, config.progress_output_env_variable)
        self.assertEqual(progress_output_name, config.progress_output_name)
        self.assertEqual(progress_regex_string, config.progress_regex_string)
        self.assertEqual(progress_sample_interval_ms, config.progress_sample_interval_ms)
        self.assertEqual(5 * 60 * 1000, config.recovery_timeout_ms)
        self.assertEqual(sandbox_directory, config.sandbox_directory)
        self.assertEqual(5000, config.shutdown_grace_period_ms)
        self.assertEqual(os.path.join(sandbox_directory, 'foo.bar'), config.sandbox_file('foo.bar'))
        self.assertEqual(os.path.join(sandbox_directory, 'stderr'), config.stderr_file())
        self.assertEqual(os.path.join(sandbox_directory, 'stdout'), config.stdout_file())

    def test_initialize_config_defaults(self):
        environment = {}
        config = cc.initialize_config(environment)

        self.assertEqual(False, config.checkpoint)
        self.assertEqual(4 * 1024, config.max_bytes_read_per_line)
        self.assertEqual(512, config.max_message_length)
        self.assertEqual('executor.progress', config.progress_output_name)
        self.assertEqual('progress: ([0-9]*\\.?[0-9]+), (.*)', config.progress_regex_string)
        self.assertEqual(15 * 60 * 1000, config.recovery_timeout_ms)
        self.assertEqual(1000, config.progress_sample_interval_ms)
        self.assertEqual('', config.sandbox_directory)
        self.assertEqual(2000, config.shutdown_grace_period_ms)

    def test_initialize_config_custom(self):
        environment = {'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'EXECUTOR_MEMORY_USAGE_INTERVAL_SECS': '120',
                       'EXECUTOR_PROGRESS_OUTPUT_FILE': 'progress_file',
                       'MESOS_CHECKPOINT': '1',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'MESOS_RECOVERY_TIMEOUT': '5mins',
                       'MESOS_SANDBOX': '/sandbox/location',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(True, config.checkpoint)
        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual(120, config.memory_usage_interval_secs)
        self.assertEqual('EXECUTOR_PROGRESS_OUTPUT_FILE', config.progress_output_env_variable)
        self.assertEqual('progress_file', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(5 * 60 * 1000, config.recovery_timeout_ms)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('/sandbox/location', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)

    def test_initialize_config_custom_progress_file_without_sandbox(self):
        environment = {'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'EXECUTOR_MEMORY_USAGE_INTERVAL_SECS': '20',
                       'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'OUTPUT_TARGET_FILE',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'OUTPUT_TARGET_FILE': 'progress.out',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual(30, config.memory_usage_interval_secs)
        self.assertEqual('OUTPUT_TARGET_FILE', config.progress_output_env_variable)
        self.assertEqual('progress.out', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)

    def test_initialize_config_custom_progress_file_with_sandbox(self):
        environment = {'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'OUTPUT_TARGET_FILE',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'MESOS_SANDBOX': '/sandbox/location',
                       'OUTPUT_TARGET_FILE': 'progress.out',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual(3600, config.memory_usage_interval_secs)
        self.assertEqual('OUTPUT_TARGET_FILE', config.progress_output_env_variable)
        self.assertEqual('progress.out', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('/sandbox/location', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)

    def test_initialize_config_default_progress_file_with_sandbox(self):
        environment = {'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'OUTPUT_TARGET_FILE',
                       'MESOS_EXECUTOR_ID': 'e123456',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'MESOS_SANDBOX': '/sandbox/location',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual(3600, config.memory_usage_interval_secs)
        self.assertEqual('OUTPUT_TARGET_FILE', config.progress_output_env_variable)
        self.assertEqual('/sandbox/location/e123456.progress', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('/sandbox/location', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)

    def test_initialize_config_configured_progress_file_name_missing(self):
        environment = {'EXECUTOR_DEFAULT_PROGRESS_OUTPUT_NAME': 'stdout_file',
                       'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'OUTPUT_TARGET_FILE',
                       'MESOS_EXECUTOR_ID': 'e123456',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'MESOS_SANDBOX': '/sandbox/location',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual(3600, config.memory_usage_interval_secs)
        self.assertEqual('OUTPUT_TARGET_FILE', config.progress_output_env_variable)
        self.assertEqual('/sandbox/location/stdout_file', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('/sandbox/location', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)

    def test_initialize_config_configured_progress_file_name_exists(self):
        environment = {'EXECUTOR_DEFAULT_PROGRESS_OUTPUT_NAME': 'stdout_file',
                       'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'OUTPUT_TARGET_FILE',
                       'MESOS_EXECUTOR_ID': 'e123456',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'MESOS_SANDBOX': '/sandbox/location',
                       'OUTPUT_TARGET_FILE': '/path/to/progress_file',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual(3600, config.memory_usage_interval_secs)
        self.assertEqual('OUTPUT_TARGET_FILE', config.progress_output_env_variable)
        self.assertEqual('/path/to/progress_file', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('/sandbox/location', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)

    def test_initialize_config_configured_progress_file_name_is_dev_null(self):
        environment = {'EXECUTOR_DEFAULT_PROGRESS_OUTPUT_NAME': 'stderr_file',
                       'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV': 'OUTPUT_TARGET_FILE',
                       'MESOS_EXECUTOR_ID': 'e123456',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'MESOS_SANDBOX': '/sandbox/location',
                       'OUTPUT_TARGET_FILE': '/dev/null',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual(3600, config.memory_usage_interval_secs)
        self.assertEqual('OUTPUT_TARGET_FILE', config.progress_output_env_variable)
        self.assertEqual('/dev/null', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('/sandbox/location', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)
