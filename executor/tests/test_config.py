import logging
import unittest

from nose.tools import *

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
        max_bytes_read_per_line = 16 * 1024
        max_message_length = 300
        progress_output_name = 'stdout_name'
        progress_regex_string = 'some-regex-string'
        progress_sample_interval_ms = 100
        sandbox_directory = '/location/to/task/sandbox/task_id'
        shutdown_grace_period_secs = '5secs'
        config = cc.ExecutorConfig(max_bytes_read_per_line=max_bytes_read_per_line,
                                   max_message_length=max_message_length,
                                   progress_output_name=progress_output_name,
                                   progress_regex_string=progress_regex_string,
                                   progress_sample_interval_ms=progress_sample_interval_ms,
                                   sandbox_directory=sandbox_directory,
                                   shutdown_grace_period=shutdown_grace_period_secs)

        self.assertEqual(max_bytes_read_per_line, config.max_bytes_read_per_line)
        self.assertEqual(max_message_length, config.max_message_length)
        self.assertEqual(progress_output_name, config.progress_output_name)
        self.assertEqual(progress_regex_string, config.progress_regex_string)
        self.assertEqual(progress_sample_interval_ms, config.progress_sample_interval_ms)
        self.assertEqual(sandbox_directory, config.sandbox_directory)
        self.assertEqual(5000, config.shutdown_grace_period_ms)

    def test_initialize_config_defaults(self):
        environment = {}
        config = cc.initialize_config(environment)

        self.assertEqual(4 * 1024, config.max_bytes_read_per_line)
        self.assertEqual(512, config.max_message_length)
        self.assertEqual('stdout', config.progress_output_name)
        self.assertEqual('progress: (\\d*), (.*)', config.progress_regex_string)
        self.assertEqual(1000, config.progress_sample_interval_ms)
        self.assertEqual('', config.sandbox_directory)
        self.assertEqual(2000, config.shutdown_grace_period_ms)

    def test_initialize_config_custom(self):
        environment = {'EXECUTOR_MAX_BYTES_READ_PER_LINE': '1234',
                       'EXECUTOR_MAX_MESSAGE_LENGTH': '1024',
                       'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': '4secs',
                       'MESOS_SANDBOX': '/sandbox/location',
                       'PROGRESS_OUTPUT_FILE': 'progress_file',
                       'PROGRESS_REGEX_STRING': 'progress/regex',
                       'PROGRESS_SAMPLE_INTERVAL_MS': '2500'}
        config = cc.initialize_config(environment)

        self.assertEqual(1234, config.max_bytes_read_per_line)
        self.assertEqual(1024, config.max_message_length)
        self.assertEqual('progress_file', config.progress_output_name)
        self.assertEqual('progress/regex', config.progress_regex_string)
        self.assertEqual(2500, config.progress_sample_interval_ms)
        self.assertEqual('/sandbox/location', config.sandbox_directory)
        self.assertEqual(4000, config.shutdown_grace_period_ms)
