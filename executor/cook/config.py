#!/usr/bin/env python3

import logging
import os

from pymesos.utils import parse_duration


class ExecutorConfig(object):
    """This class is responsible for storing the executor config."""

    @staticmethod
    def parse_time_ms(time_string):
        """Parses the time string and return the milliseconds value.
        
        Parameters
        ----------
        time_string: string
            the input string to parse.
            
        Returns
        -------
        the parsed time as milliseconds. 
        """
        try:
            return int(1000 * parse_duration(time_string))
        except:
            logging.exception('Unable to extract seconds from {}'.format(time_string))
            logging.info('Defaulting time to 1 second.')
            return 1000

    def __init__(self,
                 max_bytes_read_per_line=1024,
                 max_message_length=512,
                 progress_output_name='stdout',
                 progress_regex_string='',
                 progress_sample_interval_ms=100,
                 sandbox_directory='',
                 shutdown_grace_period='1secs'):
        self.max_bytes_read_per_line = max_bytes_read_per_line
        self.max_message_length = max_message_length
        self.progress_output_name = progress_output_name
        self.progress_regex_string = progress_regex_string
        self.progress_sample_interval_ms = progress_sample_interval_ms
        self.sandbox_directory = sandbox_directory
        self.shutdown_grace_period_ms = ExecutorConfig.parse_time_ms(shutdown_grace_period)


def initialize_config(environment):
    """Initializes the config using the environment.
    Populates the default values for missing environment variables.
    """
    executor_id = environment.get('MESOS_EXECUTOR_ID', 'executor')
    sandbox_directory = environment.get('MESOS_SANDBOX', '')
    default_progress_output_name = '{}.progress'.format(executor_id)
    if sandbox_directory:
        default_progress_output_file = os.path.join(sandbox_directory, default_progress_output_name)
    else:
        default_progress_output_file = default_progress_output_name

    progress_output_file_env = environment.get('EXECUTOR_PROGRESS_OUTPUT_FILE_ENV', 'PROGRESS_OUTPUT_FILE')
    logging.info('Progress location environment variable is {}'.format(progress_output_file_env))
    if progress_output_file_env not in environment:
        logging.warning('No entry found for {} in the environment'.format(progress_output_file_env))

    max_bytes_read_per_line = max(int(environment.get('EXECUTOR_MAX_BYTES_READ_PER_LINE', 4 * 1024)), 128)
    max_message_length = max(int(environment.get('EXECUTOR_MAX_MESSAGE_LENGTH', 512)), 64)
    progress_output_name = environment.get(progress_output_file_env, default_progress_output_file)
    progress_regex_string = environment.get('PROGRESS_REGEX_STRING', 'progress: (\d+), (.*)')
    progress_sample_interval_ms = max(int(environment.get('PROGRESS_SAMPLE_INTERVAL_MS', 1000)), 100)
    sandbox_directory = environment.get('MESOS_SANDBOX', '')
    shutdown_grace_period = environment.get('MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD', '2secs')

    logging.info('Max bytes read per line is {}'.format(max_bytes_read_per_line))
    logging.info('Max message length is {}'.format(max_message_length))
    logging.info('Progress output file is {}'.format(progress_output_name))
    logging.info('Progress regex is {}'.format(progress_regex_string))
    logging.info('Progress sample interval is {}'.format(progress_sample_interval_ms))
    logging.info('Sandbox location is {}'.format(sandbox_directory))
    logging.info('Shutdown grace period is {}'.format(shutdown_grace_period))

    return ExecutorConfig(max_bytes_read_per_line=max_bytes_read_per_line,
                          max_message_length=max_message_length,
                          progress_output_name=progress_output_name,
                          progress_regex_string=progress_regex_string,
                          progress_sample_interval_ms=progress_sample_interval_ms,
                          sandbox_directory=sandbox_directory,
                          shutdown_grace_period=shutdown_grace_period)
