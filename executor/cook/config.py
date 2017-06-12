#!/usr/bin/env python3

import logging

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
    max_bytes_read_per_line = int(environment.get('EXECUTOR_MAX_BYTES_READ_PER_LINE', 4 * 1024))
    max_message_length = int(environment.get('EXECUTOR_MAX_MESSAGE_LENGTH', 512))
    progress_output_name = environment.get('PROGRESS_OUTPUT_FILE', 'stdout')
    progress_regex_string = environment.get('PROGRESS_REGEX_STRING', 'progress: (\d*), (.*)')
    progress_sample_interval_ms = int(environment.get('PROGRESS_SAMPLE_INTERVAL_MS', 1000))
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
