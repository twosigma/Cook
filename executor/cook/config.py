#!/usr/bin/env python3

import logging
import os

from pymesos.utils import parse_duration

DEFAULT_PROGRESS_FILE_ENV_VARIABLE = 'EXECUTOR_PROGRESS_OUTPUT_FILE'


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
        except Exception:
            logging.exception('Unable to extract seconds from {}'.format(time_string))
            logging.info('Defaulting time to 1 second.')
            return 1000

    def __init__(self,
                 checkpoint=0,
                 max_bytes_read_per_line=1024,
                 max_message_length=512,
                 memory_usage_interval_secs=15,
                 mesos_directory='',
                 progress_output_env_variable=DEFAULT_PROGRESS_FILE_ENV_VARIABLE,
                 progress_output_name='stdout',
                 progress_regex_string='',
                 progress_sample_interval_ms=100,
                 recovery_timeout='15mins',
                 reset_vars=[],
                 sandbox_directory='',
                 shutdown_grace_period='1secs'):
        self.checkpoint = checkpoint != 0
        self.max_bytes_read_per_line = max_bytes_read_per_line
        self.max_message_length = max_message_length
        self.memory_usage_interval_secs = memory_usage_interval_secs
        self.mesos_directory = mesos_directory
        self.progress_output_env_variable = progress_output_env_variable
        self.progress_output_name = progress_output_name
        self.progress_regex_string = progress_regex_string
        self.progress_sample_interval_ms = progress_sample_interval_ms
        self.recovery_timeout_ms = ExecutorConfig.parse_time_ms(recovery_timeout)
        self.reset_vars=reset_vars
        self.sandbox_directory = sandbox_directory
        self.shutdown_grace_period_ms = ExecutorConfig.parse_time_ms(shutdown_grace_period)

    def sandbox_file(self, file):
        return os.path.join(self.sandbox_directory, file)

    def stderr_file(self):
        return self.sandbox_file('stderr')

    def stdout_file(self):
        return self.sandbox_file('stdout')


def initialize_config(environment):
    """Initializes the config using the environment.
    Populates the default values for missing environment variables.
    """
    checkpoint = int(environment.get('MESOS_CHECKPOINT', '0'))
    executor_id = environment.get('MESOS_EXECUTOR_ID', 'executor')
    sandbox_directory = environment.get('MESOS_SANDBOX', '')
    mesos_directory = environment.get('MESOS_DIRECTORY', '')
    default_progress_output_key = 'EXECUTOR_DEFAULT_PROGRESS_OUTPUT_NAME'
    default_progress_output_name = environment.get(default_progress_output_key, '{}.progress'.format(executor_id))
    if sandbox_directory:
        default_progress_output_file = os.path.join(sandbox_directory, default_progress_output_name)
    else:
        default_progress_output_file = default_progress_output_name

    progress_output_env_key = 'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV'
    progress_output_env_variable = environment.get(progress_output_env_key, DEFAULT_PROGRESS_FILE_ENV_VARIABLE)
    logging.info('Progress location environment variable is {}'.format(progress_output_env_variable))
    if progress_output_env_variable not in environment:
        logging.info('No entry found for {} in the environment'.format(progress_output_env_variable))

    max_bytes_read_per_line = max(int(environment.get('EXECUTOR_MAX_BYTES_READ_PER_LINE', 4 * 1024)), 128)
    max_message_length = max(int(environment.get('EXECUTOR_MAX_MESSAGE_LENGTH', 512)), 64)
    memory_usage_interval_secs = max(int(environment.get('EXECUTOR_MEMORY_USAGE_INTERVAL_SECS', 3600)), 30)
    progress_output_name = environment.get(progress_output_env_variable, default_progress_output_file)
    progress_regex_string = environment.get('PROGRESS_REGEX_STRING', 'progress: ([0-9]*\.?[0-9]+), (.*)')
    progress_sample_interval_ms = max(int(environment.get('PROGRESS_SAMPLE_INTERVAL_MS', 1000)), 100)
    recovery_timeout = environment.get('MESOS_RECOVERY_TIMEOUT', '15mins')
    reset_vars = [v for v in environment.get('EXECUTOR_RESET_VARS', '').split(',')
                  if len(v) > 0]
    sandbox_directory = environment.get('MESOS_SANDBOX', '')
    shutdown_grace_period = environment.get('MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD', '2secs')

    logging.info('Checkpoint: {} with recovery timeout {}'.format(checkpoint, recovery_timeout))
    logging.info('Max bytes read per line is {}'.format(max_bytes_read_per_line))
    logging.info('Memory usage will be logged every {} secs'.format(memory_usage_interval_secs))
    logging.info('Progress message length is limited to {}'.format(max_message_length))
    logging.info('Progress output file is {}'.format(progress_output_name))
    logging.info('Progress regex is {}'.format(progress_regex_string))
    logging.info('Progress sample interval is {}'.format(progress_sample_interval_ms))
    logging.info('Reset vars are {}'.format(reset_vars))
    logging.info('Sandbox location is {}'.format(sandbox_directory))
    logging.info('Mesos directory is {}'.format(mesos_directory))
    logging.info('Shutdown grace period is {}'.format(shutdown_grace_period))

    return ExecutorConfig(checkpoint=checkpoint,
                          max_bytes_read_per_line=max_bytes_read_per_line,
                          max_message_length=max_message_length,
                          memory_usage_interval_secs=memory_usage_interval_secs,
                          mesos_directory=mesos_directory,
                          progress_output_env_variable=progress_output_env_variable,
                          progress_output_name=progress_output_name,
                          progress_regex_string=progress_regex_string,
                          progress_sample_interval_ms=progress_sample_interval_ms,
                          recovery_timeout=recovery_timeout,
                          reset_vars=reset_vars,
                          sandbox_directory=sandbox_directory,
                          shutdown_grace_period=shutdown_grace_period)
