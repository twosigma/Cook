#
#  Copyright (c) 2020 Two Sigma Open Source, LLC
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to
#  deal in the Software without restriction, including without limitation the
#  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
#  IN THE SOFTWARE.
#
"""Configuration loading for Cook's sidecar progress reporter."""

import logging
import os


class ProgressReporterConfig(object):
    """This class is responsible for storing the progress reporter config."""

    def __init__(self,
                 callback_url,
                 max_bytes_read_per_line,
                 max_message_length,
                 max_post_attempts,
                 max_post_time_secs,
                 progress_output_env_variable,
                 progress_output_name,
                 progress_regex_string,
                 progress_sample_interval_ms,
                 sandbox_directory):
        self.callback_url = callback_url
        self.max_bytes_read_per_line = max_bytes_read_per_line
        self.max_message_length = max_message_length
        self.max_post_attempts = max_post_attempts
        self.max_post_time_secs = max_post_attempts
        self.progress_output_env_variable = progress_output_env_variable
        self.progress_output_name = progress_output_name
        self.progress_regex_string = progress_regex_string
        self.progress_sample_interval_ms = progress_sample_interval_ms
        self.sandbox_directory = sandbox_directory

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
    instance_id = environment.get('COOK_INSTANCE_UUID')
    if instance_id is None:
        raise Exception('Task unknown! COOK_INSTANCE_UUID not set in environment.')
    job_uuid = environment.get('COOK_JOB_UUID')
    if job_uuid is None:
        raise Exception('Job unknown! job_uuid not set in environment.')

    cook_scheduler_rest_url = environment.get('COOK_SCHEDULER_REST_URL')
    if cook_scheduler_rest_url is None:
        raise Exception('REST URL unknown! COOK_SCHEDULER_REST_URL not set in environment.')

    callback_url = f'{cook_scheduler_rest_url}/progress/{instance_id}'

    sandbox_directory = environment.get('COOK_WORKDIR', '')

    default_progress_output_key = 'EXECUTOR_PROGRESS_OUTPUT_FILE'
    progress_output_env_key = 'EXECUTOR_PROGRESS_OUTPUT_FILE_ENV'
    progress_output_env_variable = environment.get(progress_output_env_key, default_progress_output_key)
    logging.info(f'Progress location environment variable is {progress_output_env_variable}')
    if progress_output_env_variable not in environment:
        logging.info(f'No entry found for {progress_output_env_variable} in the environment')

    default_progress_output_name = environment.get(progress_output_env_variable, f'{job_uuid}.progress')
    if sandbox_directory:
        default_progress_output_file = os.path.join(sandbox_directory, default_progress_output_name)
    else:
        default_progress_output_file = default_progress_output_name

    max_bytes_read_per_line = max(int(environment.get('EXECUTOR_MAX_BYTES_READ_PER_LINE', 4 * 1024)), 128)
    max_message_length = max(int(environment.get('EXECUTOR_MAX_MESSAGE_LENGTH', 512)), 64)
    max_post_attempts = max(int(environment.get('PROGRESS_MAX_POST_ATTEMPTS', 5)), 1)
    max_post_time_secs = float(environment.get('PROGRESS_MAX_POST_TIME_SECS', 10.0))
    progress_output_name = environment.get(progress_output_env_variable, default_progress_output_file)
    progress_regex_string = environment.get('PROGRESS_REGEX_STRING', r'progress: ([0-9]*\.?[0-9]+), (.*)')
    progress_sample_interval_ms = max(int(environment.get('PROGRESS_SAMPLE_INTERVAL_MS', 1000)), 100)
    sandbox_directory = environment.get('COOK_WORKDIR', '')

    if sandbox_directory and not progress_output_name.startswith('/'):
        progress_output_name = os.path.join(sandbox_directory, progress_output_name)

    logging.info(f'Job instance UUID is {instance_id}')
    logging.info(f'Progress update callback url is {callback_url}')
    logging.info(f'Max bytes read per line is {max_bytes_read_per_line}')
    logging.info(f'Progress message length is limited to {max_message_length}')
    logging.info(f'Progress post attempts limit is {max_post_attempts}')
    logging.info(f'Post request timeout {max_post_time_secs}')
    logging.info(f'Progress output file is {progress_output_name}')
    logging.info(f'Progress regex is {progress_regex_string}')
    logging.info(f'Progress sample interval is {progress_sample_interval_ms}')
    logging.info(f'Sandbox location is {sandbox_directory}')

    return ProgressReporterConfig(callback_url=callback_url,
                                  max_bytes_read_per_line=max_bytes_read_per_line,
                                  max_message_length=max_message_length,
                                  max_post_attempts=max_post_attempts,
                                  max_post_time_secs=max_post_time_secs,
                                  progress_output_env_variable=progress_output_env_variable,
                                  progress_output_name=progress_output_name,
                                  progress_regex_string=progress_regex_string,
                                  progress_sample_interval_ms=progress_sample_interval_ms,
                                  sandbox_directory=sandbox_directory)
