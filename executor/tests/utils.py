import json
import logging
import os
import random
import sys
import time

from pymesos import decode_data


def wait_for(query_fn, predicate_fn, default_value=None, max_delay_ms=1000, wait_interval_ms=100):
    iterations = max(1, int(max_delay_ms / wait_interval_ms))
    for _ in range(iterations):
        data = query_fn()
        if predicate_fn(data):
            return data
        time.sleep(wait_interval_ms / 1000.0)
    return default_value


def get_random_task_id():
    return str(random.randint(100000, 999999))


def ensure_directory(output_filename):
    """"Ensures that the directory that contains output_filename is created before returning output_filename."""
    target_dir = os.path.dirname(output_filename)
    if not os.path.isdir(target_dir):
        os.mkdir(target_dir)
    return output_filename


def reset_stdout():
    sys.stdout = sys.__stdout__


def reset_stderr():
    sys.stderr = sys.__stderr__


def initialize_file(output_filename):
    output_filename = ensure_directory(output_filename)
    output_file = open(output_filename, 'w+')
    return output_file


def redirect_stdout_to_file(output_filename):
    sys.stdout = initialize_file(output_filename)


def redirect_stderr_to_file(output_filename):
    sys.stderr = initialize_file(output_filename)


def assert_status(testcase, expected_status, actual_status):
    assert actual_status['timestamp'] is not None
    if 'timestamp' in actual_status: del actual_status['timestamp']
    testcase.assertEquals(expected_status, actual_status)


def parse_message(encoded_message):
    return json.loads(decode_data(encoded_message).decode('utf8'))


def assert_message(testcase, expected_message, actual_encoded_message):
    actual_message = parse_message(actual_encoded_message)
    testcase.assertEquals(expected_message, actual_message)


def close_sys_outputs():
    if not sys.stdout.closed:
        sys.stdout.flush()
        sys.stdout.close()

    if not sys.stderr.closed:
        sys.stderr.flush()
        sys.stderr.close()


def cleanup_output(stdout_name, stderr_name):

    close_sys_outputs()

    if os.path.isfile(stdout_name):
        with open(stdout_name) as f:
            logging.debug('==========================================')
            logging.debug('Contents of {}:'.format(stdout_name))
            logging.debug(f.read())
        os.remove(stdout_name)
    if os.path.isfile(stderr_name):
        with open(stderr_name) as f:
            logging.debug('==========================================')
            logging.debug('Contents of {}:'.format(stderr_name))
            logging.debug(f.read())
        os.remove(stderr_name)

    reset_stdout()
    reset_stderr()


class FakeExecutorConfig(object):
    def __init__(self, config_map):
        self.config_map = config_map

    def __getattribute__(self, name):
        logging.info('accessing {}'.format(name))
        if name == 'config_map' or name == 'stdout_file':
            return object.__getattribute__(self, name)
        else:
            return self.config_map[name]

    def stdout_file(self):
        return self.config_map['stdout_file']


class FakeMesosExecutorDriver(object):
    def __init__(self):
        self.messages = []
        self.statuses = []

    def sendFrameworkMessage(self, message):
        self.messages.append(message)

    def sendStatusUpdate(self, status):
        self.statuses.append(status)
