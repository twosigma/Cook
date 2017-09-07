import json
import logging
import os
import random

from pymesos import decode_data


def get_random_task_id():
    return str(random.randint(100000, 999999))


def ensure_directory(output_filename):
    target_dir = os.path.dirname(output_filename)
    if not os.path.isdir(target_dir):
        os.mkdir(target_dir)
    return output_filename


def assert_status(testcase, expected_status, actual_status):
    assert actual_status['timestamp'] is not None
    if 'timestamp' in actual_status: del actual_status['timestamp']
    testcase.assertEquals(expected_status, actual_status)


def assert_message(testcase, expected_message, actual_encoded_message):
    actual_message = json.loads(decode_data(actual_encoded_message).decode('utf8'))
    testcase.assertEquals(expected_message, actual_message)


def cleanup_output(stdout_name, stderr_name):
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


class FakeMesosExecutorDriver(object):
    def __init__(self):
        self.messages = []
        self.statuses = []

    def sendFrameworkMessage(self, message):
        self.messages.append(message)

    def sendStatusUpdate(self, status):
        self.statuses.append(status)
