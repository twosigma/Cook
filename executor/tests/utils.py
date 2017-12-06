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


def assert_status(test_case, expected_status, actual_status):
    assert actual_status['timestamp'] is not None
    if 'timestamp' in actual_status: del actual_status['timestamp']
    test_case.assertEquals(expected_status, actual_status)


def parse_message(encoded_message):
    return json.loads(decode_data(encoded_message).decode('utf8'))


def assert_message(test_case, expected_message, actual_encoded_message):
    actual_message = parse_message(actual_encoded_message)
    test_case.assertEquals(expected_message, actual_message)


def assert_messages(test_case, expected_process_messages, expected_progress_messages, driver_messages):
    logging.info('Messages: {}'.format(map(parse_message, driver_messages)))
    test_case.assertEqual(len(expected_process_messages) + len(expected_progress_messages), len(driver_messages))
    for i in range(0, len(driver_messages)):
        actual_message = parse_message(driver_messages[i])
        if (('exit-code' in actual_message or 'sandbox-directory' in actual_message) and
                len(expected_process_messages) > 0):
            expected_message = expected_process_messages.pop(0)
            test_case.assertEquals(expected_message, actual_message)
        elif 'progress-sequence' in actual_message and len(expected_progress_messages) > 0:
            expected_message = expected_progress_messages.pop(0)
            test_case.assertEquals(expected_message, actual_message)
        else:
            test_case.fail('Unexpected message: {}'.format(actual_message))


def assert_statuses(test_case, expected_statuses, driver_statuses):
    logging.info('Statuses: {}'.format(driver_statuses))
    test_case.assertEqual(len(expected_statuses), len(driver_statuses))
    for i in range(1, len(expected_statuses)):
        expected_status = expected_statuses[i]
        actual_status = driver_statuses[i]
        assert_status(test_case, expected_status, actual_status)


def close_sys_outputs():
    if not sys.stdout.closed:
        sys.stdout.flush()
        sys.stdout.close()

    if not sys.stderr.closed:
        sys.stderr.flush()
        sys.stderr.close()


def cleanup_output(stdout_name, stderr_name):

    close_sys_outputs()

    def read_and_print_contents(f, name):
        file_contents = f.read()
        logging.debug('==========================================')
        logging.debug('Contents of {}:'.format(name))
        logging.debug(file_contents)

    def process_file_name(file_name):
        if os.path.isfile(file_name):
            try:
                with open(file_name, encoding='ascii') as f:
                    read_and_print_contents(f, file_name)
            except UnicodeDecodeError:
                try:
                    with open(file_name, 'r', encoding='cp437') as f:
                        read_and_print_contents(f, file_name)
                except UnicodeDecodeError:
                    with open(file_name, 'rb') as f:
                        read_and_print_contents(f, file_name)
        os.remove(file_name)

    process_file_name(stdout_name)
    process_file_name(stderr_name)

    reset_stdout()
    reset_stderr()


def os_error_handler_stub(_):
    logging.exception('Test generated OSError')


class FakeExecutorConfig(object):
    def __init__(self, config_map):
        self.config_map = config_map

    def __getattribute__(self, name):
        if name == 'config_map' or name == 'stderr_file' or name == 'stdout_file':
            return object.__getattribute__(self, name)
        else:
            return self.config_map[name]

    def stderr_file(self):
        return self.config_map['stderr_file']

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
