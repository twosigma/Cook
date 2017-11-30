import logging
import math
import re
import time
import unittest
from threading import Event, Thread

import os
from nose.tools import *

import cook.executor as ce
import cook.progress as cp
from tests.utils import assert_message, ensure_directory, get_random_task_id, FakeMesosExecutorDriver


class ProgressTest(unittest.TestCase):
    def test_match_progress_update(self):

        progress_regex_string = '\^\^\^\^JOB-PROGRESS: ([0-9]*\.?[0-9]+)(?: )?(.*)'
        progress_regex_pattern = re.compile(progress_regex_string)

        def match_progress_update(input_string):
            return cp.ProgressWatcher.match_progress_update(progress_regex_pattern, input_string)

        self.assertIsNone(match_progress_update("One percent complete"))
        self.assertIsNone(match_progress_update("^^^^JOB-PROGRESS 1 One percent complete"))
        self.assertIsNone(match_progress_update("JOB-PROGRESS: 1 One percent complete"))
        self.assertEqual(('1', ''),
                         match_progress_update("^^^^JOB-PROGRESS: 1"))
        self.assertEqual(('1', 'One percent complete'),
                         match_progress_update("^^^^JOB-PROGRESS: 1 One percent complete"))
        self.assertEqual(('50', 'Fifty percent complete'),
                         match_progress_update("^^^^JOB-PROGRESS: 50 Fifty percent complete"))
        # Fractions in progress update are also supported
        self.assertEqual(('2.2', ''),
                         match_progress_update("^^^^JOB-PROGRESS: 2.2"))
        self.assertEqual(('2.0', 'Two percent complete'),
                         match_progress_update("^^^^JOB-PROGRESS: 2.0 Two percent complete"))

    def test_send_progress_update(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        max_message_length = 100
        poll_interval_ms = 100

        def send_progress_message(message):
            ce.send_message(driver, message, max_message_length)
            message_string = str(message).encode('utf8')
            self.assertLessEqual(len(message_string), max_message_length)
            return len(message_string) <= max_message_length

        progress_updater = cp.ProgressUpdater(task_id, max_message_length, poll_interval_ms, send_progress_message)
        progress_data_0 = {'progress-message': 'Progress message-0'}
        progress_updater.send_progress_update(progress_data_0)

        self.assertEqual(1, len(driver.messages))
        actual_encoded_message_0 = driver.messages[0]
        expected_message_0 = {'progress-message': 'Progress message-0', 'task-id': task_id}
        assert_message(self, expected_message_0, actual_encoded_message_0)

        progress_data_1 = {'progress-message': 'Progress message-1'}
        progress_updater.send_progress_update(progress_data_1)

        self.assertEqual(1, len(driver.messages))

        time.sleep(poll_interval_ms / 1000.0)
        progress_data_2 = {'progress-message': 'Progress message-2'}
        progress_updater.send_progress_update(progress_data_2)

        self.assertEqual(2, len(driver.messages))
        actual_encoded_message_2 = driver.messages[1]
        expected_message_2 = {'progress-message': 'Progress message-2', 'task-id': task_id}
        assert_message(self, expected_message_2, actual_encoded_message_2)

    def test_send_progress_update_trims_progress_message(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        max_message_length = 100
        poll_interval_ms = 10

        def send_progress_message(message):
            ce.send_message(driver, message, max_message_length)
            message_string = str(message).encode('utf8')
            self.assertLessEqual(len(message_string), max_message_length)
            return len(message_string) <= max_message_length

        progress_updater = cp.ProgressUpdater(task_id, max_message_length, poll_interval_ms, send_progress_message)
        progress_data_0 = {'progress-message': 'Progress message-0 is really long lorem ipsum dolor sit amet text'}
        progress_updater.send_progress_update(progress_data_0)

        self.assertEqual(1, len(driver.messages))
        actual_encoded_message_0 = driver.messages[0]
        expected_message_0 = {'progress-message': 'Progress message-0 is really long lorem ipsum dolor...',
                              'task-id': task_id}
        assert_message(self, expected_message_0, actual_encoded_message_0)

    def test_send_progress_does_not_trim_unknown_field(self):
        driver = FakeMesosExecutorDriver()
        task_id = get_random_task_id()
        max_message_length = 100
        poll_interval_ms = 10

        def send_progress_message(message):
            ce.send_message(driver, message, max_message_length)
            message_string = str(message).encode('utf8')
            self.assertGreater(len(message_string), max_message_length)
            return len(message_string) <= max_message_length

        progress_updater = cp.ProgressUpdater(task_id, max_message_length, poll_interval_ms, send_progress_message)
        progress_data_0 = {'unknown': 'Unknown field has a really long lorem ipsum dolor sit amet exceed limit text'}
        progress_updater.send_progress_update(progress_data_0)

        self.assertEqual(0, len(driver.messages))

    def test_watcher_tail(self):
        file_name = ensure_directory('build/tail_progress_test.' + get_random_task_id())
        items_to_write = 12
        stop_signal = Event()
        completed_signal = Event()
        write_sleep_ms = 50
        tail_sleep_ms = 25

        try:
            def write_to_file():
                file = open(file_name, 'w+')
                for item in range(items_to_write):
                    time.sleep(write_sleep_ms / 1000.0)
                    file.write("{}\n".format(item))
                    file.flush()
                file.close()
                time.sleep(0.15)
                completed_signal.set()

            Thread(target=write_to_file, args=()).start()

            counter = cp.ProgressSequenceCounter()
            watcher = cp.ProgressWatcher(file_name, 'test', counter, 1024, '', stop_signal, completed_signal)
            collected_data = []
            for line in watcher.tail(tail_sleep_ms):
                collected_data.append(line.strip())

            self.assertEqual(items_to_write, len(collected_data))
            self.assertEqual(list(map(lambda x: str(x), range(items_to_write))), collected_data)
        finally:
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_watcher_tail_lot_of_writes(self):
        file_name = ensure_directory('build/tail_progress_test.' + get_random_task_id())
        items_to_write = 250000
        stop_signal = Event()
        completed_signal = Event()
        tail_sleep_ms = 25

        try:
            def write_to_file():
                file = open(file_name, 'w+')
                for item in range(items_to_write):
                    file.write("line-{}\n".format(item))
                    if item % 100 == 0:
                        file.flush()
                file.flush()
                file.close()
                time.sleep(0.15)
                completed_signal.set()

            Thread(target=write_to_file, args=()).start()

            counter = cp.ProgressSequenceCounter()
            watcher = cp.ProgressWatcher(file_name, 'test', counter, 1024, '', stop_signal, completed_signal)
            collected_data = []
            for line in watcher.tail(tail_sleep_ms):
                collected_data.append(line.strip())

            logging.info('Items read: {}'.format(len(collected_data)))
            if items_to_write != len(collected_data):
                for index in range(len(collected_data)):
                    logging.info('{}: {}'.format(index, collected_data[index]))
            self.assertEqual(items_to_write, len(collected_data))
            expected_data = list(map(lambda x: 'line-{}'.format(x), range(items_to_write)))
            self.assertEqual(expected_data, collected_data)
        finally:
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_watcher_tail_with_read_limit(self):
        file_name = ensure_directory('build/tail_progress_test.' + get_random_task_id())
        stop_signal = Event()
        completed_signal = Event()
        tail_sleep_ms = 25

        try:
            def write_to_file():
                file = open(file_name, 'w+')

                file.write("abcd\n")
                file.flush()

                file.write("abcdefghijkl\n")
                file.flush()

                file.write("abcdefghijklmnopqrstuvwxyz\n")
                file.flush()

                file.close()
                time.sleep(0.15)
                completed_signal.set()

            Thread(target=write_to_file, args=()).start()

            counter = cp.ProgressSequenceCounter()
            watcher = cp.ProgressWatcher(file_name, 'test', counter, 10, '', stop_signal, completed_signal)
            collected_data = []
            for line in watcher.tail(tail_sleep_ms):
                collected_data.append(line.strip())

            logging.debug('collected_data = {}'.format(collected_data))
            expected_data = ['abcd',
                             'abcdefghij', 'kl',
                             'abcdefghij', 'klmnopqrst', 'uvwxyz']
            self.assertEqual(expected_data, collected_data)
        finally:
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_collect_progress_updates(self):
        file_name = ensure_directory('build/collect_progress_test.' + get_random_task_id())
        progress_regex = '\^\^\^\^JOB-PROGRESS: ([0-9]*\.?[0-9]+)(?: )?(.*)'
        stop_signal = Event()
        completed_signal = Event()

        file = open(file_name, 'w+')
        file.flush()
        counter = cp.ProgressSequenceCounter()
        watcher = cp.ProgressWatcher(file_name, 'test', counter, 1024, progress_regex, stop_signal, completed_signal)

        try:
            def read_progress_states():
                for _ in watcher.retrieve_progress_states():
                    pass

            Thread(target=read_progress_states, args=()).start()

            file.write("Stage One complete\n")
            file.flush()
            file.write("^^^^JOB-PROGRESS: 25 Twenty-Fine percent\n")
            file.flush()
            file.write("Stage Two complete\n")
            file.flush()
            file.write("^^^^JOB-PROGRESS: 50 Fifty percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertEqual({'progress-message': 'Fifty percent', 'progress-percent': 50, 'progress-sequence': 2},
                             watcher.current_progress())

            file.write("Stage Three complete\n")
            file.flush()

            time.sleep(0.10)
            self.assertEqual({'progress-message': 'Fifty percent', 'progress-percent': 50, 'progress-sequence': 2},
                             watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 55.0 Fifty-five percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertEqual({'progress-message': 'Fifty-five percent', 'progress-percent': 55, 'progress-sequence': 3},
                             watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 65.8 Sixty-six percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertEqual({'progress-message': 'Sixty-six percent', 'progress-percent': 66, 'progress-sequence': 4},
                             watcher.current_progress())

            file.write("Stage Four complete\n")
            file.flush()
            file.write("^^^^JOB-PROGRESS: 100 Hundred percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertEqual({'progress-message': 'Hundred percent', 'progress-percent': 100, 'progress-sequence': 5},
                             watcher.current_progress())

        finally:
            completed_signal.set()
            file.close()
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_collect_progress_updates_skip_faulty(self):
        file_name = ensure_directory('build/collect_progress_updates_skip_faulty.' + get_random_task_id())
        progress_regex = '\^\^\^\^JOB-PROGRESS: ([0-9]*\.?[0-9]+)(?: )?(.*)'
        stop_signal = Event()
        completed_signal = Event()

        file = open(file_name, 'w+')
        file.flush()
        counter = cp.ProgressSequenceCounter()
        watcher = cp.ProgressWatcher(file_name, 'test', counter, 1024, progress_regex, stop_signal, completed_signal)

        try:
            def read_progress_states():
                for _ in watcher.retrieve_progress_states():
                    pass

            Thread(target=read_progress_states, args=()).start()

            file.write("^^^^JOB-PROGRESS: F50 Fifty percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 200 Two-hundred percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 121212121212121212 Huge percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 075 75% percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertEqual({'progress-message': '75% percent', 'progress-percent': 75, 'progress-sequence': 1},
                             watcher.current_progress())

        finally:
            completed_signal.set()
            file.close()
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_collect_progress_updates_faulty_regex(self):
        file_name = ensure_directory('build/collect_progress_updates_skip_faulty.' + get_random_task_id())
        progress_regex = '\^\^\^\^JOB-PROGRESS: (\S+)(?: )?(.*)'
        stop_signal = Event()
        completed_signal = Event()

        file = open(file_name, 'w+')
        file.flush()
        counter = cp.ProgressSequenceCounter()
        watcher = cp.ProgressWatcher(file_name, 'test', counter, 1024, progress_regex, stop_signal, completed_signal)

        try:
            def read_progress_states():
                for _ in watcher.retrieve_progress_states():
                    pass

            Thread(target=read_progress_states, args=()).start()

            file.write("^^^^JOB-PROGRESS: ABCDEF string percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: F50 Fifty percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 1019101010101010101010101018101101010101010110171010110 Sixty percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 75 75% percent\n")
            file.flush()
            time.sleep(0.10)
            self.assertEqual({'progress-message': '75% percent', 'progress-percent': 75, 'progress-sequence': 1},
                             watcher.current_progress())

        finally:
            completed_signal.set()
            file.close()
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_collect_progress_updates_dev_null(self):
        file_name = ensure_directory('build/collect_progress_test.' + get_random_task_id())
        progress_regex = '\^\^\^\^JOB-PROGRESS: ([0-9]*\.?[0-9]+)(?: )?(.*)'
        location = '/dev/null'
        stop_signal = Event()
        completed_signal = Event()

        file = open(file_name, 'w+')
        file.flush()
        counter = cp.ProgressSequenceCounter()
        dn_watcher = cp.ProgressWatcher(location, 'dn', counter, 1024, progress_regex, stop_signal, completed_signal)
        out_watcher = cp.ProgressWatcher(file_name, 'so', counter, 1024, progress_regex, stop_signal, completed_signal)

        try:
            def read_progress_states(watcher):
                for _ in watcher.retrieve_progress_states():
                    pass

            Thread(target=read_progress_states, args=(dn_watcher,)).start()
            Thread(target=read_progress_states, args=(out_watcher,)).start()

            file.write("Stage One complete\n")
            file.flush()
            file.write("^^^^JOB-PROGRESS: 100 Hundred percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertIsNone(dn_watcher.current_progress())
            self.assertEqual({'progress-message': 'Hundred percent', 'progress-percent': 100, 'progress-sequence': 1},
                             out_watcher.current_progress())

        finally:
            completed_signal.set()
            file.close()
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_collect_progress_updates_lots_of_writes(self):
        file_name = ensure_directory('build/collect_progress_test.' + get_random_task_id())
        progress_regex = 'progress: ([0-9]*\.?[0-9]+), (.*)'
        items_to_write = 250000
        stop_signal = Event()
        completed_signal = Event()

        def write_to_file():
            target_file = open(file_name, 'w+')
            unit_progress_granularity = int(items_to_write / 100)

            for item in range(items_to_write):
                remainder = (item + 1) % unit_progress_granularity
                if remainder == 0:
                    progress_percent = math.ceil(item / unit_progress_granularity)
                    target_file.write('progress: {0}, completed-{0}-percent\n'.format(progress_percent))
                    target_file.flush()
                target_file.write("{}\n".format(item))
            target_file.flush()

            target_file.close()
            time.sleep(0.15)
            completed_signal.set()

        write_thread = Thread(target=write_to_file, args=())
        write_thread.start()

        counter = cp.ProgressSequenceCounter()
        watcher = cp.ProgressWatcher(file_name, 'test', counter, 1024, progress_regex, stop_signal, completed_signal)

        try:
            collected_data = []

            def read_progress_states():
                for progress in watcher.retrieve_progress_states():
                    logging.info('Received: {}'.format(progress))
                    collected_data.append(progress)

            read_progress_states_thread = Thread(target=read_progress_states, args=())
            read_progress_states_thread.start()

            read_progress_states_thread.join()

            expected_data = list(map(lambda x: {'progress-message': 'completed-{}-percent'.format(x),
                                                'progress-percent': x,
                                                'progress-sequence': x},
                                     range(1, 101)))

            self.assertEqual(expected_data, collected_data)
        finally:
            completed_signal.set()
            if os.path.isfile(file_name):
                os.remove(file_name)

    def test_collect_progress_updates_with_empty_regex(self):
        file_name = ensure_directory('build/collect_progress_test.' + get_random_task_id())
        progress_regex = ''
        stop_signal = Event()
        completed_signal = Event()

        file = open(file_name, 'w+')
        file.flush()
        counter = cp.ProgressSequenceCounter()
        watcher = cp.ProgressWatcher(file_name, 'test', counter, 1024, progress_regex, stop_signal, completed_signal)

        try:
            def read_progress_states():
                for _ in watcher.retrieve_progress_states():
                    pass

            Thread(target=read_progress_states, args=()).start()

            file.write("Stage One complete\n")
            file.flush()
            file.write("^^^^JOB-PROGRESS: 25 Twenty-Fine percent\n")
            file.flush()
            file.write("Stage Two complete\n")
            file.flush()
            file.write("^^^^JOB-PROGRESS: 50 Fifty percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("Stage Three complete\n")
            file.flush()

            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("^^^^JOB-PROGRESS: 55.0 Fifty-five percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

            file.write("Stage Four complete\n")
            file.flush()
            file.write("^^^^JOB-PROGRESS: 100 Hundred percent\n")
            file.flush()

            time.sleep(0.10)
            self.assertIsNone(watcher.current_progress())

        finally:
            completed_signal.set()
            file.close()
            if os.path.isfile(file_name):
                os.remove(file_name)
