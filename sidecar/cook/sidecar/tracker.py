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
"""Implementation for Cook's sidecar progress reporter."""

import logging
import os
import re
import time
from threading import Event, Lock, Thread


class ProgressSequenceCounter:
    """Utility class that supports atomically incrementing the sequence value."""
    def __init__(self, initial=0):
        self.lock = Lock()
        self.value = initial

    def increment_and_get(self):
        """Atomically increments by one the current value and returns the new value."""
        with self.lock:
            self.value += 1
            return self.value


class ProgressUpdater(object):
    """This class is responsible for sending progress updates to the scheduler.
    It throttles the rate at which progress updates are sent.
    """

    def __init__(self, max_message_length, poll_interval_ms, send_progress_message_fn):
        """
        max_message_length: int
            The allowed max message length after encoding.
        poll_interval_ms: int
            The interval after which to send a subsequent progress update.
        send_progress_message_fn: function(message)
            The helper function used to send the progress message.
        """
        self.max_message_length = max_message_length
        self.poll_interval_ms = poll_interval_ms
        self.last_reported_time = None
        self.last_progress_data_sent = None
        self.send_progress_message = send_progress_message_fn
        self.lock = Lock()

    def has_enough_time_elapsed_since_last_update(self):
        """Returns true if enough time (based on poll_interval_ms) has elapsed since
        the last progress update (available in last_reported_time).
        """
        if self.last_reported_time is None:
            return True
        else:
            current_time = time.time()
            time_diff_ms = (current_time - self.last_reported_time) * 1000
            return time_diff_ms >= self.poll_interval_ms

    def is_increasing_sequence(self, progress_data):
        """Checks if the sequence number in progress_data is larger than the previously published progress.

        Parameters
        ----------
        progress_data: dictionary
            The progress data to send.

        Returns
        -------
        True if the sequence number in progress_data is larger than the previously published progress, False otherwise
        """
        last_progress_data = self.last_progress_data_sent
        last_progress_sequence = last_progress_data['progress-sequence'] if last_progress_data else -1
        return progress_data['progress-sequence'] > last_progress_sequence

    def send_progress_update(self, progress_data, force_send=False):
        """Sends a progress update if enough time has elapsed since the last progress update.
        The force_send flag can be used to ignore the check for enough time having elapsed.
        Using this method is thread-safe.

        Parameters
        ----------
        progress_data: dictionary
            The progress data to send.
        force_send: boolean, optional
            Defaults to false.

        Returns
        -------
        Nothing
        """
        with self.lock:
            # ensure we do not send outdated progress data due to parallel repeated calls to this method
            if progress_data is None or not self.is_increasing_sequence(progress_data):
                logging.info(f'Skipping invalid/outdated progress data {progress_data}')
            elif not force_send and not self.has_enough_time_elapsed_since_last_update():
                logging.debug('Not sending progress data as enough time has not elapsed since last update')
            else:
                logging.info(f'Sending progress message {progress_data}')
                message_dict = dict(progress_data)

                raw_progress_message = progress_data['progress-message']
                try:
                    progress_str = raw_progress_message.decode('ascii').strip()
                except UnicodeDecodeError:
                    logging.info('Unable to decode progress message in ascii, using empty string instead')
                    progress_str = ''

                if len(progress_str) <= self.max_message_length:
                    message_dict['progress-message'] = progress_str
                else:
                    allowed_progress_message_length = max(self.max_message_length - 3, 0)
                    new_progress_str = progress_str[:allowed_progress_message_length].strip() + '...'
                    logging.info(f'Progress message trimmed to {new_progress_str}')
                    message_dict['progress-message'] = new_progress_str

                send_success = self.send_progress_message(message_dict)
                if send_success:
                    self.last_progress_data_sent = progress_data
                    self.last_reported_time = time.time()
                else:
                    logging.info(f'Unable to send progress message {message_dict}')


class ProgressWatcher(object):
    """This class tails the output from the target file listening for progress messages.
    The retrieve_progress_states generates all progress messages iteratively.
    """

    def __init__(self, output_name, location_tag, sequence_counter,
                 max_bytes_read_per_line, progress_regex_string, stop_event):
        """The ProgressWatcher constructor.

        Parameters
        ----------
        progress_regex_string: string
            The progress regex to match against, it must return one or two capture groups.
            The first capture group represents the progress percentage.
            The second capture group, if present, represents the progress message.
        """
        self.target_file = output_name
        self.location_tag = location_tag
        self.sequence_counter = sequence_counter
        self.max_bytes_read_per_line = max_bytes_read_per_line
        self.progress_regex_string = progress_regex_string
        self.progress_regex_pattern = re.compile(progress_regex_string.encode())
        self.progress = None
        self.stop_event = stop_event

    def stopped(self):
        """Check if this progress tracker has been stopped."""
        return self.stop_event.is_set()

    def current_progress(self):
        """Returns the current progress dictionary."""
        return self.progress

    def tail(self, sleep_time_ms):
        """This method incrementally generates lines from a file by waiting for new content from a file.
        It behaves like the 'tail -f' shell command.

        Parameters
        ----------
        sleep_time_ms: int
            The unit of time in ms to repetitively sleep when the file has not been created or no new
            content is available in the file being tailed.

        Returns
        -------
        an incrementally generated list of lines in the file being tailed.
        """
        try:
            sleep_param = sleep_time_ms / 1000

            if os.path.exists(self.target_file) and not os.path.isfile(self.target_file):
                logging.info(f'Skipping progress monitoring on {self.target_file} as it is not a file')
                return

            if not os.path.isfile(self.target_file):
                logging.debug(f'Awaiting creation of file {self.target_file} [tag={self.location_tag}]')

            while not os.path.isfile(self.target_file):
                if self.stopped():
                    return
                time.sleep(sleep_param)

            if not os.path.isfile(self.target_file):
                logging.info(f'Progress output file has not been created [tag={self.location_tag}]')
                return

            logging.info(f'File has been created, reading contents [tag={self.location_tag}]')
            linesep_bytes = os.linesep.encode()
            fragment_index = 0
            line_index = 0

            post_stop_bytes_to_read = 2000000  # read max of 2 MB after stop signal comes in

            with open(self.target_file, 'rb') as target_file_obj:
                while True:
                    line = target_file_obj.readline(self.max_bytes_read_per_line)

                    if self.stopped():
                        # exit generator once input or post_stop_bytes_to_read is exhausted
                        if not line or post_stop_bytes_to_read <= 0:
                            return
                        # limit how much data to process after stop signal received
                        else:
                            post_stop_bytes_to_read -= len(line)

                    # no new data available, sleep before trying again
                    if not line:
                        time.sleep(sleep_param)
                        continue

                    fragment_index += 1
                    if line.endswith(linesep_bytes):
                        line_index += 1
                    yield line

        except Exception as exception:
            logging.exception(f'Error while tailing {self.target_file} [tag={self.location_tag}]')
            raise exception

    def match_progress_update(self, input_data):
        """Returns the progress tuple when the input string matches the provided regex.

        Parameters
        ----------
        input_data: bytes
            The input data.

        Returns
        -------
        the tuple (percent, message) if the string matches the provided regex,
                 else return None.
        """
        matches = self.progress_regex_pattern.findall(input_data)
        return matches[0] if len(matches) >= 1 else None

    def __update_progress(self, progress_report):
        """Updates the progress field with the data from progress_report if it is valid."""
        if isinstance(progress_report, tuple) and len(progress_report) == 2:
            percent_data, message_data = progress_report
        elif isinstance(progress_report, tuple) and len(progress_report) == 1:
            percent_data, message_data = progress_report[0], b''
        else:
            percent_data, message_data = progress_report, b''

        percent_float = float(percent_data.decode())
        if percent_float < 0 or percent_float > 100:
            logging.info(f'Skipping "{progress_report}" as the percent is not in [0, 100]')
            return False

        percent_int = int(round(percent_float))
        logging.debug(f'Updating progress to {percent_int} percent [tag={self.location_tag}]')

        self.progress = {'progress-message': message_data,
                         'progress-percent': percent_int,
                         'progress-sequence': self.sequence_counter.increment_and_get()}
        return True

    def retrieve_progress_states(self):
        """Generates the progress states by tailing the target_file.
        It tails a target file (using the tail() method) and uses the provided
        regex to find a match for a progress message. The regex is expected to
        generate two components in the match: the progress percent as an int and
        a progress message string. When such a message is found, this method
        yields the current progress as a dictionary.

        Note: This function must rethrow any OSError exceptions that it encounters.

        Returns
        -------
        An incrementally generated list of progress states.
        """
        if self.progress_regex_string:
            sleep_time_ms = 50
            for line in self.tail(sleep_time_ms):
                try:
                    progress_report = self.match_progress_update(line)
                    if progress_report is not None:
                        if self.__update_progress(progress_report):
                            yield self.progress
                except Exception:
                    logging.exception(f'Skipping progress line due to error: {line}')


class ProgressTracker(object):
    """Helper class to track progress messages from the specified location."""

    def __init__(self, config, counter, progress_updater, location, location_tag):
        """Launches the threads that track progress and send progress updates to the driver.

        Parameters
        ----------
        config: cook.sidecar.config.ExecutorConfig
            The current executor config.
        progress_updater: ProgressUpdater
            The progress updater used to send the progress messages
        counter: ProgressSequenceCounter
            The sequence counter
        location: string
            The target location to read for progress messages
        location_tag: string
            A tag to identify the target location."""
        self.location_tag = location_tag
        self.stop_event = Event()
        self.watcher = ProgressWatcher(location, location_tag, counter, config.max_bytes_read_per_line,
                                       config.progress_regex_string, self.stop_event)
        self.updater = progress_updater
        self.tracker_thread = Thread(target=self.track_progress, args=(), daemon=True)

    def start(self):
        """Launches a thread that starts monitoring the progress location for progress messages."""
        logging.info(f'Starting progress monitoring from [tag={self.location_tag}]')
        self.tracker_thread.start()

    def stop(self):
        """Signal this progress tracker thread to stop."""
        logging.info(f'Stop signal received on progress monitoring thread [tag={self.location_tag}]')
        self.stop_event.set()

    def wait(self):
        """Wait for this progress tracker to complete."""
        while self.tracker_thread.is_alive():
            self.tracker_thread.join(0.01)
        self.force_send_progress_update()
        logging.info(f'Completed progress monitoring from [tag={self.location_tag}]')

    def track_progress(self):
        """Retrieves and sends progress updates using send_progress_update_fn."""
        try:
            for current_progress in self.watcher.retrieve_progress_states():
                self.updater.send_progress_update(current_progress)
        except Exception:
            logging.exception(f'Exception while tracking progress [tag={self.location_tag}]')

    def force_send_progress_update(self):
        """Retrieves the latest progress message and attempts to force send it to the scheduler."""
        latest_progress = self.watcher.current_progress()
        self.updater.send_progress_update(latest_progress, force_send=True)
