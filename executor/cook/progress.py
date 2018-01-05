import logging
import os
import re
import time
from threading import Event, Lock, Thread

import cook.util as cu

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

    def __init__(self, task_id, max_message_length, poll_interval_ms, send_progress_message_fn):
        """
        task_id: string
            The task id.
        max_message_length: int
            The allowed max message length after encoding.
        poll_interval_ms: int
            The interval after which to send a subsequent progress update.
        send_progress_message_fn: function(message)
            The helper function used to send the progress message.
        """
        self.task_id = task_id
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
                logging.info('Skipping invalid/outdated progress data {}'.format(progress_data))
            elif not force_send and not self.has_enough_time_elapsed_since_last_update():
                logging.debug('Not sending progress data as enough time has not elapsed since last update')
            else:
                logging.info('Sending progress message {}'.format(progress_data))
                message_dict = dict(progress_data)
                message_dict['task-id'] = self.task_id

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
                    logging.info('Progress message trimmed to {}'.format(new_progress_str))
                    message_dict['progress-message'] = new_progress_str

                send_success = self.send_progress_message(message_dict)
                if send_success:
                    self.last_progress_data_sent = progress_data
                    self.last_reported_time = time.time()
                else:
                    logging.info('Unable to send progress message {}'.format(message_dict))


class ProgressWatcher(object):
    """This class tails the output from the target file listening for progress messages.
    The retrieve_progress_states generates all progress messages iteratively.
    """

    def __init__(self, output_name, location_tag, sequence_counter, max_bytes_read_per_line, progress_regex_string,
                 stop_signal, task_completed_signal, progress_termination_signal):
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
        self.stop_signal = stop_signal
        self.task_completed_signal = task_completed_signal
        self.progress_termination_signal = progress_termination_signal

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
                logging.info('Skipping progress monitoring on %s as it is not a file', self.target_file)
                return

            if not os.path.isfile(self.target_file):
                logging.debug('Awaiting creation of file %s [tag=%s]', self.target_file, self.location_tag)

            while not os.path.isfile(self.target_file) and not self.task_completed_signal.isSet():
                time.sleep(sleep_param)

            if not os.path.isfile(self.target_file):
                logging.info('Progress output file has not been created [tag=%s]', self.location_tag)
                return

            if self.stop_signal.isSet():
                logging.info('Parsing progress messages interrupted [tag=%s]', self.location_tag)
                return

            logging.info('File has been created, reading contents [tag=%s]', self.location_tag)
            linesep_bytes = os.linesep.encode()
            fragment_index = 0
            line_index = 0

            def log_tail_summary():
                log_message = '%s fragments and %s lines read while processing progress messages [tag=%s]'
                logging.info(log_message, fragment_index, line_index, self.location_tag)

            with open(self.target_file, 'rb') as target_file_obj:
                while not self.stop_signal.isSet():
                    if self.progress_termination_signal.isSet():
                        logging.info('tail short-circuiting due to progress termination [tag=%s]', self.location_tag)
                        log_tail_summary()
                        break
                    line = target_file_obj.readline(self.max_bytes_read_per_line)
                    if not line:
                        # exit if program has completed and there are no more lines to read
                        if self.task_completed_signal.isSet():
                            log_tail_summary()
                            break
                        # no new line available, sleep before trying again
                        time.sleep(sleep_param)
                        continue

                    fragment_index += 1
                    if line.endswith(linesep_bytes):
                        line_index += 1
                    yield line
                if self.stop_signal.isSet() and not self.task_completed_signal.isSet():
                    logging.info('Task requested to be killed, may not have processed all progress messages')
        except Exception as exception:
            logging.exception('Error while tailing %s [tag=%s]', self.target_file, self.location_tag)
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
            logging.info('Skipping "%s" as the percent is not in [0, 100]', progress_report)
            return False

        percent_int = int(round(percent_float))
        logging.debug('Updating progress to %s percent [tag=%s]', percent_int, self.location_tag)

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
        last_unprocessed_report = None
        if self.progress_regex_string:
            sleep_time_ms = 50
            for line in self.tail(sleep_time_ms):
                try:
                    progress_report = self.match_progress_update(line)
                    if progress_report is not None:
                        if self.task_completed_signal.isSet():
                            last_unprocessed_report = progress_report
                        elif self.__update_progress(progress_report):
                            yield self.progress
                except Exception as exception:
                    if cu.is_out_of_memory_error(exception):
                        raise exception
                    else:
                        logging.exception('Skipping "%s" as a progress entry', line)
        if last_unprocessed_report is not None:
            if self.__update_progress(last_unprocessed_report):
                yield self.progress


class ProgressTracker(object):
    """Helper class to track progress messages from the specified location."""

    def __init__(self, config, stop_signal, task_completed_signal, counter, progress_updater,
                 progress_termination_signal, location, location_tag, os_error_handler):
        """Launches the threads that track progress and send progress updates to the driver.

        Parameters
        ----------
        config: cook.config.ExecutorConfig
            The current executor config.
        stop_signal: threading.Event
            Event that determines if an interrupt was sent
        task_completed_signal: threading.Event
            Event that tracks task execution completion
        progress_updater: ProgressUpdater
            The progress updater used to send the progress messages
        counter: ProgressSequenceCounter
            The sequence counter
        location: string
            The target location to read for progress messages
        location_tag: string
            A tag to identify the target location.
        os_error_handler: fn(os_error)
            OSError exception handler for out of memory situations."""
        self.location_tag = location_tag
        self.os_error_handler = os_error_handler
        self.progress_complete_event = Event()
        self.watcher = ProgressWatcher(location, location_tag, counter, config.max_bytes_read_per_line,
                                       config.progress_regex_string, stop_signal, task_completed_signal,
                                       progress_termination_signal)
        self.updater = progress_updater

    def start(self):
        """Launches a thread that starts monitoring the progress location for progress messages."""
        logging.info('Starting progress monitoring from [tag=%s]', self.location_tag)
        tracker_thread = Thread(target=self.track_progress, args=())
        tracker_thread.daemon = True
        tracker_thread.start()

    def wait(self, timeout=None):
        """Waits for the progress tracker thread to run to completion."""
        self.progress_complete_event.wait(timeout=timeout)
        if self.progress_complete_event.isSet():
            logging.info('Progress monitoring complete [tag=%s]', self.location_tag)
        else:
            logging.info('Progress monitoring did not complete [tag=%s]', self.location_tag)

    def track_progress(self):
        """Retrieves and sends progress updates using send_progress_update_fn.
        It sets the progress_complete_event before returning."""
        try:
            for current_progress in self.watcher.retrieve_progress_states():
                self.updater.send_progress_update(current_progress)
        except Exception as exception:
            if cu.is_out_of_memory_error(exception):
                self.os_error_handler(exception)
            else:
                logging.exception('Exception while tracking progress [tag=%s]', self.location_tag)
        finally:
            self.progress_complete_event.set()

    def force_send_progress_update(self):
        """Retrieves the latest progress message and attempts to force send it to the scheduler."""
        latest_progress = self.watcher.current_progress()
        self.updater.send_progress_update(latest_progress, force_send=True)
