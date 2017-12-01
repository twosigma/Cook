import json
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
            last_progress_data_sent = self.last_progress_data_sent
            if progress_data is not None and last_progress_data_sent != progress_data:
                last_progress_sequence = last_progress_data_sent['progress-sequence'] if last_progress_data_sent else 0
                if last_progress_sequence < progress_data['progress-sequence']:
                    if force_send or self.has_enough_time_elapsed_since_last_update():
                        logging.info('Sending progress message {}'.format(progress_data))
                        message_dict = dict(progress_data)
                        message_dict['task-id'] = self.task_id

                        raw_progress_message = progress_data['progress-message']
                        try:
                            progress_str = raw_progress_message.decode('ascii').strip()
                        except UnicodeDecodeError:
                            logging.info('Unable to decode progress message in ascii, using empty string instead')
                            progress_str = ''
                        message_dict['progress-message'] = progress_str

                        progress_message = json.dumps(message_dict)
                        if len(progress_message) > self.max_message_length:
                            num_extra_chars = len(progress_message) - self.max_message_length
                            allowed_progress_message_length = max(len(progress_str) - num_extra_chars - 3, 0)
                            new_progress_str = progress_str[:allowed_progress_message_length].strip() + '...'
                            logging.info('Progress message trimmed to {}'.format(new_progress_str))
                            message_dict['progress-message'] = new_progress_str
                            progress_message = json.dumps(message_dict)

                        send_success = self.send_progress_message(progress_message)
                        if send_success:
                            self.last_progress_data_sent = progress_data
                            self.last_reported_time = time.time()
                        else:
                            logging.info('Unable to send progress message {}'.format(progress_message))
                    else:
                        logging.debug('Not sending progress data as enough time has not elapsed since last update')
                else:
                    logging.info('Skipping outdated progress data {}'.format(progress_data))


class ProgressWatcher(object):
    """This class tails the output from the target file listening for progress messages.
    The retrieve_progress_states generates all progress messages iteratively.
    """

    @staticmethod
    def match_progress_update(progress_regex_pattern, input_data):
        """Returns the progress tuple when the input string matches the provided regex.
        
        Parameters
        ----------
        progress_regex_pattern: re.pattern
            The progress regex to match against, it must return two capture groups.
        input_data: bytes
            The input data.
        
        Returns
        -------
        the tuple (percent, message) if the string matches the provided regex, 
                 else return None. 
        """
        matches = progress_regex_pattern.findall(input_data)
        return matches[0] if len(matches) >= 1 else None

    def __init__(self, output_name, location_tag, sequence_counter, max_bytes_read_per_line, progress_regex_string,
                 stop_signal, task_completed_signal):
        self.target_file = output_name
        self.location_tag = location_tag
        self.sequence_counter = sequence_counter
        self.progress_regex_string = progress_regex_string
        self.progress_regex_pattern = re.compile(progress_regex_string.encode())
        self.progress = None
        self.stop_signal = stop_signal
        self.task_completed_signal = task_completed_signal
        self.max_bytes_read_per_line = max_bytes_read_per_line

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
            with open(self.target_file, 'rb') as target_file_obj:
                while not self.stop_signal.isSet():
                    line = target_file_obj.readline(self.max_bytes_read_per_line)
                    if not line:
                        # exit if program has completed and there are no more lines to read
                        if self.task_completed_signal.isSet():
                            log_message = '%s fragments and %s lines read while processing progress messages [tag=%s]'
                            logging.info(log_message, fragment_index, line_index, self.location_tag)
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
        except:
            logging.exception('Error while tailing %s [tag=%s]', self.target_file, self.location_tag)

    def retrieve_progress_states(self):
        """Generates the progress states by tailing the target_file.
        It tails a target file (using the tail() method) and uses the provided 
        regex to find a match for a progress message. The regex is expected to 
        generate two components in the match: the progress percent as an int and 
        a progress message string. When such a message is found, this method 
        yields the current progress as a dictionary.
        
        Returns
        -------
        an incrementally generated list of progress states.
        """
        if self.progress_regex_string:
            sleep_time_ms = 50
            for line in self.tail(sleep_time_ms):
                try:
                    progress_report = ProgressWatcher.match_progress_update(self.progress_regex_pattern, line)
                    if progress_report is None:
                        continue
                    percent_data, message_data = progress_report
                    percent_int = int(round(float(percent_data.decode())))
                    if percent_int < 0 or percent_int > 100:
                        logging.info('Skipping "%s" as the percent is not in [0, 100]', progress_report)
                        continue
                    logging.debug('Updating progress to %s percent [tag=%s]', percent_int, self.location_tag)
                    self.progress = {'progress-message': message_data,
                                     'progress-percent': percent_int,
                                     'progress-sequence': self.sequence_counter.increment_and_get()}
                    yield self.progress
                except:
                    logging.exception('Skipping "%s" as a progress entry', progress_report)


class ProgressTracker(object):
    """Helper class to track progress messages from the specified location."""

    def __init__(self, config, stop_signal, task_completed_signal, counter, progress_updater, location, location_tag):
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
            A tag to identify the target location."""
        self.location_tag = location_tag
        self.progress_complete_event = Event()
        self.watcher = ProgressWatcher(location, location_tag, counter, config.max_bytes_read_per_line,
                                       config.progress_regex_string, stop_signal, task_completed_signal)
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
            logging.info('Progress monitoring from complete [tag=%s]', self.location_tag)
        else:
            logging.info('Progress monitoring from did not complete [tag=%s]', self.location_tag)

    def track_progress(self):
        """Retrieves and sends progress updates using send_progress_update_fn.
        It sets the progress_complete_event before returning."""
        try:
            for current_progress in self.watcher.retrieve_progress_states():
                self.updater.send_progress_update(current_progress)
        except:
            logging.exception('Exception while tracking progress [tag=%s]', self.location_tag)
        finally:
            self.progress_complete_event.set()

    def force_send_progress_update(self):
        """Retrieves the latest progress message and attempts to force send it to the scheduler."""
        latest_progress = self.watcher.current_progress()
        self.updater.send_progress_update(latest_progress, force_send=True)
