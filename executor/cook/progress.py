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
        self.last_progress_data = None
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
            if progress_data is not None and self.last_progress_data != progress_data:
                if force_send or self.has_enough_time_elapsed_since_last_update():
                    logging.info('Sending progress message {}'.format(progress_data))
                    message_dict = dict(progress_data)
                    message_dict['task-id'] = self.task_id
                    progress_message = json.dumps(message_dict)

                    if len(progress_message) > self.max_message_length and 'progress-message' in message_dict:
                        progress_str = message_dict['progress-message']
                        num_extra_chars = len(progress_message) - self.max_message_length
                        allowed_progress_message_length = max(len(progress_str) - num_extra_chars - 3, 0)
                        new_progress_str = progress_str[:allowed_progress_message_length].strip() + '...'
                        logging.info('Progress message trimmed to {}'.format(new_progress_str))
                        message_dict['progress-message'] = new_progress_str
                        progress_message = json.dumps(message_dict)

                    self.send_progress_message(progress_message)
                    self.last_reported_time = time.time()
                    self.last_progress_data = progress_data
                else:
                    logging.debug('Not sending progress data as enough time has not elapsed since last update')


class ProgressWatcher(object):
    """This class tails the output from the target file listening for progress messages.
    The retrieve_progress_states generates all progress messages iteratively.
    """

    @staticmethod
    def match_progress_update(progress_regex_str, input_string):
        """Returns the progress tuple when the input string matches the provided regex.
        
        Parameters
        ----------
        progress_regex_str: string
            The progress regex to match against, it must return two capture groups.
        input_string: string
            The input string.
        
        Returns
        -------
        the tuple (percent, message) if the string matches the provided regex, 
                 else return None. 
        """
        matches = re.findall(progress_regex_str, input_string)
        return matches[0] if len(matches) >= 1 else None

    def __init__(self, output_name, sequence_counter, max_bytes_read_per_line, progress_regex_string, stop_signal,
                 task_completed_signal):
        self.target_file = output_name
        self.sequence_counter = sequence_counter
        self.progress_regex_string = progress_regex_string
        self.progress = None
        self.stop_signal = stop_signal
        self.task_completed_signal = task_completed_signal
        self.max_bytes_read_per_line = max_bytes_read_per_line

    def progress_target_file(self):
        """Returns the file that is being monitored for progress."""
        return self.target_file

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
                logging.info('Skipping progress monitoring on {} as it is not a file'.format(self.target_file))
                return

            while not os.path.isfile(self.target_file) and not self.task_completed_signal.isSet():
                logging.debug('{} has not yet been created, sleeping {} ms'.format(self.target_file, sleep_time_ms))
                time.sleep(sleep_param)

            if not os.path.isfile(self.target_file):
                logging.info('Progress output file {} has not been created'.format(self.target_file))
                return

            if self.stop_signal.isSet():
                logging.info('{} has not been read to parse progress messages'.format(self.target_file))
                return

            logging.info('{} has been created, reading contents'.format(self.target_file))
            target_file_obj = open(self.target_file, 'r')
            fragment_index = 0
            line_index = 0
            while not self.stop_signal.isSet():
                line = target_file_obj.readline(self.max_bytes_read_per_line)
                if not line:
                    # exit if program has completed and there are no more lines to read
                    if self.task_completed_signal.isSet():
                        log_message = 'Done processing progress messages from {}, {} fragments and {} lines read'
                        logging.info(log_message.format(self.target_file, fragment_index, line_index))
                        break
                    # no new line available, sleep before trying again
                    time.sleep(sleep_param)
                    continue

                fragment_index += 1
                if line.endswith(os.linesep):
                    line_index += 1
                yield line
            if self.stop_signal.isSet() and not self.task_completed_signal.isSet():
                logging.info('Task requested to be killed, may not have processed all progress messages')
        except:
            logging.exception('Error while tailing {}'.format(self.target_file))

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
                progress_report = ProgressWatcher.match_progress_update(self.progress_regex_string, line)
                if progress_report is None:
                    continue
                percent_str, message = progress_report
                if not percent_str or not percent_str.isdigit():
                    logging.info('Skipping "{}" as the percent entry is not an int'.format(progress_report))
                    continue
                percent_int = int(percent_str, 10)
                if percent_int < 0 or percent_int > 100:
                    logging.info('Skipping "{}" as the percent is not in [0, 100]'.format(progress_report))
                    continue
                log_message = 'Updating progress to {} percent, message: {} [source={}]'
                logging.info(log_message.format(percent_str, message, self.target_file))
                self.progress = {'progress-message': message.strip(),
                                 'progress-percent': percent_int,
                                 'progress-sequence': self.sequence_counter.increment_and_get()}
                yield self.progress


class ProgressTracker(object):
    """Helper class to track progress messages from the specified location."""

    @staticmethod
    def track_progress(progress_watcher, progress_complete_event, send_progress_update_fn):
        """Retrieves and sends progress updates using send_progress_update_fn.
        It sets the progress_complete_event before returning.

            Parameters
            ----------
            progress_watcher: ProgressWatcher
                The progress watcher which maintains the current progress state
            progress_complete_event: threading.Event
                Event that triggers completion of progress tracking
            send_progress_update_fn: function(progress)
                The function to invoke while sending progress updates

            Returns
            -------
            Nothing.
            """
        try:
            for current_progress in progress_watcher.retrieve_progress_states():
                logging.debug('Latest progress: {}'.format(current_progress))
                send_progress_update_fn(current_progress)
        except:
            logging.exception('Exception while tracking progress')
        finally:
            progress_complete_event.set()

    def __init__(self, task_id, config, stop_signal, task_completed_signal, send_progress_message, location, counter):
        """Launches the threads that track progress and send progress updates to the driver.

        Parameters
        ----------
        task_id: string
            The task id.
        config: cook.config.ExecutorConfig
            The current executor config.
        stop_signal: threading.Event
            Event that determines if an interrupt was sent
        task_completed_signal: threading.Event
            Event that tracks task execution completion
        send_progress_message: function(message)
            The helper function used to send the progress message
        location: string
            The target location to read for progress messages
        counter: ProgressSequenceCounter
            The sequence counter."""
        self.progress_complete_event = Event()
        self.watcher = ProgressWatcher(location, counter, config.max_bytes_read_per_line, config.progress_regex_string,
                                       stop_signal, task_completed_signal)
        self.updater = ProgressUpdater(task_id, config.max_message_length, config.progress_sample_interval_ms,
                                       send_progress_message)

    def start(self):
        """Launches a thread that starts monitoring the progress location for progress messages."""
        logging.info('Starting progress monitoring from {}'.format(self.watcher.progress_target_file()))
        Thread(target=ProgressTracker.track_progress,
               args=(self.watcher, self.progress_complete_event, self.updater.send_progress_update)).start()

    def wait(self, timeout=None):
        """Waits for the progress tracker thread to run to completion."""
        self.progress_complete_event.wait(timeout=timeout)
        if self.progress_complete_event.isSet():
            logging.info('Progress monitoring from {} complete'.format(self.watcher.progress_target_file()))
        else:
            logging.info('Progress monitoring from {} did not complete'.format(self.watcher.progress_target_file()))

    def force_send_progress_update(self):
        """Retrieves the latest progress message and attempts to force send it to the scheduler."""
        latest_progress = self.watcher.current_progress()
        self.updater.send_progress_update(latest_progress, force_send=True)
