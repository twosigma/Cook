import json
import logging
import time
from threading import Event, Lock, Thread

import os
import re

import cook.io_helper as cio


class ProgressUpdater(object):
    """This class is responsible for sending progress updates to the scheduler.
    It throttles the rate at which progress updates are sent.
    """

    def __init__(self, driver, task_id, max_message_length, poll_interval_ms, send_message_fn):
        """
        driver: MesosExecutorDriver
            The mesos driver to use.
        task_id: string
            The task id.
        max_message_length: int
            The allowed max message length after encoding.
        poll_interval_ms: int
            The interval after which to send a subsequent progress update.
        send_message: function(driver, message, max_message_length)
            The helper function used to send the message.
        """
        self.driver = driver
        self.task_id = task_id
        self.max_message_length = max_message_length
        self.poll_interval_ms = poll_interval_ms
        self.last_reported_time = None
        self.last_progress_data = None
        self.send_message = send_message_fn
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
        progress_data: map 
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

                    self.send_message(self.driver, progress_message, self.max_message_length)
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

    def __init__(self, config, stop_signal, task_completed_signal):
        self.target_file = config.progress_output_name
        self.progress_regex_string = config.progress_regex_string
        self.progress = None
        self.stop_signal = stop_signal
        self.task_completed_signal = task_completed_signal
        self.max_bytes_read_per_line = config.max_bytes_read_per_line

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

            while not os.path.isfile(self.target_file) and not self.task_completed_signal.isSet():
                logging.debug('{} has not yet been created, sleeping {} ms'.format(self.target_file, sleep_time_ms))
                time.sleep(sleep_param)

            if self.stop_signal.isSet():
                logging.info('No output has been read to parse progress messages')
                return

            logging.info('{} has been created, reading contents'.format(self.target_file))
            target_file_obj = open(self.target_file, 'r')
            line_index = 0
            while not self.stop_signal.isSet():
                line = target_file_obj.readline(self.max_bytes_read_per_line)
                if not line:
                    # exit if program has completed and there are no more lines to read
                    if self.task_completed_signal.isSet():
                        message = 'Done processing progress messages, {} lines read'.format(line_index)
                        cio.printline_out(message)
                        logging.info(message)
                        break
                    # no new line available, sleep before trying again
                    time.sleep(sleep_param)
                    continue
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
            sequence = 0
            for line in self.tail(sleep_time_ms):
                progress_report = ProgressWatcher.match_progress_update(self.progress_regex_string, line)
                if progress_report is not None:
                    percent, message = progress_report
                    if not percent or not percent.isdigit():
                        logging.info('Skipping "{}" as the percent entry is not an int'.format(progress_report))
                        continue
                    logging.info('Updating progress to {} percent, message: {}'.format(percent, message))
                    sequence += 1
                    self.progress = {'progress-message': message.strip(),
                                     'progress-percent': int(percent),
                                     'progress-sequence': sequence}
                    yield self.progress


def track_progress(progress_watcher, progress_complete_event, send_progress_update):
    """Sends progress updates to the mesos driver until the stop_signal is set.
    
    Parameters
    ----------
    progress_watcher: ProgressWatcher
        The progress watcher which maintains the current progress state
    progress_complete_event: Event
        Event that triggers completion of progress tracking
    send_progress_update: function(current_progress)
        The function to invoke while sending progress updates
        
    Returns
    -------
    Nothing.
    """
    try:
        for current_progress in progress_watcher.retrieve_progress_states():
            logging.debug('Latest progress: {}'.format(current_progress))
            send_progress_update(current_progress)
    except:
        logging.exception('Exception while tracking progress')
    finally:
        progress_complete_event.set()


def launch_progress_tracker(progress_watcher, progress_updater):
    """Launches the threads that track progress and send progress updates to the driver.

    Parameters
    ----------
    progress_watcher: ProgressWatcher
        The progress watcher which maintains the current progress state.
    progress_updater: ProgressUpdater
        The progress updater which sends progress updates to the scheduler.
        
    Returns
    -------
    The progress complete threading.Event.
    """
    progress_complete_event = Event()
    progress_update_thread = Thread(target=track_progress,
                                    args=(progress_watcher, progress_complete_event,
                                          progress_updater.send_progress_update))
    progress_update_thread.start()
    return progress_complete_event


def force_send_progress_update(progress_watcher, progress_updater):
    """Retrieves the latest progress message and attempts to force send it to the scheduler.

    Parameters
    ----------
    progress_watcher: ProgressWatcher
        The progress watcher which maintains the current progress state.
    progress_updater: ProgressUpdater
        The progress updater which sends progress updates to the scheduler.
        
    Returns
    -------
    Nothing.
    """
    latest_progress = progress_watcher.current_progress()
    progress_updater.send_progress_update(latest_progress, force_send=True)
