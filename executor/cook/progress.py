import json
import logging
import time
from threading import Event, Lock, Thread

import os
import re


class ProgressUpdater(object):
    """This class is responsible for sending progress updates to the scheduler.
    It throttles the rate at which progress updates are sent.
    """

    def __init__(self, poll_interval_ms):
        self.poll_interval_ms = poll_interval_ms
        self.last_reported_time = None
        self.last_progress_data = None
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

    def send_progress_update(self, driver, task_id, max_message_length, progress_data, send_message, force_send=False):
        """Sends a progress update if enough time has elapsed since the last progress update.
        The force_send flag can be used to ignore the check for enough time having elapsed.
        Using this method is thread-safe.
        
        Parameters
        ----------
        :param driver: MesosExecutorDriver
            The mesos driver to use.
        :param task_id: string
            The task id.
        :param max_message_length: int
            The allowed max message length after encoding.
        :param progress_data: map 
            The progress data to send.
        :param send_message: function(driver, message, max_message_length)
            The helper function used to send the message.
        :param force_send: boolean, optional
            Defaults to false.
            
        Returns
        -------
        :return: Nothing 
        """
        with self.lock:
            if progress_data is not None and self.last_progress_data != progress_data:
                if force_send or self.has_enough_time_elapsed_since_last_update():
                    logging.info('Sending progress message {}'.format(progress_data))
                    message_dict = dict(progress_data)
                    message_dict['task-id'] = task_id
                    progress_message = json.dumps(message_dict)
                    send_message(driver, progress_message, max_message_length)
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
        """Verifies whether the input string matches the provided regex.
        
        Parameters
        ----------
        :param progress_regex_str: string
            The progress regex to match against, it must return two capture groups.
        :param input_string: string
            The input string.
        
        Returns
        -------
        :return: whether the input string matches the provided regex. 
        """
        matches = re.findall(progress_regex_str, input_string)
        return matches[0] if len(matches) >= 1 else None

    def __init__(self, config, completed_signal):
        self.target_file = config.progress_output_name
        self.progress_regex_string = config.progress_regex_string
        self.progress = None
        self.completed_signal = completed_signal
        self.max_bytes_read_per_line = config.max_bytes_read_per_line

    def current_progress(self):
        """Returns the current progress dictionary."""
        return self.progress

    def tail(self, sleep_time_ms):
        """This method incrementally generates lines from a file by waiting for new content from a file.
        It behaves like the 'tail -f' shell command.
        
        Parameters
        ----------
        :param sleep_time_ms: int
            The unit of time in ms to repetitively sleep when the file has not been created or no new
            content is available in the file being tailed.
        
        Returns
        -------
        :return: an incrementally generated list of lines in the file being tailed.
        """
        try:
            sleep_param = sleep_time_ms / 1000
            while True:
                if os.path.isfile(self.target_file):
                    logging.info('{} has been created, reading contents'.format(self.target_file))
                    break
                if self.completed_signal.isSet():
                    logging.info('No output has been read to parse progress messages')
                    return
                logging.debug('{} has not yet been created, sleeping {} ms'.format(self.target_file, sleep_time_ms))
                time.sleep(sleep_param)

            target_file_obj = open(self.target_file, 'r')
            while True:
                line = target_file_obj.readline(self.max_bytes_read_per_line)
                if not line:
                    # no new line available, sleep before trying again
                    if self.completed_signal.isSet():
                        target_file_obj.close()
                        break
                    time.sleep(sleep_param)
                    continue
                yield line
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
        :return: an incrementally generated list of progress states.
        """
        if self.progress_regex_string:
            sleep_time_ms = 50
            for line in self.tail(sleep_time_ms):
                progress_report = ProgressWatcher.match_progress_update(self.progress_regex_string, line)
                if progress_report is not None:
                    percent, message = progress_report
                    logging.info('Updating progress to {} percent, message: {}'.format(percent, message))
                    self.progress = {'progress-message': message.strip(), 'progress-percent': int(percent)}
                    yield self.progress


def track_progress(driver, task_id, progress_watcher, max_message_length, progress_complete_event,
                   send_progress_update, send_message):
    """Sends progress updates to the mesos driver until the stop_signal is set.
    
    Parameters
    ----------
    :param driver: MesosExecutorDriver
        The mesos driver to use.
    :param task_id: string
        The id of the task to execute.
    :param progress_watcher: ProgressWatcher
        The progress watcher which maintains the current progress state
    :param max_message_length: int
        Allowed max message length to send
    :param progress_complete_event: Event
        Event that triggers completion of progress tracking
    :param send_progress_update: function(driver, task_id, max_message_length, current_progress, send_message)
        The function to invoke while sending progress updates
        
    Returns
    -------
    :return: Nothing.
    """
    try:
        for current_progress in progress_watcher.retrieve_progress_states():
            logging.debug('Latest progress: {}'.format(current_progress))
            send_progress_update(driver, task_id, max_message_length, current_progress, send_message)
    except:
        logging.exception('Exception while tracking progress')
    finally:
        progress_complete_event.set()


def launch_progress_tracker(driver, task, max_message_length, progress_watcher, progress_updater, send_message):
    """Launches the threads that track progress and send progress updates to the driver."""
    progress_complete_event = Event()
    progress_update_thread = Thread(target=track_progress,
                                    args=(driver, task, progress_watcher, max_message_length, progress_complete_event,
                                          progress_updater.send_progress_update, send_message))
    progress_update_thread.start()
    return progress_complete_event
