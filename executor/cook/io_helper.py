#!/usr/bin/env python3

"""This module ensures atomic writes to stdout and stderr.
"""

import logging
import os
import sys
from threading import Event, Lock, Thread, Timer

__stdout_lock__ = Lock()
__stderr_lock__ = Lock()


def print_to_buffer(lock, buffer, data, flush=False, newline=True):
    """Helper function that prints data to the specified buffer in a thread-safe manner using the lock.

    Parameters
    ----------
    lock: threading.Lock
        The lock to use
    buffer: byte buffer
        The buffer to write to
    data: string or bytes
        The data to output
    flush: boolean
        Flag determining whether to trigger a sys.stdout.flush()
    newline: boolean
        Flag determining whether to output a newline at the end

    Returns
    -------
    Nothing.
    """
    with lock:
        if isinstance(data, str):
            buffer.write(data.encode())
        else:
            buffer.write(data)
        if newline:
            buffer.write(os.linesep.encode())
        if flush:
            buffer.flush()


def print_out(data, flush=False, newline=True):
    """Wrapper function that prints to stdout in a thread-safe manner using the __stdout_lock__ lock.

    Parameters
    ----------
    data: string or bytes
        The data to output
    flush: boolean
        Flag determining whether to trigger a sys.stdout.flush()
    newline: boolean
        Flag determining whether to output a newline at the end

    Returns
    -------
    Nothing.
    """
    print_to_buffer(__stdout_lock__, sys.stdout.buffer, data, flush=flush, newline=newline)


def print_and_log(string_data, flush=False, newline=True):
    """Wrapper function that prints to stdout in a thread-safe manner ensuring newline at the start.
    The function also outputs the same message via logging.info().

    Parameters
    ----------
    string_data: string
        The string to output
    flush: boolean
        Flag determining whether to trigger a sys.stdout.flush()
    newline: boolean
        Flag determining whether to output a newline at the end

    Returns
    -------
    Nothing.
    """
    print_out('{}{}'.format(os.linesep, string_data), flush=flush, newline=newline)
    logging.info(string_data)


def print_err(data, flush=False, newline=True):
    """Wrapper function that prints to stderr in a thread-safe manner using the __stderr_lock__ lock.

    Parameters
    ----------
    data: string or bytes
        The data to output
    flush: boolean
        Flag determining whether to trigger a sys.stderr.flush()
    newline: boolean
        Flag determining whether to output a newline at the end

    Returns
    -------
    Nothing.
    """
    print_to_buffer(__stderr_lock__, sys.stderr.buffer, data, flush=flush, newline=newline)


def process_output(label, out_buffer, out_fn, flush_fn, flush_interval_secs, max_bytes_per_read):
    """Processes output piped from the out_buffer and prints it using the out_fn function.
    When done reading the file, calls flush_fn to flush the output.

    Parameters
    ----------
    label: string
        The string to associate in outputs
    out_buffer: a file object
        Provides that output from the child process
    out_fn: function(string)
        Function to output a string
    flush_fn: function()
        Function that flushes the output.
    flush_interval_secs: number
        The number of seconds to wait between flushing buffered data
    max_bytes_per_read: int
        The maximum number of bytes to read per call to read().

    Returns
    -------
    Nothing.
    """
    io_lock = Lock()
    lines_buffered_event = Event()
    buffering_complete_event = Event()

    def safe_flush():
        try:
            if lines_buffered_event.is_set():
                with io_lock:
                    if lines_buffered_event.is_set():
                        logging.info('Flushing contents {}'.format(label))
                        flush_fn()
                        lines_buffered_event.clear()
        except:
            logging.exception('Error while flushing contents {}'.format(label))

    def trigger_flush_daemon():
        safe_flush()
        if not buffering_complete_event.is_set():
            Timer(flush_interval_secs, trigger_flush_daemon).start()

    try:
        logging.info('Starting to pipe {}'.format(label))
        trigger_flush_daemon()
        while True:
            line = out_buffer.read1(max_bytes_per_read)
            if not line:
                break
            with io_lock:
                out_fn(line, newline=False)
                lines_buffered_event.set()
        safe_flush()
    except Exception:
        logging.exception('Error in process_output of {}'.format(label))
    finally:
        buffering_complete_event.set()
        logging.info('Done piping {}'.format(label))


def track_outputs(task_id, process, flush_interval_secs, max_bytes_per_read):
    """Launches two threads to pipe the stderr/stdout from the subprocess to the system stderr/stdout.

    Parameters
    ----------
    task_id: string
        The ID of the task being executed.
    process: subprocess.Popen
        The process whose stderr and stdout to monitor.
    flush_interval_secs: number
        The number of seconds to wait between flushing buffered data
    max_bytes_per_read: int
        The maximum number of bytes to read per call to read().

    Returns
    -------
    A tuple containing the two threads that have been started: stdout_thread, stderr_thread.
    """

    def launch_tracker_thread(label, out_buffer, out_fn, flush_fn):
        tracker_thread = Thread(target=process_output,
                                args=(label, out_buffer, out_fn, flush_fn, flush_interval_secs, max_bytes_per_read))
        tracker_thread.daemon = True
        tracker_thread.start()
        return tracker_thread

    stderr_label = 'stderr (task-id: {}, pid: {})'.format(task_id, process.pid)
    stderr_thread = launch_tracker_thread(stderr_label, process.stderr, print_err, sys.stderr.flush)

    stdout_label = 'stdout (task-id: {}, pid: {})'.format(task_id, process.pid)
    stdout_thread = launch_tracker_thread(stdout_label, process.stdout, print_out, sys.stdout.flush)

    return stdout_thread, stderr_thread
