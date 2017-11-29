#!/usr/bin/env python3

"""This module ensures atomic writes to stdout and stderr.
"""

import logging
import os
import select
import sys
from threading import Event, Lock, Thread, Timer

__stdout_lock__ = Lock()
__stderr_lock__ = Lock()


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
    stdout_buffer = sys.stdout.buffer
    with __stdout_lock__:
        if isinstance(data, str):
            stdout_buffer.write(data.encode())
        else:
            stdout_buffer.write(data)
        if newline:
            stdout_buffer.write(os.linesep.encode())
        if flush:
            stdout_buffer.flush()


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
    stderr_buffer = sys.stderr.buffer
    with __stderr_lock__:
        if isinstance(data, str):
            stderr_buffer.write(data.encode())
        else:
            stderr_buffer.write(data)
        if newline:
            stderr_buffer.write(os.linesep)
        if flush:
            stderr_buffer.flush()


def process_output(label, process, out_fd, out_fn, flush_fn, min_reads_before_flush, max_read_nb_chunk_size):
    """Processes output piped from the out_file and prints it using the out_fn function.
    When done reading the file, calls flush_fn to flush the output.

    Parameters
    ----------
    label: string
        The string to associate in outputs
    out_fd: a file descriptor
        Provides that output from the child process
    out_fn: function(string)
        Function to output a string
    flush_fn: function()
        Function that flushes the output.
    min_reads_before_flush: int
        The number of read attempts before triggering a flush
    max_read_nb_chunk_size: int
        The maximum number of bytes to read per call to readline().

    Returns
    -------
    Nothing.
    """
    logging.info('Starting to pipe {}'.format(label))

    def safe_flush():
        try:
            flush_fn()
        except:
            logging.exception('Error while flushing contents {}'.format(label))

    def nb_read(read_pipe, timeout_secs=0.10):
        read_ready, _, _ = select.select([read_pipe], [], [], timeout_secs)
        if read_ready:
            return os.read(read_pipe, max_read_nb_chunk_size)
        return None

    reads_since_flush = 0

    bytes_in_buffer = 0
    try:
        while True:
            data = nb_read(out_fd)
            reads_since_flush += 1
            if data is not None:
                logging.info('Read: {} [label={}]'.format(data, label))
                out_fn(data, newline=False)
                bytes_in_buffer += len(data)
            if ((reads_since_flush >= min_reads_before_flush and bytes_in_buffer > 0) or
                        bytes_in_buffer > max_read_nb_chunk_size):
                safe_flush()
                # reset counters
                reads_since_flush = 0
                bytes_in_buffer = 0
            elif process.poll() is not None:
                logging.info('Done piping {}'.format(label))
                break
    except Exception:
        logging.exception('Error in process_output of {}'.format(label))


def track_outputs(task_id, process, stderr_out, stdout_out, min_reads_before_flush, max_read_nb_chunk_size):
    """Launches two threads to pipe the stderr/stdout from the subprocess to the system stderr/stdout.

    Parameters
    ----------
    task_id: string
        The ID of the task being executed.
    process: subprocess.Popen
        The process whose stderr and stdout to monitor.
    stderr_out: file descriptor
        The file descriptor for the process' stderr.
    stdout_out: file descriptor
        The file descriptor for the process' stdout.
    min_reads_before_flush: number
        The number of seconds to wait between flushing buffered data
    max_read_nb_chunk_size: int
        The maximum number of bytes to read per call to readline().

    Returns
    -------
    A tuple containing the two threads that have been started: stdout_thread, stderr_thread.
    """

    def launch_tracker_thread(label, out_fd, out_fn, flush_fn):
        tracker_thread = Thread(target=process_output,
                                args=(label, process, out_fd, out_fn, flush_fn, min_reads_before_flush, max_read_nb_chunk_size))
        tracker_thread.daemon = True
        tracker_thread.start()
        return tracker_thread

    stderr_label = 'stderr (task-id: {}, pid: {})'.format(task_id, process.pid)
    stderr_thread = launch_tracker_thread(stderr_label, stderr_out, print_err, sys.stderr.flush)

    stdout_label = 'stdout (task-id: {}, pid: {})'.format(task_id, process.pid)
    stdout_thread = launch_tracker_thread(stdout_label, stdout_out, print_out, sys.stdout.flush)

    return stdout_thread, stderr_thread
