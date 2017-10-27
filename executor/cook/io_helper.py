#!/usr/bin/env python3

"""This module ensures atomic writes to stdout and stderr.
"""

import logging
import os
import sys
from threading import Lock, Thread

__stdout_lock__ = Lock()
__stderr_lock__ = Lock()


def print_out(string_data, flush=False, newline=True):
    """Wrapper function that prints to stdout in a thread-safe manner using the __stdout_lock__ lock.

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
    message = '{}{}'.format(string_data, os.linesep) if newline else string_data
    with __stdout_lock__:
        sys.stdout.write(message)
    if flush:
        sys.stdout.flush()


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


def print_err(string_data, flush=False, newline=True):
    """Wrapper function that prints to stderr in a thread-safe manner using the __stderr_lock__ lock.

    Parameters
    ----------
    string_data: string
        The string to output
    flush: boolean
        Flag determining whether to trigger a sys.stderr.flush()
    newline: boolean
        Flag determining whether to output a newline at the end

    Returns
    -------
    Nothing.
    """
    message = '{}{}'.format(string_data, os.linesep) if newline else string_data
    with __stderr_lock__:
        sys.stderr.write(message)
    if flush:
        sys.stderr.flush()


def process_output(label, out_file, max_bytes_read_per_line, out_fn, flush_fn):
    """Processes output piped from the out_file and prints it using the out_fn function.
    When done reading the file, calls flush_fn to flush the output.

    Parameters
    ----------
    label: string
        The string to associate in outputs
    out_file: a file object
        Provides that output from the child process
    max_bytes_read_per_line: int
        The maximum number of bytes to read per call to readline().
    out_fn: function(string)
        Function to output a string
    flush_fn: function()
        Function that flushes the output.

    Returns
    -------
    Nothing.
    """
    print_and_log('Starting to pipe {}'.format(label))
    try:
        while True:
            line = out_file.readline(max_bytes_read_per_line)
            if not line:
                break
            out_fn(line.decode('utf-8'), newline=False)
        flush_fn()
        print_and_log('Done piping {}'.format(label))
    except Exception:
        logging.exception('Error in process_output of {}'.format(label))
        print_out('Error in process_output of {}{}'.format(label, os.linesep), flush=True)


def track_outputs(task_id, process, max_bytes_read_per_line):
    """Launches two threads to pipe the stderr/stdout from the subprocess to the system stderr/stdout.

    Parameters
    ----------
    task_id: string
        The ID of the task being executed.
    process: subprocess.Popen
        The process whose stderr and stdout to monitor.
    max_bytes_read_per_line: int
        The maximum number of bytes to read per call to readline().

    Returns
    -------
    A tuple containing the two threads that have been started: stdout_thread, stderr_thread.
    """
    stderr_label = 'stderr (task-id: {}, pid: {})'.format(task_id, process.pid)
    stderr_thread = Thread(target=process_output,
                           args=(stderr_label, process.stderr, max_bytes_read_per_line, print_err, sys.stderr.flush))
    stderr_thread.start()

    stdout_label = 'stdout (task-id: {}, pid: {})'.format(task_id, process.pid)
    stdout_thread = Thread(target=process_output,
                           args=(stdout_label, process.stdout, max_bytes_read_per_line, print_out, sys.stdout.flush))
    stdout_thread.start()

    return stdout_thread, stderr_thread
