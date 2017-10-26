#!/usr/bin/env python3

"""This module ensures atomic writes to stdout and stderr.
"""

import logging
import sys
from threading import Lock, Thread

__stdout_lock__ = Lock()
__stderr_lock__ = Lock()


def print_out(string_data, flush=False):
    """Wrapper function that prints to stdout in a thread-safe manner using the __stdout_lock__ lock.

    Parameters
    ----------
    string_data: string
        The string to output

    Returns
    -------
    Nothing.
    """
    with __stdout_lock__:
        print(string_data, file=sys.stdout)
    if flush:
        sys.stdout.flush()


def print_err(string_data, flush=False):
    """Wrapper function that prints to stderr in a thread-safe manner using the __stderr_lock__ lock.

    Parameters
    ----------
    string_data: string
        The string to output

    Returns
    -------
    Nothing.
    """
    with __stderr_lock__:
        print(string_data, file=sys.stderr)
    if flush:
        sys.stdout.flush()


def process_output(label, out_file, out_fn, flush_fn):
    """Processes output piped from the out_file and prints it using the out_fn function.
    When done reading the file, calls flush_fn to flush the output.

    Parameters
    ----------
    label: string
        The string to associate in outputs
    out_file: a file object
        Provides that output from the child process
    out_fn: function(string)
        Function to output a string
    flush_fn: function()
        Function that flushes the output.

    Returns
    -------
    Nothing.
    """
    message = 'Starting to pipe {} from launched process'.format(label)
    print_out(message)
    logging.info(message)
    try:
        for line in out_file:
            out_fn(line.decode('utf-8').rstrip())
        flush_fn()
        message = 'Done piping {} from launched process'.format(label)
        print_out(message)
        logging.info(message)
    except Exception:
        logging.exception('Error in process_output of {}'.format(label))
        print_out('Error in process_output of {}'.format(label), flush=True)


def track_outputs(process):
    """Launches two threads to pipe the stderr/stdout from the subprocess to the system stderr/stdout.

    Parameters
    ----------
    process: subprocess.Popen
        The process whose stderr and stdout to monitor.

    Returns
    -------
    Nothing.
    """
    Thread(target=process_output, args=('stderr', process.stderr, print_err, sys.stderr.flush)).start()
    Thread(target=process_output, args=('stdout', process.stdout, print_out, sys.stdout.flush)).start()
