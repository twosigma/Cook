#!/usr/bin/env python3

"""This module ensures atomic writes to stdout."""

import logging
import sys
from threading import Lock

import os

__stdout_lock__ = Lock()


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


def print_and_log(string_data, newline=True):
    """Wrapper function that prints and flushes to stdout in a locally thread-safe manner ensuring newline at the start.
    The function also outputs the same message via logging.info().

    Parameters
    ----------
    string_data: string
        The string to output
    newline: boolean
        Flag determining whether to output a newline at the end

    Returns
    -------
    Nothing.
    """
    print_out('{}{}'.format(os.linesep, string_data), flush=True, newline=newline)
    logging.info(string_data)
