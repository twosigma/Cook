#!/usr/bin/env python3

"""
The primary entry point for Cook's custom executor. This module configures logging,
initializes a data store, and starts the executor's components.

Components:

    * executor - An executor driver handling communication with the Mesos agent
    * launcher - A component launching and monitoring command processes
    * server - An HTTP server handling requests from command processes
"""

import sys
import time
import signal
import logging

from threading import Event, Thread

from store import Store
from server import run_server
from launcher import run_launcher
from executor import run_executor

if __name__ == "__main__":
    # TODO: get log level config from env
    logging.basicConfig(filename = "executor.log", level = "INFO")

    logging.info("Starting Cook Executor")

    event = Event()

    store = Store({
        'task': {
            'message': str,
            'progress': float,
            'env': {str: str},
            'commands': [{
                'name': str,
                'value': str,
                'async': bool,
                'guard': bool,
            }],
            'codes': [int]
        }})

    threads = [
        Thread(target = run_server, args = (store, event)),
        # TODO: rename this to driver in order to avoid confusion?
        Thread(target = run_executor, args = (store, event)),
        Thread(target = run_launcher, args = (store, event))
    ]

    for t in threads:
        t.start()

    signal.signal(signal.SIGINT, lambda *_: event.set())

    while all([t.is_alive() for t in threads]):
        time.sleep(1)
    else:
        event.set()

    # TODO: return non-zero exit code when there's an error
    sys.exit(0)
