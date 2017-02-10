#!/usr/bin/env python3

"""
The primary entry point for Cook's custom executor. This module configures logging,
initializes a data store, and starts the executor's components.

Components:

    * executor - An executor driver handling communication with the Mesos agent
    * launcher - A component launching and monitoring command processes
    * server - An HTTP server handling requests from command processes
"""

import os
import sys
import time
import signal
import logging

from threading import Event, Thread

from cook.store import Store
from cook.server import run_server
from cook.launcher import run_launcher
from cook.executor import run_executor

def main(args=None):
    id = os.environ.get('MESOS_EXECUTOR_ID', '1')
    port = int(os.environ.get('EXECUTOR_PORT0', 8080))
    level = os.environ.get('EXECUTOR_LOG_LEVEL', 'INFO')
    length = int(os.environ.get('EXECUTOR_MAX_MESSAGE_LENGTH', 512))
    sandbox = os.environ.get('MESOS_SANDBOX', '')

    os.environ['EXECUTOR_ENDPOINT'] = '127.0.0.1:%s/task/%s' % (port, id)

    logging.basicConfig(filename = "executor.log", level = "INFO")
    logging.info("Starting executor with id: " + id + ", port: " + str(port))

    event = Event()

    store = Store({
        'task': {
            'state': str,
            'message': lambda v: isinstance(v, str) and len(v) < length,
            'sandbox': str,
            'progress': float,
            'env': {str: str},
            'command': str,
            'before_commands': [{
                'name': str,
                'value': str,
                'async': bool,
                'guard': bool,
                'default': bool
            }],
            'after_commands': [{
                'name': str,
                'value': str,
                'async': bool,
                'guard': bool,
                'default': bool
            }],
            'exit_code': int,
            'before_exit_codes': [lambda v: isinstance(v, int) or v is None],
            'after_exit_codes': [lambda v: isinstance(v, int) or v is None]
        }})

    threads = [
        Thread(target = run_executor, args = (store, event, sandbox)),
        Thread(target = run_server, args = (store, event, port)),
        Thread(target = run_launcher, args = (store, event))
    ]

    for t in threads:
        t.start()

    signal.signal(signal.SIGINT, lambda *_: event.set())

    while all([t.is_alive() for t in threads]):
        time.sleep(1)
    else:
        event.set()

    logging.info("Exiting executor with id: " + id + ", port: " + str(port))

    # TODO: return non-zero exit code when there's an error
    sys.exit(0)

if __name__ == "__main__":
    main()
