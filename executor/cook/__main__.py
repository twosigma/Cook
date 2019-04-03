#!/usr/bin/env python3

"""The primary entry point for Cook's custom executor.
This module configures logging and starts the executor's driver thread.
"""

import logging
import signal
import sys
from threading import Event, Timer

import os

# CPython bug: including the idna encoding registers it,
# the encoding is loaded with the built-in frozen importer
# https://github.com/pyinstaller/pyinstaller/issues/1113
import encodings.idna

import cook.config as cc
import cook.executor as ce
import cook.io_helper as cio
import cook.util as cu
import pymesos as pm


def main(args=None):
    from cook._version import __version__

    if len(sys.argv) == 2 and sys.argv[1] == "--version":
        print(__version__)
        sys.exit(0)

    cio.print_out('Cook Executor version {}'.format(__version__), flush=True)

    environment = os.environ
    executor_id = environment.get('MESOS_EXECUTOR_ID', '1')
    log_level = environment.get('EXECUTOR_LOG_LEVEL', 'INFO')
    sandbox_directory = environment.get('MESOS_SANDBOX', '.')

    logging.basicConfig(level = log_level,
                        filename = os.path.join(sandbox_directory, 'executor.log'),
                        format='%(asctime)s %(levelname)s %(message)s')
    logging.info('Starting Cook Executor {} for executor-id={}'.format(__version__, executor_id))
    logging.info('Log level is {}'.format(log_level))

    config = cc.initialize_config(environment)

    def print_memory_usage_task():
        cu.print_memory_usage()
        timer = Timer(config.memory_usage_interval_secs, print_memory_usage_task)
        timer.daemon = True
        timer.start()
    print_memory_usage_task()

    stop_signal = Event()
    non_zero_exit_signal = Event()

    def handle_interrupt(interrupt_code, _):
        logging.info('Executor interrupted with code {}'.format(interrupt_code))
        cio.print_and_log('Received kill for task {} with grace period of {}'.format(
            executor_id, config.shutdown_grace_period))
        stop_signal.set()
        non_zero_exit_signal.set()
        cu.print_memory_usage()

    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    try:
        executor = ce.CookExecutor(stop_signal, config)
        driver = pm.MesosExecutorDriver(executor)

        logging.info('MesosExecutorDriver is starting...')
        driver.start()

        executor.await_completion()

        logging.info('MesosExecutorDriver requested to stop')
        driver.stop()
        logging.info('MesosExecutorDriver has been stopped')

        executor.await_disconnect()
    except Exception:
        logging.exception('Error in __main__')
        stop_signal.set()
        non_zero_exit_signal.set()

    cu.print_memory_usage()
    exit_code = 1 if non_zero_exit_signal.isSet() else 0
    logging.info('Executor exiting with code {}'.format(exit_code))
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
