#!/usr/bin/env python3

"""The primary entry point for Cook's custom executor.
This module configures logging and starts the executor's driver thread.
"""

import logging
import signal
import sys
from threading import Event, Thread

import os

# CPython bug: including the idna encoding registers it,
# the encoding is loaded with the built-in frozen importer
# https://github.com/pyinstaller/pyinstaller/issues/1113
import encodings.idna

import cook.config as cc
import cook.executor as ce


def main(args=None):
    print('Starting cook executor...')

    environment = os.environ
    executor_id = environment.get('MESOS_EXECUTOR_ID', '1')
    log_level = environment.get('EXECUTOR_LOG_LEVEL', 'INFO')

    logging.basicConfig(level = log_level,
                        filename = 'executor.log',
                        format='%(asctime)s %(levelname)s %(message)s')
    logging.info('Starting cook executor {}'.format(executor_id))
    logging.info('Log level is {}'.format(log_level))

    config = cc.initialize_config(environment)

    def handle_interrupt(interrupt_code, _):
        logging.info('Received interrupt code {}, preparing to terminate executor'.format(interrupt_code))
        stop_signal.set()
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    stop_signal = Event()
    driver_thread = Thread(target=ce.run_mesos_driver, args=(stop_signal, config))
    driver_thread.start()
    logging.info('Driver thread has started')
    driver_thread.join()
    logging.info('Driver thread has completed')

    exit_code = 1 if stop_signal.isSet() else 0
    logging.info('Executor exiting with code {}'.format(exit_code))
    sys.exit(exit_code)

if __name__ == '__main__':
    main()
