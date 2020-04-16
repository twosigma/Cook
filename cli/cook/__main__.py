#!/usr/bin/env python3
"""Module implementing a CLI for the Cook scheduler API. """

import logging
import signal
import sys

from cook import util
from cook.cli import run
from cook.util import print_error


def main(args=None, plugins={}):
    if args is None:
        args = sys.argv[1:]

    try:
        result = run(args, plugins)
        sys.exit(result)
    except Exception as e:
        logging.exception('exception when running with %s' % args)
        print_error(str(e))
        sys.exit(1)


def sigint_handler(_, __):
    """
    Sets util.quit_running to True (which is read by other
    threads to determine when to stop), and then exits.
    """
    util.quit_running = True
    print('Exiting...')
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)

if __name__ == '__main__':
    main()
