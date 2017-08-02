#!/usr/bin/env python3
"""Module implementing a CLI for the Cook scheduler API. """

import logging
import sys

from cook.cli import cli


def main(args=None):
    """The main routine."""

    if args is None:
        args = sys.argv[1:]

    try:
        result = cli(args)
        exit(result)
    except Exception as e:
        logging.exception('exception when running with %s' % args)
        print(str(e), file=sys.stderr)
        exit(1)


if __name__ == '__main__':
    main()
