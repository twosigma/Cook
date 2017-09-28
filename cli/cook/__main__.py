#!/usr/bin/env python3
"""Module implementing a CLI for the Cook scheduler API. """

import logging
import sys

from cook import colors
from cook.cli import run


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    try:
        result = run(args)
        sys.exit(result)
    except Exception as e:
        logging.exception('exception when running with %s' % args)
        print(colors.failed(str(e)), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
