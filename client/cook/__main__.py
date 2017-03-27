#!/usr/bin/env python3
"""Module implementing a CLI for the Cook scheduler API. """

import sys

from cook.cli import cli

if __name__ == '__main__':
    okay, result = cli(sys.argv[1:])

    if type(result) is list:
        for line in result:
            print(line)
    elif result:
        print(result)

    exit(0 if okay else 1)
