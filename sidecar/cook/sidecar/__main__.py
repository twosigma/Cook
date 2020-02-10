#!/usr/bin/env python3
#
#  Copyright (c) 2020 Two Sigma Open Source, LLC
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to
#  deal in the Software without restriction, including without limitation the
#  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
#  IN THE SOFTWARE.
#

import argparse
import logging
import os
import sys

from cook.sidecar import file_server, progress, util
from cook.sidecar.version import VERSION


def main(args=None):
    util.init_logging()

    parser = argparse.ArgumentParser(description='Cook Sidecar')
    parser.add_argument('--no-file-server', action='store_true', help='Disable sandbox file server')
    parser.add_argument('--no-progress-reporter', action='store_true', help='Disable progress reporter')
    parser.add_argument('file_server_args', metavar='FILE_SERVER_ARGS', nargs=argparse.REMAINDER,
                        help='Arguments for file server: PORT [NUM_WORKERS]')
    parser.add_argument('--version', action='version', version=f'Cook Sidecar {VERSION}')
    options = parser.parse_args(args or sys.argv[1:])
    logging.info(f'OPTIONS = {options}')

    logging.info(f'Starting cook.sidecar {VERSION}')

    # Start progress reporter workers (non-blocking)
    if not options.no_progress_reporter:
        progress_trackers = progress.start_progress_trackers()

    # Start Flask file server (blocking)
    if not options.no_file_server:
        exit_code = file_server.start_file_server(options.file_server_args)
    else:
        exit_code = 0

    # Wait for progress reporter threads (blocking)
    if not options.no_progress_reporter:
        progress.await_progress_trackers(progress_trackers)

    # Propagate file server's exit code
    sys.exit(exit_code)

if __name__ == '__main__':
    main()
