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

import logging
import os
import sys

from cook.sidecar.file_server import FileServerApplication
from cook.sidecar.version import VERSION


def main(args=None):
    log_level = os.environ.get('EXECUTOR_LOG_LEVEL', 'INFO')
    logging.basicConfig(level = log_level,
                        stream = sys.stderr,
                        format='%(asctime)s %(levelname)s %(message)s')

    if args is None:
        args = sys.argv[1:]

    progress_reporter_enabled = True
    file_server_enabled = True

    while len(args) > 0 and args[0].startswith('--no-'):
        if args[0] == '--no-file-server':
            file_server_enabled = False
        elif args[0] == '--no-progress-reporting':
            progress_reporter_enabled = False
        else:
            logging.warning(f'Ignoring unrecognized flag: {args[0]}')
        args.pop(0)

    logging.info(f'Starting cook.sidecar {__version__}')

    if progress_reporter_enabled:
        progress_trackers = progress.start_progress_trackers()

    if file_server_enabled:
        file_server.start_file_server(args)

    if progress_reporter_enabled:
        progress.await_progress_trackers(progress_trackers)

if __name__ == '__main__':
    main()
