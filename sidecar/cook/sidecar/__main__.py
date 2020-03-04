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
import sys
import threading

from cook.sidecar import exit_sentinel, file_server, progress, util
from cook.sidecar.version import VERSION


def main(args=None):
    util.init_logging()

    parser = argparse.ArgumentParser(description='Cook Sidecar')
    parser.add_argument('--exit-sentinel-file-path', metavar='PATH', help='file path which signals this process to exit when it appears')
    parser.add_argument('--file-server-port', type=int, metavar='PORT', help='file server port number')
    parser.add_argument('--file-server-threads', type=int, default=2, metavar='THREADS', help='file server threads-per-worker count')
    parser.add_argument('--file-server-workers', type=int, default=2, metavar='WORKERS', help='file server worker process count')
    parser.add_argument('--no-file-server', action='store_true', help='disable sandbox file server')
    parser.add_argument('--no-progress-reporter', action='store_true', help='disable progress reporter')
    parser.add_argument('--version', action='version', version=f'Cook Sidecar {VERSION}')
    options = parser.parse_args(args or sys.argv[1:])
    logging.info(f'OPTIONS = {options}')

    logging.info(f'Starting cook.sidecar {VERSION}')

    all_started_event = threading.Event()
    exit_code = 0

    # Start progress reporter workers (non-blocking)
    if not options.no_progress_reporter:
        progress_trackers = progress.start_progress_trackers()

    # Start exit sentinel file watcher thread
    if options.exit_sentinel_file_path:
        exit_sentinel.watch_for_file(options.exit_sentinel_file_path, all_started_event)

    # Start Flask file server (blocking)
    if not options.no_file_server:
        file_server_args = [options.file_server_port, options.file_server_workers, options.file_server_threads]
        exit_code = file_server.start_file_server(all_started_event, file_server_args)
    # Wait for progress reporter threads (blocking)
    elif not options.no_progress_reporter:
        all_started_event.set()
        progress.await_progress_trackers(progress_trackers)

    # Propagate file server's exit code
    sys.exit(exit_code)

if __name__ == '__main__':
    main()
