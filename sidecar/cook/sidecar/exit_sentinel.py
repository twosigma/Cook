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
"""Cook sidecar exit sentinel file watcher thread logic."""

import logging
import os
import signal
import threading
import time

def watch_for_file(sentinel_file_path, started_event):
    def daemon_routine():
        # wait for other components to finish starting
        logging.info(f'Waiting for all components to start...')
        started_event.wait()
        # wait for sentinel file to appear
        logging.info(f'Watching for sentinel file: {sentinel_file_path}')
        while not os.path.exists(sentinel_file_path):
            time.sleep(0.1)
        # trigger this process's termination handler
        logging.info(f'Sidecar termination triggered by sentinel file: {sentinel_file_path}')
        os.kill(os.getpid(), signal.SIGTERM)
    threading.Thread(target=daemon_routine, args=(), daemon=True).start()
