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
"""Module implementing the Mesos file access REST API to serve Cook job logs. """

import logging
import os
import signal
import sys
from operator import itemgetter
from pathlib import Path
from stat import *

import gunicorn.app.base
import gunicorn.arbiter
from flask import Flask, jsonify, request, send_file

from cook.sidecar import util
from cook.sidecar.version import VERSION

app = Flask(__name__)
sandbox_directory = None
max_read_length = int(os.environ.get('COOK_FILE_SERVER_MAX_READ_LENGTH', '25000000'))


def start_file_server(started_event, args):
    try:
        logging.info(f'Starting cook.sidecar {VERSION} file server')
        port, workers, threads = (args + [None] * 3)[0:3]
        if port is None:
            logging.error('Must provide file server port')
            sys.exit(1)
        cook_workdir = os.environ.get('COOK_WORKDIR')
        if not cook_workdir:
            logging.error('COOK_WORKDIR environment variable must be set')
            sys.exit(1)
        FileServerApplication(cook_workdir, started_event, {
            'bind': f'0.0.0.0:{port}',
            'threads': 2 if threads is None else threads,
            'workers': 4 if workers is None else workers,
        }).run()
        return 0

    except Exception as e:
        logging.exception(f'exception when running file server with {args}')
        return 1


class FileServerArbiter(gunicorn.arbiter.Arbiter):
    '''
    Custom Gunicorn Arbiter object,
    with overridden logic for re-installing existing signal handlers,
    and an event to indicate when the server is up and running.
    '''

    def __init__(self, app, started_event):
        self.started_event = started_event
        # Since gunicorn installs its own signal handler routine for basically all signals,
        # we need to save any existing signal handlers that we want preserved and then
        # inject them back into gunicorn's hanlder (injected via the `signal` method below).
        self.user_signal_handlers = {}
        for sig in range(1, signal.NSIG):
            user_handler = signal.getsignal(sig)
            if callable(user_handler):
                logging.info(f'Saving user handler for signal {sig}')
                self.user_signal_handlers[sig] = user_handler
        super().__init__(app)

    def start(self):
        super().start()
        # Gunicorn invokes the `when_ready` hook at the end of the Arbiter's `start` method.
        # https://docs.gunicorn.org/en/stable/settings.html#when-ready
        # However, since we already needed a custom Arbiter for signal handling logic,
        # providing that custom hook was much more complex than signaling
        # here that the file server has started than additionally providing a custom Config object.
        logging.info(f'Sidecar file server is ready')
        self.started_event.set()

    def signal(self, sig, frame):
        '''Generic signal handler for all signals, installed by gunicorn.'''
        # Invoke the user's handler for the signal (if present).
        # See comment in `__init__` above for more details.
        user_handler = self.user_signal_handlers.get(sig)
        if user_handler is not None:
            logging.info(f'Entering user handler for signal {sig}')
            user_handler(sig, frame)
            logging.info(f'Exiting user handler for signal {sig}')
        # Enter gunicorn's handler for the signal.
        super().signal(sig, frame)


class FileServerApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, cook_workdir, started_event, options=None):
        self.options = options or {}
        self.application = app
        self.started_event = started_event
        global sandbox_directory
        sandbox_directory = cook_workdir
        super(FileServerApplication, self).__init__()

    def load_config(self):
        for key, value in self.options.items():
            if key in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

    def run(self):
        try:
            FileServerArbiter(self, self.started_event).run()
        except RuntimeError as e:
            logging.exception('Error while running cook.sidecar file server')


def path_is_valid(path):
    if not os.path.exists(path):
        return False
    normalized_path = os.path.normpath(path)
    return normalized_path.startswith(sandbox_directory)


@app.route('/files/download')
@app.route('/files/download.json')
def download():
    path = request.args.get('path')
    if path is None:
        return "Expecting 'path=value' in query.\n", 400
    if not path_is_valid(path):
        return "", 404
    if os.path.isdir(path):
        return "Cannot download a directory.\n", 400
    return send_file(path, as_attachment=True)


@app.route('/files/read')
@app.route('/files/read.json')
def read():
    path = request.args.get('path')
    offset_param = request.args.get('offset', -1)
    length_param = request.args.get('length', -1)
    if path is None:
        return "Expecting 'path=value' in query.\n", 400
    try:
        offset = int(offset_param)
    except ValueError as _:
        return f"Failed to parse offset: Failed to convert '{offset_param}' to number.\n", 400
    if offset < -1:
        return f"Negative offset provided: {offset_param}.\n", 400
    try:
        length = int(length_param)
    except ValueError as _:
        return f"Failed to parse length: Failed to convert '{length_param}' to number.\n", 400
    if length < -1:
        return f"Negative length provided: {length_param}.\n", 400
    if not path_is_valid(path):
        return "", 404
    if os.path.isdir(path):
        return "Cannot read a directory.\n", 400
    if offset == -1:
        return jsonify({
            "data": "",
            "offset": os.path.getsize(path),
        })
    f = open(path)
    f.seek(offset)
    length = max_read_length if length == -1 else length
    if length > max_read_length:
        return f"Requested length for file read, {length} is greater than max allowed length, {max_read_length}", 400
    data = f.read(length)
    f.close()
    return jsonify({
        "data": data,
        "offset": offset,
    })


def make_permission_string(permission_bits):
    return ''.join(["rwxrwxrwx"[i] if (permission_bits & (1 << (8 - i)) != 0) else "-" for i in range(0, 9)])


@app.route('/files/browse')
@app.route('/files/browse.json')
def browse():
    path = request.args.get('path')
    if path is None:
        return "Expecting 'path=value' in query.\n", 400
    if not path_is_valid(path):
        return "", 404
    if not os.path.isdir(path):
        return jsonify([])
    retval = [
        {
            "gid": path_obj.group(),
            "mode": ('d' if S_ISDIR(st.st_mode) else '-') + make_permission_string(S_IMODE(st.st_mode) % 512),
            "mtime": int(st.st_mtime),
            "nlink": st.st_nlink,
            "path": path,
            "size": st.st_size,
            "uid": path_obj.owner(),
        }
        for st, path_obj, path in [(os.stat(path), Path(path), path)
                                   for path in [os.path.join(path, f)
                                                for f in os.listdir(path)]]
    ]
    return jsonify(sorted(retval, key=itemgetter("path")))


# This endpoint is not part of the Mesos API. It is used by the kubernetes readiness probe on the sidecar container.
# Cook will see that the sidecar is ready to serve files and will set the output_url. If we expose the output_url
# before the server is ready, then someone might use it and get an error.
@app.route('/readiness-probe')
def readiness_probe():
    return ""


def main():
    util.init_logging()
    if len(sys.argv) == 2 and sys.argv[1] == "--version":
        print(VERSION)
    else:
        start_file_server(sys.argv[1:])


if __name__ == '__main__':
    main()
