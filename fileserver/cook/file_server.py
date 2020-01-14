#!/usr/bin/env python3
#
#  Copyright (c) 2019 Two Sigma Open Source, LLC
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

import os
from operator import itemgetter
from pathlib import Path
from stat import *

import gunicorn.app.base
from flask import Flask, jsonify, request, send_file

app = Flask(__name__)
sandbox_directory = None
max_read_length = int(os.getenv('COOK_FILE_SERVER_MAX_READ_LENGTH', '25000000'))


class FileServerApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, cook_workdir, options=None):
        self.options = options or {}
        self.application = app
        global sandbox_directory
        sandbox_directory = cook_workdir
        super(FileServerApplication, self).__init__()

    def load_config(self):
        for key, value in self.options.items():
            if key in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


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


def try_parse_int(param_name, val):
    def err_message():
        return f"Failed to parse {param_name}: Failed to convert '{val}' to number.\n"
    try:
        int_val = int(val)
    except ValueError as _:
        return (None, err_message())
    except Exception as _:
        return (None, err_message())
    if int_val < -1:
        return f"Negative {param_name} provided: {int_val}.\n", 400
    return (int_val, None)


@app.route('/files/read')
@app.route('/files/read.json')
def read():
    path = request.args.get('path')
    offset_param = request.args.get('offset', -1)
    length_param = request.args.get('length', -1)
    if path is None:
        return "Expecting 'path=value' in query.\n", 400
    offset, err = try_parse_int("offset", offset_param)
    if not err is None:
        return err, 400
    length, err = try_parse_int("length", length_param)
    if not err is None:
        return err, 400
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


permission_strings = {
    '0': '---',
    '1': '--x',
    '2': '-w-',
    '3': '-wx',
    '4': 'r--',
    '5': 'r-x',
    '6': 'rw-',
    '7': 'rwx',
}
# maps permission bit masks to a linux permission string. e.g. 775 -> "rwxrwxr-x"
permissions = {n - 512: ''.join(permission_strings[c] for c in str(oct(n))[-3:]) for n in range(512, 1024)}


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
            "mode": ('d' if S_ISDIR(st.st_mode) else '-') + permissions[S_IMODE(st.st_mode) % 512],
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


# This endpoint is not part of the Mesos API. It is used by the kubernetes readiness probe on the fileserver container.
# Cook will see that the fileserver is ready to serve files and will set the output_url. If we expose the output_url
# before the server is ready, then someone might use it and get an error.
@app.route('/readiness-probe')
def readiness_probe():
    return ""
