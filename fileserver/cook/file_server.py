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
"""Module implementing a file server to serve Cook job logs. """

import os
from operator import itemgetter
from pathlib import Path
from stat import *

from flask import Flask, jsonify, request, send_file

import gunicorn.app.base

from gunicorn.six import iteritems

app = Flask(__name__)


class FileServerApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, options=None):
        self.options = options or {}
        self.application = app
        super(FileServerApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

@app.route('/files/download')
@app.route('/files/download.json')
def download():
    path = request.args.get('path')
    if path is None:
        return "Expecting 'path=value' in query.\n", 400
    if not os.path.exists(path):
        return "", 404
    if os.path.isdir(path):
        return "Cannot download a directory.\n", 400
    return send_file(path, as_attachment=True)


def try_parse_int(param_name, val):
    err_message = lambda: f"Failed to parse {param_name}: Failed to convert '{val}' to number.\n"
    try:
        int_val = int(val)
    except ValueError as err:
        return (None, err_message())
    except Exception as ex:
        return (None, err_message())
    return (int_val, None)


@app.route('/files/read')
@app.route('/files/read.json')
def read():
    path = request.args.get('path')
    offset_param = request.args.get('offset')
    length_param = request.args.get('length')
    if path is None:
        return "Expecting 'path=value' in query.\n", 400
    offset, err = (None, None) if offset_param is None else try_parse_int("offset", offset_param)
    if not err is None:
        return err, 400
    length, err = (None, None) if length_param is None else try_parse_int("length", length_param)
    if not err is None:
        return err, 400
    if not length is None and length < 0:
        return f"Negative length provided: {length}.\n", 400
    if not os.path.exists(path):
        return "", 404
    if os.path.isdir(path):
        return "Cannot read a directory.\n", 400
    if offset is None or offset < 0:
        return jsonify({
            "data": "",
            "offset": os.path.getsize(path),
        })
    f = open(path)
    f.seek(offset)
    data = f.read() if length is None else f.read(length)
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
    '7': 'rwx'
}
# maps permission bit masks to a linux permission string. e.g. 775 -> "rwxrwxr-x"
permissions = {n - 512: ''.join(permission_strings[c] for c in str(oct(n))[-3:]) for n in range(512, 1024)}


@app.route('/files/browse')
@app.route('/files/browse.json')
def browse():
    path = request.args.get('path')
    if path is None:
        return "Expecting 'path=value' in query.\n", 400
    if not os.path.exists(path):
        return "", 404
    if not os.path.isdir(path):
        return jsonify([])
    retval = [
        {
            "gid": path_obj.group(),
            "mode": ('d' if S_ISDIR(st.st_mode) else '-') + permissions[S_IMODE(st.st_mode)],
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
