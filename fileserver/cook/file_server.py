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

from flask import Flask, jsonify, request, send_from_directory
from operator import itemgetter
from pathlib import Path
from stat import *

app = Flask(__name__)


@app.route('/files/download')
@app.route('/files/download.json')
def download():
    path = request.args.get('path')
    if path is None:
        return "Expecting 'path=value' in query.", 400
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    if not os.path.exists(file_path):
        return "", 404
    print(path)
    return send_from_directory(file_path, path, as_attachment=True)


@app.route('/files/read')
@app.route('/files/read.json')
def read():
    path = request.args.get('path')
    offset = request.args.get('offset', default=0, type=int)
    length = request.args.get('length', type=int)
    if path is None:
        return "Expecting 'path=value' in query.", 400
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    if not os.path.exists(file_path):
        return "", 404
    print(path)
    print(offset)
    print(length)
    f = open(file_path)
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
        return "Expecting 'path=value' in query.", 400
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    if not os.path.exists(file_path):
        return "", 404
    print(path)
    if not os.path.isdir(file_path):
        return jsonify([])
    print(os.listdir(file_path))
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
                                   for path in [os.path.join(file_path, f)
                                                for f in os.listdir(file_path)]]
    ]
    return jsonify(sorted(retval, key=itemgetter("path")))
