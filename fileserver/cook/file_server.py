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

import os.path

from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__)


@app.route('/files/download')
def download():
    path = request.args.get('path')
    if path is None:
        return "ERROR: No path given", 400
    print(path)
    return send_from_directory(os.path.dirname(os.path.realpath(__file__)), path, as_attachment=True)


@app.route('/files/read')
def read():
    path = request.args.get('path')
    offset = request.args.get('offset', default=0, type=int)
    length = request.args.get('length', type=int)
    if path is None:
        return "ERROR: No path given", 400
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    if not os.path.exists(file_path):
        return "ERROR: File {path} not found".format(path=path), 404
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
