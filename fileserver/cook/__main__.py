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

import logging
import os
import sys

from cook.file_server import FileServerApplication
from cook.version import VERSION


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    try:
        print(f'Starting cook.file_server {VERSION}')
        port, workers = (args + [None] * 2)[0:2]
        if port is None:
            logging.error('Must provide port')
            sys.exit(1)
        cook_workdir = os.getenv('COOK_WORKDIR')
        if not cook_workdir:
            logging.error('COOK_WORKDIR environment variable must be set')
            sys.exit(1)
        FileServerApplication(cook_workdir, {
            'bind': f'0.0.0.0:{port}',
            'workers': 4 if workers is None else workers,
        }).run()

    except Exception as e:
        logging.exception('exception when running with %s' % args)
        sys.exit(1)


if __name__ == '__main__':
    main()
