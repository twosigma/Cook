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

import sys

from http.server import BaseHTTPRequestHandler, HTTPServer

port = sys.argv[1]


class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Respond to a GET request."""
        if self.path == "/healthcheck":
            self.send_response(200)
            self.send_header("content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"OK")
            return
        elif self.path == "/files/download":
            url_tokens = self.path.split("?")
            if not url_tokens or len(url_tokens) < 1:
                return
            query_params = parse_qs(url_tokens[1])
            saml_request = query_params["SAMLRequest"][0]
            relay_state = query_params["RelayState"][0]

        return


def run(server_class=HTTPServer, handler_class=MyHandler):
    server_address = ('', int(port))
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()


run()
