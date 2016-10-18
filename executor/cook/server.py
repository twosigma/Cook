"""
An HTTP server exposing a simple API for updating task data. It is meant to be consumed by
the processes of a task's commands. It allows setting custom failure reasons and updating
task progress.
"""

import re
import json

from http.server import BaseHTTPRequestHandler, HTTPServer

class CookExecutorHTTPRequestHandler(BaseHTTPRequestHandler):
    """
    A BaseHTTPRequestHandler subclass implementing a very simple HTTP API
    """

    def do_GET(self):
        """
        GET /health
          => returns 200
        """
        if self.path is '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps('{"response": "okay"}'))

    def do_PATCH(self):
        """
        POST /task/:task_id
          => returns 200 if data is valid and we can merge it into the store
          => returns 400 if data is malformed
        """
        if re.match('\/task\/[^/]+', self.path):
            task_id = self.path.split('/')[2]
            task_data = self.parse_body()

            if self.server.store.merge('task', task_id, task_data):
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps('{"response": "okay"}').encode('utf-8'))
            else:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps('{"response": "malformed"}').encode('utf-8'))

    def parse_body(self):
        n = int(self.headers['content-length'] or 0)

        try:
            return json.loads(self.rfile.read(n).decode('utf-8'))
        except:
            return None

def run_server(store, stop = None, port = 8080):
    """
    Run a web server on the specified port until the stop event is set.
    """
    # TODO: get port from executor info?
    server = HTTPServer(('', port), CookExecutorHTTPRequestHandler)
    server.store = store
    server.timeout = 1

    while not (stop and stop.isSet()):
        server.handle_request()

    server.socket.close()
