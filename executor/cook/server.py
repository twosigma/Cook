"""
An HTTP server exposing a simple API for updating task data. It is meant to be consumed by
the processes of a task's commands. It allows setting custom failure messages and updating
task progress.
"""

import re
import json
import socket
import logging

from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

class CookExecutorHTTPRequestHandler(BaseHTTPRequestHandler):
    """
    A BaseHTTPRequestHandler subclass implementing a very simple HTTP API
    """

    def do_GET(self):
        """
        GET /health
          => returns 200
        GET /task/:task_id
          => returns 200 and task data if task_id is valid
          => returns 404 if task_id is unknown or invalid
        """
        if self.path is '/health':
            self.send_json_response(200, {'response': 'okay'})
        elif re.match('\/task\/[^/]+', self.path):
            task_id = self.path.split('/')[2]
            task_data = self.server.store.get('task', task_id)

            if task_data:
                self.send_json_response(200, task_data)
            else:
                self.send_json_response(404, {'response': 'not found'})

    def do_PATCH(self):
        """
        POST /task/:task_id
          => returns 200 if data is valid and we can merge it into the store
          => returns 400 if data is malformed
        """
        if re.match('\/task\/[^/]+', self.path):
            task_id = self.path.split('/')[2]
            task_data = self.parse_body()

            if all(k in ('progress', 'message') for k in task_data) and \
               self.server.store.merge('task', task_id, task_data):
                self.send_json_response(200, {'response': 'okay'})
            else:
                self.send_json_response(400, {'response': 'malformed'})

    def parse_body(self):
        n = int(self.headers['content-length'] or 0)

        try:
            return json.loads(self.rfile.read(n).decode('utf-8'))
        except:
            return None

    def send_json_response(self, code, content):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(content).encode('utf-8'))

class CookExecutorHTTPServer():
    def __init__(self, store, event = None, port = 8080):
        self.port = port
        self.store = store
        self.event = event

    def start(self):
        self.server = HTTPServer(('', self.port), CookExecutorHTTPRequestHandler)
        self.server.store = self.store
        self.server.timeout = 1

        def run_server(server, event = None):
            while not (event and event.isSet()):
                try:
                    server.handle_request()
                except ValueError:
                    # the underlying socket already closed
                    pass

            try:
                server.socket.shutdown(socket.SHUT_RDWR)
                server.socket.close()
            except socket.error:
                # socket already closed
                pass

        self.thread = Thread(target = run_server, args = (self.server, self.event))
        self.thread.daemon = True
        self.thread.start()

        return True

    def stop(self):
        if self.server:
            try:
                self.server.socket.shutdown(socket.SHUT_RDWR)
                self.server.socket.close()
            except socket.error:
                # socket already closed
                pass

        return True
