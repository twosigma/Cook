import time
import json
import http.client

from threading import Event, Thread

from cook.server import run_server

class FakeStore():
    def merge(self, type, id, e):
        self.e = e

    def get(self, type, id):
        return self.e

def request(verb, path, body = None):
    conn = http.client.HTTPConnection('localhost:8080')
    if body:
        conn.request(verb, path, json.dumps(body))
    else:
        conn.request(verb, path)

    return json.loads(conn.getresponse().read().decode('utf-8'))

def test_run_server():
    event = Event()
    store = FakeStore()
    entity =  {'progress': 1.0}

    thread = Thread(target = run_server, args = (store, event))
    thread.daemon = True
    thread.start()

    request('PATCH', '/task/123', entity)

    time.sleep(1)

    assert store.get('task', '123') == entity
    assert store.get('task', '123') == request('GET', '/task/123')
