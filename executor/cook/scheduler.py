#!/usr/bin/env python3

"""
An implementation of a Mesos scheduler, using the agent HTTP API via pymesos.

It exists only to test the custom executor, and assumes Mesos is running locally.
"""

import os
import sys
import time
import uuid
import json
import signal
import logging

from threading import Thread

from pymesos import MesosSchedulerDriver, Scheduler, encode_data, decode_data

def new_framework_info(name):
    return {
        'user': '',
        'name': name
    }

def new_executor_info():
    root = os.path.dirname(os.path.realpath(__file__))
    uris = ['__main__.py', 'executor.py', 'launcher.py', 'store.py', 'server.py']

    return {
        'name': 'cook-executor',
        'executor_id': {'value': 'cook-executor'},
        'command': {
            'value': 'python3 -u __main__.py',
            'uris': [{
                'extract': False,
                'value': root + '/' + u
            } for u in uris]
        }
    }

def new_task_info(offer):
    id = uuid.uuid4()

    commands = [
        {'name': '0', 'value': 'sleep 10', 'async': True},
        {'name': '1', 'value': 'echo 1'},
        {'name': '2', 'value': 'echo 2'},
        {'value': 'echo default'},
    ]

    return {
        'task_id': {'value': str(id)},
        'agent_id': offer['agent_id'],
        'name': 'task {}'.format(str(id)),
        'data': encode_data(json.dumps({'commands': commands}).encode('utf8')).decode('utf8'),
        'resources': [
            dict(name = 'mem', type = 'SCALAR', scalar = {'value': 1}),
            dict(name = 'cpus', type = 'SCALAR', scalar = {'value': 1})
        ],
        'executor': new_executor_info()
    }

TERMINAL_TASK_STATES = set([
    'TASK_LOST',
    'TASK_ERROR',
    'TASK_FAILED',
    'TASK_KILLED',
    'TASK_FINISHED'
])

def decode_payload(p):
    return json.loads(decode_data(p).decode('utf8'))

class CookScheduler(Scheduler):
    def __init__(self):
        self.task = None

    def resourceOffers(self, driver, offers):
        logging.info('CookScheduler:resourceOffers')

        if not self.task:
            self.task = new_task_info(offers[0])
            time.sleep(1)
            driver.launchTasks(offers[0]['id'], [self.task])

    def statusUpdate(self, driver, status):
        task_id = status['task_id']['value']

        logging.info('CookScheduler:statusupdate: %s %s' %
                     (status['state'], status.get('data') and decode_payload(status['data'])))

        # if status.state in TERMINAL_TASK_STATES:
        #     driver.stop()

if __name__ == '__main__':
    logging.basicConfig(level = "DEBUG")
    logging.info('Starting Cook Scheduler')

    scheduler = CookScheduler()
    framework = new_framework_info('cook-scheduler')
    # master = 'zk://localhost:2181/mesos'
    master = 'localhost'
    driver = MesosSchedulerDriver(scheduler, framework, master)

    driver_thread = Thread(target = lambda: driver.run(), args = ())
    driver_thread.start()

    print('(Listening for Ctrl-C)')
    signal.signal(signal.SIGINT, lambda *_: driver.stop())

    while driver_thread.is_alive():
        time.sleep(1)

    print('Goodbye!')
    sys.exit(0)
