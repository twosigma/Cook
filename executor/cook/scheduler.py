#!/usr/bin/env python

import os
import sys
import time
import uuid
import json
import signal

from threading import Thread

from mesos.scheduler import MesosSchedulerDriver
from mesos.interface import Scheduler, mesos_pb2

def new_framework_info(name):
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ''
    framework.name = name

    return framework

def new_executor_info():
    root = os.path.dirname(os.path.realpath(__file__))
    uris = [
        'executor.py',
        'command.py'
    ]

    executor = mesos_pb2.ExecutorInfo()
    executor.name = 'cook-executor'
    executor.executor_id.value = 'cook-executor'
    executor.command.value = 'python executor.py'

    for uri in uris:
        u = executor.command.uris.add()
        u.value = root + '/' + uri
        u.extract = False

    return executor

def new_task_info(offer):
    task = mesos_pb2.TaskInfo()

    id = uuid.uuid4()

    commands = [{
        'value': 'echo 1'
    }]

    task.task_id.value = str(id)
    task.slave_id.value = offer.slave_id.value
    task.name = 'task {}'.format(str(id))
    task.data = json.dumps(commands, separators=(',', ':'))

    cpus = task.resources.add()
    cpus.name = 'cpus'
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 1

    mem = task.resources.add()
    mem.name = 'mem'
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 1

    task.executor.MergeFrom(new_executor_info())

    return task

class CookScheduler(Scheduler):
    def __init__(self):
        self.task = None

    def registered(self, driver, frameworkId, masterInfo):
        print 'CookScheduler registered'

    def reregistered(self, driver, masterInfo):
        print 'CookScheduler reregistered'

    def resourceOffers(self, driver, offers):
        print 'CookScheduler resourceOffers'

        if not self.task:
            self.task = new_task_info(offers[0])
            time.sleep(1)
            driver.launchTasks(offers[0].id, [self.task])

    def disconnected(self, driver):
        print 'CookScheduler disconnected'
        pass

if __name__ == '__main__':
    print 'Starting Cook Scheduler'

    scheduler = CookScheduler()
    framework = new_framework_info('cook-scheduler')
    master = 'zk://localhost:2181/mesos'
    driver = MesosSchedulerDriver(scheduler, framework, master)

    def run_driver():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    driver_thread = Thread(target = run_driver, args = ())
    driver_thread.start()

    print '(Listening for Ctrl-C)'
    signal.signal(signal.SIGINT, lambda *_: driver.stop())
    while driver_thread.is_alive():
        time.sleep(1)

    print 'Goodbye!'
    sys.exit(0)
