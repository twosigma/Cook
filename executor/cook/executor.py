"""
An implementation of a Mesos executor, using the agent HTTP API via pymesos.

It's primary responsibility is responding to Mesos callbacks and updating the status of the
running task.
"""

import json
import time
import uuid
import logging

from pymesos import MesosExecutorDriver, Executor, encode_data, decode_data

from cook.store import WATCH_ACTION_PUT

def new_task_info(id, state, data = None):
    """
    Create a new TaskInfo dict.
    """
    return {
        'state': state,
        'task_id': {'value': id},
        # FIXME: setting uuid manually is a workaround for a bug in pymesos
        # https://github.com/douban/pymesos/issues/33
        'uuid': encode_data(uuid.uuid4().bytes).decode('utf8'),
        'data': encode_data(json.dumps(data).encode('utf8')).decode('utf8') if data else ''
    }

class CookExecutor(Executor):
    def __init__(self, store, event, server, sandbox):
        self.store = store
        self.event = event
        self.server = server
        self.sandbox = sandbox

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        """
        Called when the executor is registered with the Mesos agent.

        Adds a watch to store in order to keep task info in sync.
        """
        logging.info("CookExecutor:registered")

        def sync_task_status(action, type, id, entity):
            if action is WATCH_ACTION_PUT and type is 'task':
                state = entity.get('state')

                entity.update({'sandbox': self.sandbox})

                if state:
                    if state == 'TASK_FAILED' or state == 'TASK_FINISHED':
                        self.server.stop()
                    driver.sendStatusUpdate(new_task_info(id, entity.get('state'), entity))

        self.store.add_watch('sync_task_status', sync_task_status)

    def reregistered(self, driver, slaveInfo):
        logging.info("CookExecutor:reregistered")

    def disconnected(self, driver):
        logging.info("CookExecutor:disconnected")

    def launchTask(self, driver, task):
        """
        Called when a new task is launched. The custom executor can currently only handle
        launching one task.

        Rather than doing any actual work here, the task is added to the store. The watch
        added in self.registered will send the 'TASK_RUNNING' status update.

        """
        logging.info("CookExecutor:launchTask")

        try:
            task_id = task['task_id']['value']
            task_data = json.loads(decode_data(task['data']).decode('utf-8'))

            self.server.start()

            self.store.put('task', task_id, task_data)
            self.store.merge('task', task_id, {'state': 'TASK_STARTING'})
        except Exception as e:
            logging.exception("Exception in CookExecutor:launchTask")

            driver.sendStatusUpdate(
                new_task_info(task['task_id']['value'], 'TASK_FAILED'))
            raise e

    def killTask(self, driver, taskId):
        logging.info("CookExecutor:killTask")
        self.event.set()

    def frameworkMessage(self, driver, message):
        logging.info("CookExecutor:frameworkMessage")

    def shutdown(self, driver):
        logging.info("CookExecutor:shutdown")
        self.event.set()

    def error(self, error, message):
        logging.info("CookExecutor:error")

def run_executor(store, stop, server, sandbox = ''):
    """
    Run an executor driver until the stop event is set.
    """
    driver = MesosExecutorDriver(CookExecutor(store, stop, server, sandbox))
    driver.start()

    # TODO: check the status of the driver and bail if it's crashed
    while not stop.isSet():
        time.sleep(1)

    driver.stop()
