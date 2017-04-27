#!/usr/bin/env python3

import cook
import cook.progress as cp
import json
import logging
import subprocess
import time

from pymesos import Executor, MesosExecutorDriver, decode_data, encode_data
from threading import Event, Thread


def get_task_id(task):
    """
        :param task: The task
        :return: The id of the task.
    """
    return task['task_id']['value']


def create_status(task_id, task_state):
    """
        :param task_id: The task id.
        :param task_state: The state of the task to report.
        :return: a status dictionary that can be sent to the driver.
    """
    return {'task_id': {'value': task_id},
            'state': task_state,
            'timestamp': time.time()}


def update_status(driver, task_id, task_state):
    """
        :param driver: The driver to send the status update to.
        :param task: The task whose status update to send.
        :param task_state: The state of the task which will be sent to the driver.
        :return: Nothing.
    """
    logging.debug('Updating task {} state to {}'.format(task_id, task_state))
    status = create_status(task_id, task_state)
    driver.sendStatusUpdate(status)


def send_message(driver, message, max_message_length):
    """
        :param driver: The driver to send the message to.
        :param message: The raw message to send.
        :param max_message_length: The allowed max message length after encoding
        :return: whether the message was successfully sent
    """
    logging.debug('Sending framework message {}'.format(message))
    encoded_message = encode_data(str(message).encode('utf8'))
    if len(encoded_message) < max_message_length:
        driver.sendFrameworkMessage(encoded_message)
        return True
    else:
        log_message_template = 'Unable to send message {} as it exceeds allowed max length of {}'
        logging.info(log_message_template.format(message, max_message_length))
        return False


def launch_task(driver, task, stdout_name, stderr_name):
    """
        :param driver: The mesos driver to use.
        :param task: The task to execute.
        :param stdout_name: The file to use to redirect stdout.
        :param stderr_name: The file to use to redirect stderr.
        :return: The tuple containing the process launched, stdout file, and stderr file.
    """
    task_id = get_task_id(task)
    try:
        update_status(driver, task_id, cook.TASK_RUNNING)

        command = decode_data(task['data']).decode('utf8')
        logging.info('Command: {}'.format(command))
        stdout = open(stdout_name, 'a+')
        stderr = open(stderr_name, 'a+')
        process = subprocess.Popen(command, shell=True, stdout=stdout, stderr=stderr)

        return process, stdout, stderr
    except Exception:
        logging.exception('Error in launch_task')
        update_status(driver, task_id, cook.TASK_FAILED)


def is_running(process):
    """
        :param process: The process to query
        :return: whether the process is still running.
    """
    return process.poll() is None


def cleanup_process(process_info):
    """
        :param process_info: Tuple of (process, stdout_file, stderr_file)
        :return: Nothing
    """
    _, stdout, stderr = process_info
    logging.info('Closing {}'.format(stdout.name))
    stdout.close()
    logging.info('Closing {}'.format(stderr.name))
    stderr.close()


def kill_task(driver, task, process_info, shutdown_grace_period_ms):
    """
        Attempts to kill a process by sending it a SIGTERM.
        If the process does not terminate inside (shutdown_grace_period_ms - 100) ms, it is then sent a SIGKILL.
        100 ms grace period is allocated for the executor to perform its other cleanup actions.
        :param driver: The mesos driver to use.
        :param task: The task to execute.
        :param process_info: Tuple of (process, stdout_file, stderr_file)
        :param shutdown_grace_period_ms: Grace period before forceful kill
        :return: Nothing
    """
    task_id = get_task_id(task)
    try:
        shutdown_grace_period_ms = max(shutdown_grace_period_ms - 100, 0)
        process, _, _ = process_info
        if is_running(process):
            logging.info('Waiting up to {} ms for process to terminate'.format(shutdown_grace_period_ms))
            process.terminate()
            loop_limit = int(shutdown_grace_period_ms / 10)
            for i in range(loop_limit):
                time.sleep(0.01)
                if not is_running(process):
                    logging.info('Process has terminated')
                    break
            if is_running(process):
                logging.info('Process did not terminate, forcefully killing it')
                process.kill()
        cleanup_process(process_info)
        update_status(driver, task_id, cook.TASK_KILLED)
    except:
        logging.exception('Error in kill_task')
        update_status(driver, task_id, cook.TASK_FAILED)


def await_process_completion(driver, task, stop_signal, process_info, shutdown_grace_period_ms):
    """
        :param driver: The mesos driver to use.
        :param task: The task to execute.
        :param stop_signal: Event that determines if an interrupt was sent
        :param process_info: Tuple of (process, stdout_file, stderr_file)
        :param shutdown_grace_period_ms: Grace period before forceful kill
        :return: True if the process was killed, False if it terminated naturally.
    """
    process, _, _ = process_info

    process_killed = False
    while is_running(process):

        if stop_signal.isSet():
            logging.info('Executor has been instructed to terminate')
            kill_task(driver, task, process_info, shutdown_grace_period_ms)
            process_killed = True
            break

        time.sleep(1)
    return process_killed


def manage_task(driver, task, stop_signal, completed_signal, config, stdout_name, stderr_name):
    """
        Manages the execution of a task.
        :return: Nothing
    """
    task_id = get_task_id(task)
    try:
        sandbox_message = json.dumps({'sandbox-location': config.sandbox_location, 'task-id': task_id})
        send_message(driver, sandbox_message, config.max_message_length)

        process_info = launch_task(driver, task, stdout_name, stderr_name)
        process, _, _ = process_info

        progress_watcher = cp.ProgressWatcher(config, completed_signal)
        progress_updater = cp.ProgressUpdater(config.progress_sample_interval_ms)
        progress_complete_event = cp.launch_progress_tracker(driver, task_id, config.max_message_length,
                                                             progress_watcher, progress_updater, send_message)

        process_killed = await_process_completion(driver, task, stop_signal, process_info,
                                                  config.shutdown_grace_period_ms)
        exit_code = process.returncode

        # set the completed signal to trigger progress updater termination and await progress
        # updater termination if executor is terminating normally
        completed_signal.set()
        if not stop_signal.isSet():
            logging.info('Awaiting progress updater completion with timeout of one second')
            progress_complete_event.wait(1)

        logging.info('Process completed with exit code {}'.format(exit_code))
        exit_message = json.dumps({'exit-code': exit_code, 'task-id': task_id})
        send_message(driver, exit_message, config.max_message_length)

        # send the latest progress state if available
        latest_progress = progress_watcher.current_progress()
        progress_updater.send_progress_update(driver, task_id, config.max_message_length, latest_progress, send_message,
                                              force_send=True)

        if not process_killed:
            task_state = cook.TASK_FINISHED if exit_code == 0 else cook.TASK_FAILED
            update_status(driver, task_id, task_state)

    except Exception:
        logging.exception('Error in executing task')
        update_status(driver, task_id, cook.TASK_FAILED)

    finally:
        # ensure completed_signal is set
        completed_signal.set()


def run_mesos_driver(stop_signal, config):
    """
        Run an executor driver until the stop_signal event is set or the first task it runs completes.
    """
    executor = CookExecutor(stop_signal, config)
    driver = MesosExecutorDriver(executor)
    driver.start()

    # check the status of the executor and bail if it has crashed
    while not executor.has_stopped():
        time.sleep(1)
    else:
        logging.info('Executor thread has completed')

    driver.stop()


class CookExecutor(Executor):
    """
        This class is responsible for launching the task sent by the scheduler.
    """
    def __init__(self, stop_signal, config):
        self.stop_signal = stop_signal
        self.completed_signal = Event()
        self.config = config

    def registered(self, driver, executor_info, framework_info, agent_info):
        logging.info('Executor registered executor={}, framework={}, agent={}'.
                     format(executor_info['executor_id']['value'], framework_info['id'], agent_info['id']['value']))

    def reregistered(self, driver, agent_info):
        logging.info('Executor re-registered agent={}'.format(agent_info))

    def disconnected(self, driver):
        logging.info('Executor disconnected!')

    def launchTask(self, driver, task):
        logging.info('Driver {} launching task {}'.format(driver, task))
        thread = Thread(target=manage_task,
                        args=(driver, task, self.stop_signal, self.completed_signal, self.config, 'stdout', 'stderr'))
        thread.start()

    def killTask(self, driver, task_id):
        logging.info('Task {} has been killed by Mesos'.format(task_id))
        self.stop_signal.set()

    def shutdown(self, driver):
        logging.info('Mesos requested executor to shutdown!')
        self.stop_signal.set()

    def has_stopped(self):
        return self.completed_signal.isSet()
