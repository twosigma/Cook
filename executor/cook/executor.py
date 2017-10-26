#!/usr/bin/env python3

import json
import logging
import subprocess
import time
from threading import Event, Thread

from pymesos import Executor, MesosExecutorDriver, decode_data, encode_data

import cook
import cook.io_helper as cio
import cook.progress as cp


def get_task_id(task):
    """Retrieves the id of the task.

    Parameters
    ----------
    task: map
        The task

    Returns
    -------
    the id of the task.
    """
    return task['task_id']['value']


def create_status(task_id, task_state):
    """Creates a dictionary representing the task status.

    Parameters
    ----------
    task_id: The task id.
    task_state: The state of the task to report.

    Returns
    -------
    a status dictionary that can be sent to the driver.
    """
    return {'task_id': {'value': task_id},
            'state': task_state,
            'timestamp': time.time()}


def update_status(driver, task_id, task_state):
    """Sends the status using the driver. 

    Parameters
    ----------
    driver: MesosExecutorDriver
        The driver to send the status update to.
    task: map
        The task whose status update to send.
    task_state: string
        The state of the task which will be sent to the driver.

    Returns
    -------
    Nothing.
    """
    logging.debug('Updating task {} state to {}'.format(task_id, task_state))
    status = create_status(task_id, task_state)
    driver.sendStatusUpdate(status)


def send_message(driver, message, max_message_length):
    """Sends the message, if it is smaller than the max length, using the driver.

    Parameters
    ----------
    driver: MesosExecutorDriver
        The driver to send the message to.
    message: object
        The raw message to send.
    max_message_length: int
        The allowed max message length after encoding.

    Returns
    -------
    whether the message was successfully sent
    """
    logging.debug('Sending framework message {}'.format(message))
    message_string = str(message).encode('utf8')
    if len(message_string) < max_message_length:
        encoded_message = encode_data(message_string)
        driver.sendFrameworkMessage(encoded_message)
        return True
    else:
        log_message_template = 'Unable to send message {} as it exceeds allowed max length of {}'
        logging.warning(log_message_template.format(message, max_message_length))
        return False


def launch_task(task):
    """Launches the task using the command available in the json map from the data field.

    Parameters
    ----------
    task: map
        The task to execute.

    Returns
    -------
    When command is provided and a process can be started, the process launched along
    with the two threads that are piping the stdout and stderr.
    Else it logs the reason and returns None.
    """
    try:
        data_string = decode_data(task['data']).decode('utf8')
        data_json = json.loads(data_string)
        command = str(data_json['command']).strip()
        logging.info('Command: {}'.format(command))
        if not command:
            logging.warning('No command provided!')
            return None

        process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout_thread, stderr_thread = cio.track_outputs(process)

        return process, stdout_thread, stderr_thread
    except Exception:
        logging.exception('Error in launch_task')
        return None


def is_running(process_info):
    """Checks whether the process is still running.

    Parameters
    ----------
    process_info: tuple of process, stdout_thread, stderr_thread
        process: subprocess.Popen
            The process to query
        stdout_thread: threading.Thread
            The thread that is piping the subprocess stdout.
        stderr_thread: threading.Thread
            The thread that is piping the subprocess stderr.

    Returns
    -------
    whether any of the process or the piping threads are still running.
    """
    process, stdout_thread, stderr_thread = process_info
    return process.poll() is None or stdout_thread.isAlive() or stderr_thread.isAlive()


def kill_task(process_info, shutdown_grace_period_ms):
    """Attempts to kill a process.
     First attempt is made by sending the process a SIGTERM.
     If the process does not terminate inside (shutdown_grace_period_ms - 100) ms, it is then sent a SIGKILL.
     The 100 ms grace period is allocated for the executor to perform its other cleanup actions.

    Parameters
    ----------
    process_info: tuple of process, stdout_thread, stderr_thread
        process: subprocess.Popen
            The process to query
        stdout_thread: threading.Thread
            The thread that is piping the subprocess stdout.
        stderr_thread: threading.Thread
            The thread that is piping the subprocess stderr.
    shutdown_grace_period_ms: int
        Grace period before forceful kill

    Returns
    -------
    Nothing
    """
    process, _, _ = process_info
    shutdown_grace_period_ms = max(shutdown_grace_period_ms - 100, 0)
    if is_running(process_info):
        logging.info('Waiting up to {} ms for process to terminate'.format(shutdown_grace_period_ms))
        process.terminate()
        loop_limit = int(shutdown_grace_period_ms / 10)
        for i in range(loop_limit):
            time.sleep(0.01)
            if not is_running(process_info):
                message = 'Command terminated with signal Terminated (pid: {})'.format(process.pid)
                cio.print_out(message, flush=True)
                logging.info(message)
                break
        if is_running(process_info):
            logging.info('Process did not terminate, forcefully killing it')
            process.kill()
            message = 'Command terminated with signal Killed (pid: {})'.format(process.pid)
            cio.print_out(message, flush=True)
            logging.info(message)


def await_process_completion(stop_signal, process_info, shutdown_grace_period_ms):
    """Awaits process completion.

    Parameters
    ----------
    stop_signal: Event
        Event that determines if an interrupt was sent
    process_info: tuple of process, stdout_thread, stderr_thread
        process: subprocess.Popen
            The process to query
        stdout_thread: threading.Thread
            The thread that is piping the subprocess stdout.
        stderr_thread: threading.Thread
            The thread that is piping the subprocess stderr.
    shutdown_grace_period_ms: int
        Grace period before forceful kill

    Returns
    -------
    True if the process was killed, False if it terminated naturally.
    """
    while is_running(process_info):

        if stop_signal.isSet():
            logging.info('Executor has been instructed to terminate')
            kill_task(process_info, shutdown_grace_period_ms)
            break

        time.sleep(cook.RUNNING_POLL_INTERVAL_SECS)


def get_task_state(exit_code):
    """Interprets the exit_code and return the corresponding task status string

    Parameters
    ----------
    exit_code: int
        An integer that represents the return code of the task.

    Returns
    -------
    A task status string corresponding to the exit code.
    """
    if exit_code > 0:
        return cook.TASK_FAILED
    elif exit_code < 0:
        return cook.TASK_KILLED
    else:
        return cook.TASK_FINISHED


def manage_task(driver, task, stop_signal, completed_signal, config):
    """Manages the execution of a task waiting for it to terminate normally or be killed.
       It also sends the task status updates, sandbox location and exit code back to the scheduler.
       Progress updates are tracked on a separate thread and are also sent to the scheduler.
       Setting the stop_signal will trigger termination of the task and associated cleanup.

    Returns
    -------
    Nothing
    """
    task_id = get_task_id(task)
    cio.print_out('Starting task {}'.format(task_id))
    try:
        # not yet started to run the task
        update_status(driver, task_id, cook.TASK_STARTING)

        sandbox_message = json.dumps({'sandbox-directory': config.sandbox_directory, 'task-id': task_id})
        send_message(driver, sandbox_message, config.max_message_length)

        process_info = launch_task(task)
        if process_info:
            # task has begun running successfully
            update_status(driver, task_id, cook.TASK_RUNNING)
        else:
            # task launch failed, report an error
            logging.info('Error in launching task')
            update_status(driver, task_id, cook.TASK_ERROR)
            return

        task_completed_signal = Event() # event to track task execution completion

        progress_watcher = cp.ProgressWatcher(config, stop_signal, task_completed_signal)
        progress_updater = cp.ProgressUpdater(driver, task_id, config.max_message_length,
                                              config.progress_sample_interval_ms, send_message)
        progress_complete_event = cp.launch_progress_tracker(progress_watcher, progress_updater)

        await_process_completion(stop_signal, process_info, config.shutdown_grace_period_ms)
        task_completed_signal.set()

        # propagate the exit code
        process, _, _ = process_info
        exit_code = process.returncode
        message = 'Command exited with status {} (pid: {})'.format(exit_code, process.pid)
        cio.print_out(message, flush=True)
        logging.info(message)
        exit_message = json.dumps({'exit-code': exit_code, 'task-id': task_id})
        send_message(driver, exit_message, config.max_message_length)

        # await progress updater termination if executor is terminating normally
        if not stop_signal.isSet():
            logging.info('Awaiting progress updater completion')
            progress_complete_event.wait()
            logging.info('Progress updater completed')

        # force send the latest progress state if available
        cp.force_send_progress_update(progress_watcher, progress_updater)

        # task either completed successfully or aborted with an error
        task_state = get_task_state(exit_code)
        update_status(driver, task_id, task_state)

    except Exception:
        # task aborted with an error
        logging.exception('Error in executing task')
        update_status(driver, task_id, cook.TASK_FAILED)

    finally:
        # ensure completed_signal is set so driver can stop
        completed_signal.set()
        message = 'Executor completed execution of {}'.format(task_id)
        cio.print_out(message, flush=True)
        logging.info(message)


def run_mesos_driver(stop_signal, config):
    """Run an executor driver until the stop_signal event is set or the first task it runs completes."""
    executor = CookExecutor(stop_signal, config)
    driver = MesosExecutorDriver(executor)
    driver.start()

    # check the status of the executor and bail if it has crashed
    while not executor.has_task_completed():
        time.sleep(1)
    else:
        logging.info('Executor thread has completed')

    driver.stop()


class CookExecutor(Executor):
    """This class is responsible for launching the task sent by the scheduler.
    It implements the Executor methods."""

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

        stop_signal = self.stop_signal
        completed_signal = self.completed_signal
        config = self.config
        Thread(target=manage_task, args=(driver, task, stop_signal, completed_signal, config)).start()

    def killTask(self, driver, task_id):
        logging.info('Task {} has been killed by Mesos'.format(task_id))
        self.stop_signal.set()

    def shutdown(self, driver):
        logging.info('Mesos requested executor to shutdown!')
        self.stop_signal.set()

    def has_task_completed(self):
        """
        Returns true when the executor has completed execution of the task specified in the call to launchTask.
        The executor is intended to run a single task.
        
        Returns
        -------
        true when the task has completed execution. 
        """
        return self.completed_signal.isSet()
