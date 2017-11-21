#!/usr/bin/env python3

import json
import logging
import os
import signal
import subprocess
import time
from threading import Event, Thread

import cook
import cook.io_helper as cio
import cook.progress as cp
import pymesos as pm


def get_task_id(task):
    """Retrieves the id of the task.

    Parameters
    ----------
    task: dictionary
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
    task: dictionary
        The task whose status update to send.
    task_state: string
        The state of the task which will be sent to the driver.

    Returns
    -------
    Nothing.
    """
    logging.info('Updating task {} state to {}'.format(task_id, task_state))
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
    logging.info('Sending framework message {}'.format(message))
    message_string = str(message).encode('utf8')
    if len(message_string) < max_message_length:
        encoded_message = pm.encode_data(message_string)
        driver.sendFrameworkMessage(encoded_message)
        return True
    else:
        log_message_template = 'Unable to send message {} as it exceeds allowed max length of {}'
        logging.warning(log_message_template.format(message, max_message_length))
        return False


def launch_task(task, environment):
    """Launches the task using the command available in the json map from the data field.

    Parameters
    ----------
    task: dictionary
        The task to execute.
    environment: dictionary
        The task environment.

    Returns
    -------
    When command is provided and a process can be started, the process launched.
    Else it logs the reason and returns None.
    """
    try:
        data_string = pm.decode_data(task['data']).decode('utf8')
        data_json = json.loads(data_string)
        command = str(data_json['command']).strip()
        logging.info('Command: {}'.format(command))
        if not command:
            logging.warning('No command provided!')
            return None

        # The preexec_fn is run after the fork() but before exec() to run the shell.
        process = subprocess.Popen(command,
                                   env=environment,
                                   preexec_fn=os.setpgrp,
                                   shell=True,
                                   stderr=subprocess.PIPE,
                                   stdout=subprocess.PIPE)

        return process
    except Exception:
        logging.exception('Error in launch_task')
        return None


def is_process_running(process):
    """Checks whether the process is still running.

    Parameters
    ----------
    process: subprocess.Popen
        The process to query

    Returns
    -------
    whether the process is still running.
    """
    return process.poll() is None


def is_running(process_info):
    """Checks whether any of the process or the piping threads are still running.

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
    return is_process_running(process) or stdout_thread.isAlive() or stderr_thread.isAlive()


def kill_task(process, shutdown_grace_period_ms):
    """Attempts to kill a process.
     First attempt is made by sending the process a SIGTERM.
     If the process does not terminate inside (shutdown_grace_period_ms - 100) ms, it is then sent a SIGKILL.
     The 100 ms grace period is allocated for the executor to perform its other cleanup actions.

    Parameters
    ----------
    process: subprocess.Popen
        The process to kill
    shutdown_grace_period_ms: int
        Grace period before forceful kill

    Returns
    -------
    Nothing
    """
    shutdown_grace_period_ms = max(shutdown_grace_period_ms - (1000 * cook.TERMINATE_GRACE_SECS), 0)
    if is_process_running(process):
        logging.info('Waiting up to {} ms for process to terminate'.format(shutdown_grace_period_ms))
        process.terminate()
        loop_limit = int(shutdown_grace_period_ms / 10)
        for i in range(loop_limit):
            time.sleep(0.01)
            if not is_process_running(process):
                cio.print_and_log('Command terminated with signal Terminated (pid: {})'.format(process.pid),
                                  flush=True)
                break
        if is_process_running(process):
            logging.info('Process did not terminate, forcefully killing it')
            process.kill()
            cio.print_and_log('Command terminated with signal Killed (pid: {})'.format(process.pid),
                              flush=True)


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
            process, _, _ = process_info
            kill_task(process, shutdown_grace_period_ms)
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


def set_environment(environment, key, value):
    """Updates an entry in the environment dictionary.

    Returns
    -------
    Nothing.
    """
    if key not in environment or environment[key] != value:
        logging.info('Setting process environment[{}]={}'.format(key, value))
        environment[key] = value


def retrieve_process_environment(config, os_environ):
    """Prepares the environment for the subprocess.
    The function also ensures that env[config.progress_output_env_variable] is set to config.progress_output_name.
    This protects against the scenario where the config.progress_output_env_variable was specified
    in the environment, but the progress output file was not specified.

    Parameters
    ----------
    config: cook.config.ExecutorConfig
        The current executor config.
    os_environ: dictionary
        A dictionary representing the current environment.

    Returns
    -------
    The environment dictionary for the subprocess.
    """
    environment = dict(os_environ)
    set_environment(environment, config.progress_output_env_variable, config.progress_output_name)
    return environment


def find_process_group(process_id):
    """Return the process group id of the process with process id process_id.
    :param process_id: int
        The process id.
    Returns
    -------
    The process group id of the process with process id process_id or None.
    """
    try:
        group_id = os.getpgid(process_id)
        logging.info('Process (pid: {}) belongs to group (id: {})'.format(process_id, group_id))
        return group_id
    except ProcessLookupError:
        logging.info('Unable to find group for process (pid: {})'.format(process_id))
    except Exception:
        logging.exception('Error in finding group for process (pid: {})'.format(process_id))


def kill_process_group(group_id):
    """Send the SIGKILL signal to the process group with group_id.
    :param group_id: int
        The group id.
    Returns
    -------
    Nothing
    """
    try:
        if group_id:
            logging.info('Sending kill signal to group (id: {})'.format(group_id))
            os.killpg(group_id, signal.SIGKILL)
    except ProcessLookupError:
        logging.info('Unable to find group (id: {}) to send kill signal'.format(group_id))
    except Exception:
        logging.exception('Error in sending kill signal to group (id: {})'.format(group_id))


def output_task_completion(task_id, task_state):
    """Prints and logs the executor completion message."""
    cio.print_and_log('Executor completed execution of {} (state={})'.format(task_id, task_state), flush=True)


def manage_task(driver, task, stop_signal, completed_signal, config):
    """Manages the execution of a task waiting for it to terminate normally or be killed.
       It also sends the task status updates, sandbox location and exit code back to the scheduler.
       Progress updates are tracked on a separate thread and are also sent to the scheduler.
       Setting the stop_signal will trigger termination of the task and associated cleanup.

    Returns
    -------
    Nothing
    """
    group_id = None
    task_id = get_task_id(task)
    cio.print_and_log('Starting task {}'.format(task_id))
    try:
        # not yet started to run the task
        update_status(driver, task_id, cook.TASK_STARTING)

        sandbox_message = json.dumps({'sandbox-directory': config.sandbox_directory,
                                      'task-id': task_id,
                                      'type': 'directory'})
        send_message(driver, sandbox_message, config.max_message_length)

        environment = retrieve_process_environment(config, os.environ)
        process = launch_task(task, environment)
        if process:
            # task has begun running successfully
            update_status(driver, task_id, cook.TASK_RUNNING)
            cio.print_and_log('Forked command at {}'.format(process.pid))
        else:
            # task launch failed, report an error
            logging.error('Error in launching task')
            update_status(driver, task_id, cook.TASK_ERROR)
            return

        group_id = find_process_group(process.pid)

        stdout_thread, stderr_thread = cio.track_outputs(task_id, process, config.max_bytes_read_per_line)
        task_completed_signal = Event() # event to track task execution completion

        progress_watcher = cp.ProgressWatcher(config, stop_signal, task_completed_signal)
        progress_updater = cp.ProgressUpdater(driver, task_id, config.max_message_length,
                                              config.progress_sample_interval_ms, send_message)
        progress_complete_event = cp.launch_progress_tracker(progress_watcher, progress_updater)

        process_info = process, stdout_thread, stderr_thread
        await_process_completion(stop_signal, process_info, config.shutdown_grace_period_ms)
        task_completed_signal.set()

        # propagate the exit code
        exit_code = process.returncode
        cio.print_and_log('Command exited with status {} (pid: {})'.format(exit_code, process.pid),
                          flush=True)

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
        output_task_completion(task_id, task_state)
        update_status(driver, task_id, task_state)

    except Exception:
        # task aborted with an error
        logging.exception('Error in executing task')
        output_task_completion(task_id, cook.TASK_FAILED)
        update_status(driver, task_id, cook.TASK_FAILED)

    finally:
        # ensure completed_signal is set so driver can stop
        completed_signal.set()
        kill_process_group(group_id)


class CookExecutor(pm.Executor):
    """This class is responsible for launching the task sent by the scheduler.
    It implements the Executor methods."""

    def __init__(self, stop_signal, config):
        self.completed_signal = Event()
        self.config = config
        self.disconnect_signal = Event()
        self.stop_signal = stop_signal

    def registered(self, driver, executor_info, framework_info, agent_info):
        logging.info('Executor registered executor={}, framework={}, agent={}'.
                     format(executor_info['executor_id']['value'], framework_info['id'], agent_info['id']['value']))

    def reregistered(self, driver, agent_info):
        logging.info('Executor re-registered agent={}'.format(agent_info))

    def disconnected(self, driver):
        logging.info('Mesos requested executor to disconnect')
        self.disconnect_signal.set()

    def launchTask(self, driver, task):
        logging.info('Driver {} launching task {}'.format(driver, task))

        stop_signal = self.stop_signal
        completed_signal = self.completed_signal
        config = self.config
        Thread(target=manage_task, args=(driver, task, stop_signal, completed_signal, config)).start()

    def killTask(self, driver, task_id):
        logging.info('Mesos requested executor to kill task {}'.format(task_id))
        self.stop_signal.set()

    def shutdown(self, driver):
        logging.info('Mesos requested executor to shutdown')
        self.stop_signal.set()

    def error(self, driver, message):
        logging.error(message)
        super().error(driver, message)

    def await_completion(self):
        """
        Blocks until the internal flag completed_signal is set.
        The completed_signal Event is expected to be set by manage_task.
        """
        logging.info('Waiting for CookExecutor to complete...')
        self.completed_signal.wait()
        logging.info('CookExecutor has completed')

    def await_disconnect(self):
        """
        Blocks until the internal flag disconnect_signal is set or the disconnect grace period expires.
        The disconnect grace period is computed based on whether stop_signal is set.
        """
        disconnect_grace_secs = cook.DAEMON_GRACE_SECS if not self.stop_signal.isSet() else cook.TERMINATE_GRACE_SECS
        if not self.disconnect_signal.isSet():
            logging.info('Waiting up to {} second(s) for CookExecutor to disconnect'.format(disconnect_grace_secs))
            self.disconnect_signal.wait(disconnect_grace_secs)
        if not self.disconnect_signal.isSet():
            logging.info('CookExecutor did not disconnect in {} seconds'.format(disconnect_grace_secs))
