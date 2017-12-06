import json
import logging
import signal
import time
from threading import Event, Thread, Timer

import os
import pymesos as pm

import cook
import cook.io_helper as cio
import cook.progress as cp
import cook.subprocess as cs


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


def create_status(task_id, task_state, reason=None):
    """Creates a dictionary representing the task status.

    Parameters
    ----------
    task_id: string
        The task id.
    task_state: string
        The state of the task to report.
    reason: string
        The reason for the task state.

    Returns
    -------
    a status dictionary that can be sent to the driver.
    """
    task_status = {'task_id': {'value': task_id},
                   'state': task_state,
                   'timestamp': time.time()}
    if reason:
        task_status['reason'] = reason
    return task_status


def update_status(driver, task_id, task_state, reason=None):
    """Sends the status using the driver. 

    Parameters
    ----------
    driver: MesosExecutorDriver
        The driver to send the status update to.
    task_id: string
        The task id of the task whose status update to send.
    task_state: string
        The state of the task which will be sent to the driver.
    reason: string
        The reason for the task state.

    Returns
    -------
    Nothing.
    """
    logging.info('Updating task {} state to {}'.format(task_id, task_state))
    status = create_status(task_id, task_state, reason=reason)
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
    if len(message_string) <= max_message_length:
        encoded_message = pm.encode_data(message_string)
        driver.sendFrameworkMessage(encoded_message)
        return True
    else:
        log_message_template = 'Unable to send message as its length of {} exceeds allowed max length of {}'
        logging.warning(log_message_template.format(len(message_string), max_message_length))
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
        return cs.launch_process(command, environment)
    except Exception:
        logging.exception('Error in launch_task')
        return None


def await_process_completion(process, stop_signal, shutdown_grace_period_ms):
    """Awaits process completion. Also sets up the thread that will kill the process if stop_signal is set.

    Parameters
    ----------
    process: subprocess.Popen
        The process to whose termination to wait on.
    stop_signal: Event
        Event that determines if an interrupt was sent
    shutdown_grace_period_ms: int
        Grace period before forceful kill

    Returns
    -------
    True if the process was killed, False if it terminated naturally.
    """
    def process_stop_signal():
        stop_signal.wait() # wait indefinitely for the stop_signal to be set
        if cs.is_process_running(process):
            logging.info('Executor has been instructed to terminate running task')
            cs.kill_process(process, shutdown_grace_period_ms)

    kill_thread = Thread(target=process_stop_signal, args=())
    kill_thread.daemon = True
    kill_thread.start()

    # wait indefinitely for process to terminate (either normally or by being killed)
    process.wait()


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


def output_task_completion(task_id, task_state):
    """Prints and logs the executor completion message."""
    cio.print_and_log('Executor completed execution of {} (state={})'.format(task_id, task_state))


def manage_task(driver, task, stop_signal, completed_signal, config):
    """Manages the execution of a task waiting for it to terminate normally or be killed.
       It also sends the task status updates, sandbox location and exit code back to the scheduler.
       Progress updates are tracked on a separate thread and are also sent to the scheduler.
       Setting the stop_signal will trigger termination of the task and associated cleanup.

    Returns
    -------
    Nothing
    """
    launched_process = None
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
        launched_process = launch_task(task, environment)
        if launched_process:
            # task has begun running successfully
            update_status(driver, task_id, cook.TASK_RUNNING)
            cio.print_and_log('Forked command at {}'.format(launched_process.pid))
        else:
            # task launch failed, report an error
            logging.error('Error in launching task')
            update_status(driver, task_id, cook.TASK_ERROR, reason=cook.REASON_TASK_INVALID)
            return

        task_completed_signal = Event() # event to track task execution completion
        sequence_counter = cp.ProgressSequenceCounter()

        def send_progress_message(message):
            return send_message(driver, message, config.max_message_length)

        progress_termination_signal = Event()
        progress_updater = cp.ProgressUpdater(task_id, config.max_message_length, config.progress_sample_interval_ms,
                                              send_progress_message)

        def launch_progress_tracker(progress_location, location_tag):
            logging.info('Location {} tagged as [tag={}]'.format(progress_location, location_tag))
            progress_tracker = cp.ProgressTracker(config, stop_signal, task_completed_signal, sequence_counter,
                                                  progress_updater, progress_termination_signal, progress_location,
                                                  location_tag)
            progress_tracker.start()
            return progress_tracker

        progress_locations = {config.progress_output_name: 'progress',
                              config.stderr_file(): 'stderr',
                              config.stdout_file(): 'stdout'}
        logging.info('Progress will be tracked from {} locations'.format(len(progress_locations)))
        progress_trackers = [launch_progress_tracker(l, progress_locations[l]) for l in progress_locations]

        await_process_completion(launched_process, stop_signal, config.shutdown_grace_period_ms)
        task_completed_signal.set()

        progress_termination_timer = Timer(config.shutdown_grace_period_ms / 1000.0, progress_termination_signal.set)
        progress_termination_timer.daemon = True
        progress_termination_timer.start()

        # propagate the exit code
        exit_code = launched_process.returncode
        cio.print_and_log('Command exited with status {} (pid: {})'.format(exit_code, launched_process.pid))

        exit_message = json.dumps({'exit-code': exit_code, 'task-id': task_id})
        send_message(driver, exit_message, config.max_message_length)

        # await progress updater termination if executor is terminating normally
        if not stop_signal.isSet():
            logging.info('Awaiting completion of progress updaters')
            [progress_tracker.wait() for progress_tracker in progress_trackers]
            logging.info('Progress updaters completed')

        # force send the latest progress state if available
        [progress_tracker.force_send_progress_update() for progress_tracker in progress_trackers]

        # task either completed successfully or aborted with an error
        task_state = get_task_state(exit_code)
        output_task_completion(task_id, task_state)
        update_status(driver, task_id, task_state)

    except Exception:
        # task aborted with an error
        logging.exception('Error in executing task')
        output_task_completion(task_id, cook.TASK_FAILED)
        update_status(driver, task_id, cook.TASK_FAILED, reason=cook.REASON_EXECUTOR_TERMINATED)

    finally:
        # ensure completed_signal is set so driver can stop
        completed_signal.set()
        if launched_process and cs.is_process_running(launched_process):
            cs.send_signal(launched_process.pid, signal.SIGKILL)


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
        self.stop_signal.set()

    def launchTask(self, driver, task):
        logging.info('Driver {} launching task {}'.format(driver, task))

        stop_signal = self.stop_signal
        completed_signal = self.completed_signal
        config = self.config

        task_thread = Thread(target=manage_task, args=(driver, task, stop_signal, completed_signal, config))
        task_thread.daemon = True
        task_thread.start()

    def killTask(self, driver, task_id):
        logging.info('Mesos requested executor to kill task {}'.format(task_id))
        task_id_str = task_id['value'] if 'value' in task_id else task_id
        grace_period = os.environ.get('MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD', '')
        cio.print_and_log('Received kill for task {} with grace period of {}'.format(task_id_str, grace_period))
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
        disconnect_grace_secs = cook.TERMINATE_GRACE_SECS if self.stop_signal.isSet() else cook.DAEMON_GRACE_SECS
        if not self.disconnect_signal.isSet():
            logging.info('Waiting up to {} second(s) for CookExecutor to disconnect'.format(disconnect_grace_secs))
            self.disconnect_signal.wait(disconnect_grace_secs)
        if not self.disconnect_signal.isSet():
            logging.info('CookExecutor did not disconnect in {} seconds'.format(disconnect_grace_secs))
