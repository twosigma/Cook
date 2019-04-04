import functools
import json
import logging
import signal
import time
from threading import Event, Lock, Thread, Timer

import os
import pymesos as pm

import cook
import cook.io_helper as cio
import cook.progress as cp
import cook.subprocess as cs
import cook.util as cu


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


class StatusUpdater(object):
    """Sends status updates for the task."""
    def __init__(self, driver, task_id):
        """
        Parameters
        ----------
        driver: MesosExecutorDriver
            The driver to send the status update to.
        task_id: dictionary
            The task whose status update to send.
        """
        self.driver = driver
        self.lock = Lock()
        self.task_id = task_id
        self.terminal_states = {cook.TASK_ERROR, cook.TASK_FAILED, cook.TASK_FINISHED, cook.TASK_KILLED}
        self.terminal_status_sent = False

    def create_status(self, task_state, reason=None):
        """Creates a dictionary representing the task status.

        Parameters
        ----------
        task_state: string
            The state of the task to report.
        reason: string
            The reason for the task state.

        Returns
        -------
        a status dictionary that can be sent to the driver.
        """
        task_status = {'state': task_state,
                       'task_id': {'value': self.task_id},
                       'timestamp': time.time()}
        if reason:
            task_status['reason'] = reason
        return task_status

    def update_status(self, task_state, reason=None):
        """Sends the status using the driver.

        Parameters
        ----------
        task_state: string
            The state of the task which will be sent to the driver.
        reason: string
            The reason for the task state.

        Returns
        -------
        True if successfully sent the status update, else False.
        """
        with self.lock:
            is_terminal_status = task_state in self.terminal_states
            if is_terminal_status and self.terminal_status_sent:
                logging.info('Terminal state for task already sent, dropping state {}'.format(task_state))
                return False
            try:
                logging.info('Updating task state to {}'.format(task_state))
                status = self.create_status(task_state, reason=reason)
                self.driver.sendStatusUpdate(status)
                self.terminal_status_sent = is_terminal_status
                return True
            except Exception:
                logging.exception('Unable to send task state {}'.format(task_state))
                return False


def send_message(driver, error_handler, message):
    """Sends the message, if it is smaller than the max length, using the driver.

    Note: This function must rethrow any OSError exceptions that it encounters.

    Parameters
    ----------
    driver: MesosExecutorDriver
        The driver to send the message to.
    error_handler: fn(os_error)
        OSError exception handler for out of memory situations.
    message: dictionary
        The raw message to send.

    Returns
    -------
    whether the message was successfully sent
    """
    try:
        logging.info('Sending framework message {}'.format(message))
        message_string = json.dumps(message).encode('utf8')
        encoded_message = pm.encode_data(message_string)
        driver.sendFrameworkMessage(encoded_message)
        return True
    except Exception as exception:
        if cu.is_out_of_memory_error(exception):
            error_handler(exception)
        else:
            logging.exception('Exception while sending message {}'.format(message))
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
        Event that determines if the process was requested to terminate
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

def await_reregister(reregister_signal, recovery_secs, *disconnect_signals):
    """Awaits reregistration on rerigster_signal, and notifies on stop_signal and disconnect_signal if not set.

    Parameters
    ----------
    reregister_signal: Event
        Event that notifies on mesos agent reregistration
    recovery_secs: int
        Number of seconds to wait for reregistration.
    disconnect_signals: [Event]
        Events to notify if reregistration does not occur
    """
    def await_reregister_thread():
        reregister_signal.wait(recovery_secs)
        if reregister_signal.isSet():
            logging.info("Reregistered with mesos agent. Not notifying on disconnect_signals")
        else:
            logging.warn("Failed to reregister within {} seconds. Notifying disconnect_signals".format(recovery_secs))
            for signal in disconnect_signals:
                signal.set()
    await_thread = Thread(target=await_reregister_thread, args=())
    await_thread.daemon = True
    await_thread.start()


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


def retrieve_process_environment(config, task, os_environ):
    """Prepares the environment for the subprocess.
    The function also ensures that env[config.progress_output_env_variable] is set to config.progress_output_name.
    This protects against the scenario where the config.progress_output_env_variable was specified
    in the environment, but the progress output file was not specified.

    Parameters
    ----------
    config: cook.config.ExecutorConfig
        The current executor config.
    task: dictionary
        The mesos task object
    os_environ: dictionary
        A dictionary representing the current environment.

    Returns
    -------
    The environment dictionary for the subprocess.
    """
    environment = dict(os_environ)
    task_env = {}
    mesos_task_variables = (task.get('executor', {})
                            .get('command', {})
                            .get('environment', {})
                            .get('variables', []))
    for variable in mesos_task_variables:
        task_env[variable['name']] = variable['value']
    for var in config.reset_vars:
        if var in task_env:
            environment[var] = task_env[var]
        elif var in environment:
            del environment[var]
    set_environment(environment, config.progress_output_env_variable, config.progress_output_name)
    return environment


def output_task_completion(task_id, task_state):
    """Prints and logs the executor completion message."""
    cio.print_and_log('Executor completed execution of {} (state={})'.format(task_id, task_state))


def os_error_handler(stop_signal, status_updater, os_error):
    """Exception handler for OSError.

    Parameters
    ----------
    stop_signal: threading.Event
        Event that determines if the process was requested to terminate.
    status_updater: StatusUpdater
        Wrapper object that sends task status messages.
    os_error: OSError
        The current executor config.

    Returns
    -------
    Nothing
    """
    stop_signal.set()
    logging.exception('OSError generated, requesting process to terminate')
    reason = cook.REASON_CONTAINER_LIMITATION_MEMORY if cu.is_out_of_memory_error(os_error) else None
    status_updater.update_status(cook.TASK_FAILED, reason=reason)
    cu.print_memory_usage()


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
    status_updater = StatusUpdater(driver, task_id)

    inner_os_error_handler = functools.partial(os_error_handler, stop_signal, status_updater)
    try:
        # not yet started to run the task
        status_updater.update_status(cook.TASK_STARTING)

        # Use MESOS_DIRECTORY instead of MESOS_SANDBOX, to report the sandbox location outside of the container
        sandbox_message = {'sandbox-directory': config.mesos_directory, 'task-id': task_id, 'type': 'directory'}
        send_message(driver, inner_os_error_handler, sandbox_message)

        environment = retrieve_process_environment(config, task, os.environ)
        launched_process = launch_task(task, environment)
        if launched_process:
            # task has begun running successfully
            status_updater.update_status(cook.TASK_RUNNING)
            cio.print_and_log('Forked command at {}'.format(launched_process.pid))
        else:
            # task launch failed, report an error
            logging.error('Error in launching task')
            status_updater.update_status(cook.TASK_ERROR, reason=cook.REASON_TASK_INVALID)
            return

        task_completed_signal = Event() # event to track task execution completion
        sequence_counter = cp.ProgressSequenceCounter()

        send_progress_message = functools.partial(send_message, driver, inner_os_error_handler)
        max_message_length = config.max_message_length
        sample_interval_ms = config.progress_sample_interval_ms
        progress_updater = cp.ProgressUpdater(task_id, max_message_length, sample_interval_ms, send_progress_message)
        progress_termination_signal = Event()

        def launch_progress_tracker(progress_location, location_tag):
            logging.info('Location {} tagged as [tag={}]'.format(progress_location, location_tag))
            progress_tracker = cp.ProgressTracker(config, stop_signal, task_completed_signal, sequence_counter,
                                                  progress_updater, progress_termination_signal, progress_location,
                                                  location_tag, inner_os_error_handler)
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

        exit_message = {'exit-code': exit_code, 'task-id': task_id}
        send_message(driver, inner_os_error_handler, exit_message)

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
        status_updater.update_status(task_state)

    except Exception as exception:
        if cu.is_out_of_memory_error(exception):
            inner_os_error_handler(exception)
        else:
            # task aborted with an error
            logging.exception('Error in executing task')
            output_task_completion(task_id, cook.TASK_FAILED)
            status_updater.update_status(cook.TASK_FAILED, reason=cook.REASON_EXECUTOR_TERMINATED)

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
        self.reregister_signal = None

    def registered(self, driver, executor_info, framework_info, agent_info):
        logging.info('Executor registered executor={}, framework={}, agent={}'.
                     format(executor_info['executor_id']['value'], framework_info['id'], agent_info['id']['value']))

        env = os.environ
        if 'EXECUTOR_TEST_EXIT' in env:
            exit_code = int(env['EXECUTOR_TEST_EXIT'])
            logging.warn('Exiting with code {} from EXECUTOR_TEST_EXIT environment variable'.
                         format(exit_code))
            os._exit(exit_code)

    def reregistered(self, driver, agent_info):
        logging.info('Executor re-registered agent={}'.format(agent_info))
        if self.config.checkpoint:
            if self.reregister_signal is not None:
                logging.info('Executor checkpointing is enabled. Notifying on reregister_signal')
                self.reregister_signal.set()
                self.reregister_signal = None
            else:
                logging.error('Checkpointing is enabled but reregister_signal is None. Unable to notify!')

    def disconnected(self, driver):
        logging.info('Mesos requested executor to disconnect')
        if self.config.checkpoint:
            if self.reregister_signal is None:
                logging.info('Executor checkpointing is enabled. Waiting for agent recovery.')
                new_event = Event()
                self.reregister_signal = new_event
                await_reregister(new_event, self.config.recovery_timeout_ms / 1000, self.stop_signal, self.disconnect_signal)
            else:
                logging.info('Checkpointing is enabled. Already launched await_reregister thread.')
        else:
            logging.info('Executor checkpointing is not enabled. Terminating task.')
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
