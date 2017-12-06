import logging
import signal
import subprocess
import time

import os
import psutil

import cook
import cook.io_helper as cio


def launch_process(command, environment):
    """Launches the process using the command and specified environment.

    Parameters
    ----------
    command: string
        The command to execute.
    environment: dictionary
        The environment.

    Returns
    -------
    The launched process.
    """
    if not command:
        logging.warning('No command provided!')
        return None
    # The preexec_fn is run after the fork() but before exec() to run the shell.
    # setsid will run the program in a new session, thus assigning a new process group to it and its children.
    return subprocess.Popen(command,
                            env=environment,
                            preexec_fn=os.setsid,
                            shell=True,
                            stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE)


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


def find_process_group(process_id):
    """Return the process group id of the process with process id process_id.
    Parameters
    ----------
    process_id: int
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


def _send_signal_to_process(process_id, signal_to_send):
    """Send the signal_to_send signal to the process with process_id.
    Parameters
    ----------
    process_id: int
        The id of the process whose group to kill.
    signal_to_send: signal.Signals enum
        The signal to send to the process group.
    Returns
    -------
    True if the signal was sent successfully.
    """
    signal_name = signal_to_send.name
    try:
        logging.info('Sending signal {} to process (id: {})'.format(signal_name, process_id))
        os.kill(process_id, signal_to_send)
        return True
    except ProcessLookupError:
        logging.info('Unable to send signal {} as could not find process (id: {})'.format(signal_name, process_id))
    except Exception:
        logging.exception('Error in sending signal {} to process (id: {})'.format(signal_name, process_id))
    return False


def _send_signal_to_process_tree(process_id, signal_to_send):
    """Send the signal_to_send signal to the process tree rooted at process_id.
    Parameters
    ----------
    process_id: int
        The id of the root process.
    signal_to_send: signal.Signals enum
        The signal to send to the process group.
    Returns
    -------
    True if the signal was sent successfully.
    """
    signal_name = signal_to_send.name
    try:
        logging.info('Sending signal {} to process tree rooted at (id: {})'.format(signal_name, process_id))
        process = psutil.Process(process_id)
        children = process.children(recursive=True)

        _send_signal_to_process(process_id, signal_to_send)

        signal_sent_to_all_children_successfully = True
        if children:
            for child in children:
                logging.info('Found child process (id: {}) of process (id: {})'.format(child.pid, process_id))
                if not _send_signal_to_process(child.pid, signal_to_send):
                    logging.info('Failed to send signal {} to child process (id: {})'.format(signal_name, child.pid))
                    signal_sent_to_all_children_successfully = False
        else:
            logging.info('No child process found for process (id: {})'.format(process_id))

        return signal_sent_to_all_children_successfully
    except ProcessLookupError:
        log_message = 'Unable to send signal {} as could not find process tree rooted at (id: {})'
        logging.info(log_message.format(signal_name, process_id))
    except Exception:
        logging.exception('Error in sending signal {} to process tree (id: {})'.format(signal_name, process_id))
    return False


def _send_signal_to_process_group(process_id, signal_to_send):
    """Send the signal_to_send signal to the process group with group_id.
    Parameters
    ----------
    process_id: int
        The id of the process whose group to kill.
    signal_to_send: signal.Signals enum
        The signal to send to the process group.
    Returns
    -------
    True if the signal was sent successfully.
    """
    signal_name = signal_to_send.name
    try:
        group_id = find_process_group(process_id)
        if group_id:
            logging.info('Sending signal {} to group (id: {})'.format(signal_name, group_id))
            os.killpg(group_id, signal_to_send)
            return True
    except ProcessLookupError:
        logging.info('Unable to send signal {} as could not find group (id: {})'.format(signal_name, group_id))
    except Exception:
        logging.exception('Error in sending signal {} to group (id: {})'.format(signal_name, group_id))
    return False


def send_signal(process_id, signal_to_send):
    """Send the signal_to_send signal to the process with process_id.
    The function uses a three-step mechanism:
    1. It sends the signal to the process tree rooted at process_id;
    2. If unsuccessful, it sends the signal to the process group of process_id;
    3. If unsuccessful, it sends the signal directly to the process with id process_id."""
    if process_id:
        signal_name = signal_to_send.name
        logging.info('Requested to send {} to process (id: {})'.format(signal_name, process_id))
        if _send_signal_to_process_tree(process_id, signal_to_send):
            logging.info('Successfully sent {} to process tree (id: {})'.format(signal_name, process_id))
        elif _send_signal_to_process_group(process_id, signal_to_send):
            logging.info('Successfully sent {} to group for process (id: {})'.format(signal_name, process_id))
        elif _send_signal_to_process(process_id, signal_to_send):
            logging.info('Successfully sent {} to process (id: {})'.format(signal_name, process_id))
        else:
            logging.info('Failed to send {} to process (id: {})'.format(signal_name, process_id))


def kill_process(process, shutdown_grace_period_ms):
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
        send_signal(process.pid, signal.SIGTERM)

        loop_limit = int(shutdown_grace_period_ms / 10)
        for i in range(loop_limit):
            time.sleep(0.01)
            if not is_process_running(process):
                cio.print_and_log('Command terminated with signal Terminated (pid: {})'.format(process.pid),
                                  flush=True)
                break
        if is_process_running(process):
            logging.info('Process did not terminate, forcefully killing it')
            send_signal(process.pid, signal.SIGKILL)
            cio.print_and_log('Command terminated with signal Killed (pid: {})'.format(process.pid),
                              flush=True)
