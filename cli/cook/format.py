import time

import humanfriendly

from cook import terminal
from cook.util import millis_to_timedelta, millis_to_date_string


def format_dict(d):
    """Formats the given dictionary for display in a table"""
    return ' '.join(['%s=%s' % (k, v) for k, v in sorted(d.items())]) if len(d) > 0 else '(empty)'


def format_list(l):
    """Formats the given list for display in a table"""
    return '; '.join([format_dict(x) if isinstance(x, dict) else str(x) for x in l]) if len(l) > 0 else '(empty)'


def format_state(state):
    """Capitalizes and colorizes the given state"""
    state = state.capitalize()
    if state == 'Running':
        text = terminal.running(state)
    elif state == 'Waiting':
        text = terminal.waiting(state)
    elif state == 'Failed':
        text = terminal.failed(state)
    elif state == 'Success':
        text = terminal.success(state)
    else:
        text = state
    return text


def format_instance_status(instance):
    """Formats the instance status field"""
    status_text = format_state(instance['status'])

    if 'reason_string' in instance:
        reason_text = f' ({terminal.reason(instance["reason_string"])})'
    else:
        reason_text = ''

    if 'progress' in instance and instance['progress'] > 0:
        if 'progress_message' in instance:
            progress_text = f' ({instance["progress"]}% {terminal.bold(instance["progress_message"])})'
        else:
            progress_text = f' ({instance["progress"]}%)'
    else:
        progress_text = ''

    return f'{status_text}{reason_text}{progress_text}'


def format_instance_run_time(instance):
    """Formats the instance run time field"""
    if 'end_time' in instance:
        end = instance['end_time']
    else:
        end = int(round(time.time() * 1000))
    run_time = millis_to_timedelta(end - instance['start_time'])
    return '%s (started %s)' % (run_time, millis_to_date_string(instance['start_time']))


def format_job_status(job):
    """Formats the job status field"""
    return format_state(job['state'])

def format_memory_amount(megabytes):
    """Formats an amount, in MB, to be human-readable"""
    return humanfriendly.format_size(megabytes * 1000 * 1000)

def format_job_memory(job):
    """Formats the job memory field"""
    return format_memory_amount(job['mem'])


def format_job_attempts(job):
    """Formats the job attempts field (e.g. 2 / 5)"""
    return '%s / %s' % (job['max_retries'] - job['retries_remaining'], job['max_retries'])
