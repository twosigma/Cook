import json
import logging
import os
import shlex
import subprocess
import tempfile

from tests.cook import util

logger = logging.getLogger(__name__)


def decode(b):
    """Decodes as UTF-8"""
    return b.decode('UTF-8')


def encode(o):
    """Encodes with UTF-8"""
    return str(o).encode('UTF-8')


def stdout(cp):
    """Returns the UTF-8 decoded and stripped stdout of the given CompletedProcess"""
    return decode(cp.stdout).strip()


def sh(command, stdin=None):
    """Runs command using subprocess.run"""
    logger.info(command + ((' # stdin: %s' % decode(stdin)) if stdin else ''))
    cp = subprocess.run(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, input=stdin)
    return cp


def cli(args, cook_url=None, flags=None, stdin=None):
    """Runs a CLI command with the given URL, flags, and stdin"""
    url_flag = ('--url %s ' % cook_url) if cook_url else ''
    other_flags = ('%s ' % flags) if flags else ''
    cp = sh('cs %s%s%s' % (url_flag, other_flags, args), stdin)
    return cp


def submit(command=None, cook_url=None, flags=None, submit_flags=None, stdin=None):
    """Submits one job via the CLI"""
    args = 'submit %s%s' % (submit_flags + ' ' if submit_flags else '', command if command else '')
    cp = cli(args, cook_url, flags, stdin)
    uuids = [s for s in stdout(cp).split() if len(s) == 36 and util.is_valid_uuid(s)]
    return cp, uuids


def submit_stdin(commands, cook_url, flags=None, submit_flags=None):
    """Submits one or more jobs via the CLI using stdin"""
    cp, uuids = submit(cook_url=cook_url, flags=flags, submit_flags=submit_flags, stdin=encode('\n'.join(commands)))
    return cp, uuids


def show_or_wait(action, uuids=None, cook_url=None, flags=None, action_flags=None):
    """Helper function used to either show or wait via the CLI"""
    action_flags = (action_flags + ' ') if action_flags else ''
    uuids = ' '.join([str(uuid) for uuid in uuids])
    cp = cli('%s %s%s' % (action, action_flags, uuids), cook_url, flags)
    return cp


def show(uuids=None, cook_url=None, flags=None, show_flags=None):
    """Shows the job(s) corresponding to the given UUID(s) via the CLI"""
    cp = show_or_wait('show', uuids, cook_url, flags, show_flags)
    return cp


def show_json(uuids, cook_url=None, flags=None):
    """Shows the job JSON corresponding to the given UUID(s)"""
    flags = (flags + ' ') if flags else ''
    cp = show(uuids, cook_url, '%s--silent' % flags, '--json')
    response = json.loads(stdout(cp))
    jobs = [job for entities in response['clusters'].values() for job in entities['jobs']]
    return cp, jobs


def wait(uuids=None, cook_url=None, flags=None, wait_flags=None):
    """Waits for the jobs corresponding to the given UUID(s) to complete"""
    cp = show_or_wait('wait', uuids, cook_url, flags, wait_flags)
    return cp


class temp_config_file:
    """
    A context manager used to generate and subsequently delete a temporary 
    config file for the CLI. Takes as input the config dictionary to use.
    """

    def __init__(self, config):
        self.config = config

    def write_temp_json(self):
        path = tempfile.NamedTemporaryFile(delete=False).name
        with open(path, 'w') as outfile:
            logger.info('echo \'%s\' > %s' % (json.dumps(self.config), path))
            json.dump(self.config, outfile)
        return path

    def __enter__(self):
        self.path = self.write_temp_json()
        return self.path

    def __exit__(self, _, __, ___):
        os.remove(self.path)


def list_jobs(cook_url=None, list_flags=None):
    """Invokes the list subcommand"""
    args = 'list %s' % list_flags + ' ' if list_flags else ''
    cp = cli(args, cook_url)
    return cp


def list_jobs_json(cook_url=None, list_flags=None):
    """Invokes the list subcommand with --json"""
    cp = list_jobs(cook_url, '%s--json' % (list_flags + ' ' if list_flags else ''))
    response = json.loads(stdout(cp))
    jobs = [job for entities in response['clusters'].values() for job in entities['jobs']]
    return cp, jobs
