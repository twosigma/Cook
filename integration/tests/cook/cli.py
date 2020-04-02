import json
import logging
import os
import pty
import re
import shlex
import subprocess
import tempfile
from fcntl import fcntl, F_GETFL, F_SETFL

from tests.cook import util

logger = logging.getLogger(__name__)

# Manually create a TTY that we can use as the default STDIN
_STDIN_TTY = pty.openpty()[1]


def decode(b):
    """Decodes as UTF-8"""
    return b.decode('UTF-8')


def encode(o):
    """Encodes with UTF-8"""
    return str(o).encode('UTF-8')


def stdout(cp):
    """Returns the UTF-8 decoded and stripped stdout of the given CompletedProcess"""
    return decode(cp.stdout).strip()


def sh(command, stdin=None, env=None, wait_for_exit=True):
    """Runs command using subprocess.run"""
    logger.info(command + (f' # stdin: {decode(stdin)}' if stdin else ''))
    command_args = shlex.split(command)
    if wait_for_exit:
        # We manually attach stdin to a TTY if there is no piped input
        # since the default stdin isn't guaranteed to be a TTY.
        input_args = {'input': stdin} if stdin is not None else {'stdin': _STDIN_TTY}
        cp = subprocess.run(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, **input_args)
        return cp
    else:
        proc = subprocess.Popen(command_args, stdin=_STDIN_TTY, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Get the current stdout, stderr flags
        stdout_flags = fcntl(proc.stdout, F_GETFL)
        stderr_flags = fcntl(proc.stderr, F_GETFL)
        # Set the O_NONBLOCK flag of the stdout, stderr file descriptors
        # (if we don't set this, calls to readlines() will block)
        fcntl(proc.stdout, F_SETFL, stdout_flags | os.O_NONBLOCK)
        fcntl(proc.stderr, F_SETFL, stderr_flags | os.O_NONBLOCK)
        return proc


def command():
    """If the COOK_CLI_COMMAND environment variable is set, returns its value, otherwise 'cs'"""
    return os.environ['COOK_CLI_COMMAND'] if 'COOK_CLI_COMMAND' in os.environ else 'cs'


def cli(args, cook_url=None, flags=None, stdin=None, env=None, wait_for_exit=True):
    """Runs a CLI command with the given URL, flags, and stdin"""
    url_flag = f'--url {cook_url} ' if cook_url else ''
    other_flags = f'{flags} ' if flags else ''
    cp = sh(f'{command()} {url_flag}{other_flags}{args}', stdin, env, wait_for_exit)
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


def show_or_wait(action, uuids=None, cook_url=None, flags=None, action_flags=None, stdin=None):
    """Helper function used to either show or wait via the CLI"""
    action_flags = (action_flags + ' ') if action_flags else ''
    uuids = ' '.join([str(uuid) for uuid in uuids])
    cp = cli('%s %s%s' % (action, action_flags, uuids), cook_url, flags, stdin)
    return cp


def show(uuids=None, cook_url=None, flags=None, show_flags=None, stdin=None):
    """Shows the job(s) corresponding to the given UUID(s) via the CLI"""
    cp = show_or_wait('show', uuids, cook_url, flags, show_flags, stdin)
    return cp


def __show_json(uuids, cook_url=None, flags=None):
    """Invokes show on the given UUIDs with --silent and --json, and returns the parsed JSON"""
    flags = (flags + ' ') if flags else ''
    cp = show(uuids, cook_url, f'{flags}--silent', '--json')
    data = json.loads(stdout(cp))
    return cp, data


def show_jobs(uuids, cook_url=None, flags=None):
    """Shows the job JSON corresponding to the given UUID(s)"""
    cp, data = __show_json(uuids, cook_url, flags)
    jobs = [job for entities in data['clusters'].values() for job in entities['jobs']]
    return cp, jobs


def show_instances(uuids, cook_url=None, flags=None):
    """Shows the instance JSON corresponding to the given UUID(s)"""
    cp, data = __show_json(uuids, cook_url, flags)
    instance_job_pairs = [pair for entities in data['clusters'].values() for pair in entities['instances']]
    return cp, instance_job_pairs


def show_groups(uuids, cook_url=None, flags=None):
    """Shows the group JSON corresponding to the given UUID(s)"""
    cp, data = __show_json(uuids, cook_url, flags)
    groups = [group for entities in data['clusters'].values() for group in entities['groups']]
    return cp, groups


def show_all(uuids, cook_url=None, flags=None):
    """Shows the job, instance, and group JSON corresponding to the given UUID(s)"""
    cp, data = __show_json(uuids, cook_url, flags)
    jobs = [job for entities in data['clusters'].values() for job in entities['jobs']]
    instance_job_pairs = [pair for entities in data['clusters'].values() for pair in entities['instances']]
    groups = [group for entities in data['clusters'].values() for group in entities['groups']]
    return cp, jobs, instance_job_pairs, groups


def wait(uuids=None, cook_url=None, flags=None, wait_flags=None, stdin=None):
    """Waits for the jobs corresponding to the given UUID(s) to complete"""
    cp = show_or_wait('wait', uuids, cook_url, flags, wait_flags, stdin)
    return cp


def write_json(path, config):
    """Writes the given config map as JSON to the given path."""
    with open(path, 'w') as outfile:
        logger.info('echo \'%s\' > %s' % (json.dumps(config), path))
        json.dump(config, outfile)


def plugins_config():
    """If the COOK_PLUGIN_JSON environment variable is set, returns its value, otherwise empty dict"""
    return json.loads(os.environ['COOK_PLUGIN_JSON']) if 'COOK_PLUGIN_JSON' in os.environ else {}


def basic_auth_config():
    """Returns a config map with HTTP basic auth configured."""
    config = {'http': {'auth': {'type': 'basic',
                                'basic': {'user': 'foo',
                                          'pass': 'bar'}}}}
    return config


def base_config():
    """Returns a "base" config map that can be added to."""
    basic_auth = basic_auth_config() if util.http_basic_auth_enabled() else {}
    plugins = plugins_config()
    return deep_merge(basic_auth, plugins)


def write_base_config():
    """If plugins or HTTP basic auth are enabled, creates the config"""
    config = base_config()
    if config:
        write_json(os.path.abspath('.cs.json'), config)
        if util.http_basic_auth_enabled():
            cp = config_get('http.auth.basic.user', '--verbose')
            auth_user = stdout(cp)
            logging.debug(f'stderr is:\n{decode(cp.stderr)}')
            logging.info(f'Auth user is "{auth_user}"')


def deep_merge(a, b):
    """Merges a and b, letting b win if there is a conflict"""
    merged = a.copy()
    for key in b:
        b_value = b[key]
        merged[key] = b_value
        if key in a:
            a_value = a[key]
            if isinstance(a_value, dict) and isinstance(b_value, dict):
                merged[key] = deep_merge(a_value, b_value)
    return merged


class temp_command_env:
    """
    A context manager used to generate and subsequently delete a temporary
    command override (via env) for the CLI. Takes a python source file path.
    """

    def __init__(self, main_src_path):
        self.original_command = os.getenv('COOK_CLI_COMMAND')
        self.new_command = f'python3 {main_src_path}'

    def __enter__(self):
        os.environ['COOK_CLI_COMMAND'] = self.new_command

    def __exit__(self, _, __, ___):
        if self.original_command is None:
            del os.environ['COOK_CLI_COMMAND']
        else:
            os.environ['COOK_CLI_COMMAND'] = self.original_command


class temp_config_file:
    """
    A context manager used to generate and subsequently delete a temporary 
    config file for the CLI. Takes as input the config dictionary to use.
    """

    def __init__(self, config):
        session_module = os.getenv('COOK_SESSION_MODULE')
        if session_module:
            self.config = {'http': {'modules': {'session-module': session_module, 'adapters-module': session_module}}}
            self.config.update(config)
        else:
            self.config = config

    def write_temp_json(self):
        path = tempfile.NamedTemporaryFile(delete=False).name
        config = deep_merge(base_config(), self.config)
        write_json(path, config)
        return path

    def __enter__(self):
        self.path = self.write_temp_json()
        return self.path

    def __exit__(self, _, __, ___):
        os.remove(self.path)


class temp_base_config_file:
    """
    A context manager used to generate and subsequently delete a temporary
    base config file for the CLI. Takes as input the config dictionary to use.
    """

    def __init__(self, config):
        # Get the location of the cs executable so we can add a default `.cs.json` file
        command_args = shlex.split(command())
        command_args.insert(0, 'which')
        cp = subprocess.run(args=command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        base_exec = cp.stdout.decode("utf-8").rstrip('\n')
        base_dir = os.path.dirname(os.path.abspath(base_exec))
        self.path = os.path.join(base_dir, '.cs.json')
        self.config = config

    def __enter__(self):
        write_json(self.path, self.config)
        return self.path

    def __exit__(self, _, __, ___):
        os.remove(self.path)


def jobs(cook_url=None, jobs_flags=None, flags=None):
    """Invokes the jobs subcommand"""
    args = f'jobs {jobs_flags}' if jobs_flags else 'jobs'
    cp = cli(args, cook_url, flags)
    return cp


def jobs_json(cook_url=None, jobs_flags=None, flags=None):
    """Invokes the jobs subcommand with --json"""
    jobs_flags = f'{jobs_flags} --json' if jobs_flags else '--json'
    cp = jobs(cook_url, jobs_flags=jobs_flags, flags=flags)
    try:
        response = json.loads(stdout(cp))
        job_list = [job for entities in response['clusters'].values() for job in entities['jobs']]
        return cp, job_list
    except:
        logging.exception(f'Exception when parsing output from jobs ({output(cp)})')
        raise


def output(cp):
    """Returns a string containing the stdout and stderr from the given CompletedProcess"""
    return f'\nstdout:\n{stdout(cp)}\n\nstderr:\n{decode(cp.stderr)}'


def ssh(uuid, cook_url=None, env=None, flags=None):
    """Invokes the ssh subcommand"""
    args = f'ssh {uuid}'
    cp = cli(args, cook_url, flags=flags, env=env)
    return cp


def tail(uuid, path, cook_url, tail_flags=None, wait_for_exit=True, flags=None):
    """Invokes the tail subcommand"""
    args = f'tail {tail_flags} {uuid} {path}' if tail_flags else f'tail {uuid} {path}'
    cp = cli(args, cook_url, flags=flags, wait_for_exit=wait_for_exit)
    return cp


def ls(uuid, cook_url, path=None, parse_json=True, flags=None):
    """Invokes the ls subcommand"""
    args = f'ls --json {uuid} {path}' if path else f'ls --json {uuid}'
    cp = cli(args, cook_url, flags=flags)
    out = stdout(cp)
    try:
        entries = json.loads(out) if parse_json else None
    except:
        err = decode(cp.stderr)
        logging.exception(f'Exception when parsing output from ls (stdout = {out}, stderr = {err})')
        raise
    return cp, entries


def tail_with_logging(uuid, path, cook_url, num_lines):
    """Invokes tail and performs some extra logging if the tail fails"""
    tail_cp = tail(uuid, path, cook_url, f'--lines {num_lines}')
    if tail_cp.returncode != 0:
        logging.error(f'tail exited {tail_cp.returncode}: {tail_cp.stderr}')
        ls_cp, entries = ls(uuid, cook_url)
        if ls_cp.returncode != 0:
            logging.error(f'ls exited {ls_cp.returncode}: {ls_cp.stderr}')
        else:
            logging.info(f'ls results: {json.dumps(entries, indent=2)}')
    return tail_cp


def ls_entry_by_name(entries, name):
    """
    Given a collection of entries returned by ls, and a name
    to find, returns the first entry with a matching name
    """
    return next(e for e in entries if os.path.basename(os.path.normpath(e['path'])) == name)


def kill(uuids, cook_url, stdin=None):
    """Invokes the kill subcommand"""
    args = f'kill {" ".join([str(u) for u in uuids])}'
    cp = cli(args, cook_url, stdin=stdin)
    return cp


def version():
    """Invokes the CLI with --version and returns the parsed version"""
    cp = cli('--version')
    assert cp.returncode == 0
    string = stdout(cp)
    match = re.match('^cs version (\d+\.\d+\.\d+)$', string)
    if match:
        version_string = match.groups()[0]
        logging.info(f'parsed version string as {version_string}')
        return version_string
    else:
        raise Exception(f'Unable to parse version from {string}')


def config_get(key, flags=None):
    """Invokes the config subcommand to get a config value"""
    cp = cli(f'config --get {key}', flags=flags)
    return cp


def config_set(key, value, flags):
    """Invokes the config subcommand to set a config value"""
    cp = cli(f'config {key} {value}', flags=flags)
    return cp


def command_prefix():
    """Returns the currently configured command-prefix, if any"""
    cp = config_get('defaults.submit.command-prefix')
    return decode(cp.stdout).rstrip('\n') if cp.returncode == 0 else ''


def cat(uuid, path, cook_url, flags=None):
    """Invokes the cat subcommand"""
    args = f'cat {uuid} {path}'
    cp = cli(args, cook_url, flags=flags)
    return cp


def cat_with_logging(uuid, path, cook_url):
    """Invokes the cat subcommand and logs the output"""
    cp = cat(uuid, path, cook_url)
    s = stdout(cp)
    logging.info(f'cat of {path}: {s}')
    return cp


def wait_for_output_file(cook_url, job_uuid, name):
    """Waits for a file with the given name for the given job to exist"""

    def query():
        cp, _ = ls(job_uuid, cook_url, parse_json=False)
        return json.loads(stdout(cp)) if cp.returncode == 0 else []

    def predicate(entries):
        logging.debug(f'Job {job_uuid} has entries {entries}')
        return ls_entry_by_name(entries, name)

    response = util.wait_until(query, predicate)
    return response


def usage(user, cook_url, usage_flags='', flags=None):
    """Invokes the usage subcommand"""
    args = f'usage --user {user} --json {usage_flags}'
    cp = cli(args, cook_url, flags=flags)
    out = stdout(cp)
    return cp, json.loads(out)
