"""
A component for launching and monitoring processes for a task's commands.
"""

import os
import time
import shlex
import subprocess

from collections import namedtuple

class Command(
        namedtuple('Command', ('value', 'name', 'async', 'guard'))
):
    """
    An internal representation of a runnable command. Using a namedtuple helps catch
    issues with data integrity.
    """

    def run(self, env):
        if self.name is None:
            stdout = open('stdout', 'w+')
            stderr = open('stderr', 'w+')
        else:
            stdout = open('stdout.' + str(self.name), 'w+')
            stderr = open('stderr.' + str(self.name), 'w+')

        return RunningCommand(
            *self,
            stdout = stdout,
            stderr = stderr,
            process = subprocess.Popen(
                shlex.split(self.value),
                stdout = stdout,
                stderr = stderr,
                env = dict(os.environ, **env)
            )
        )

# set default values for (name, async, guard)
Command.__new__.__defaults__ = (None, False, False)

class RunningCommand(namedtuple(
        'RunningCommand', Command._fields + ('stdout', 'stderr', 'process'))
):
    """
    An internal representation of a running command. Using a namedtuple helps catch
    issues with data integrity.
    """

    def kill(self):
        self.stdout.close()
        self.stderr.close()

        if self.process.returncode is None:
            self.process.terminate()
            for _ in range(0, 10):
                if self.process.poll() is not None:
                    break
                time.sleep(1)
            if self.process.returncode is None:
                self.process.kill()

        return self.process.returncode

    def is_guard(self):
        return self.guard or False

    def is_async(self):
        return self.async or False

    def is_sync(self):
        return not self.is_async()

    def is_running(self):
        return self.process.poll() is None

    def is_crashed(self):
        return not self.is_running() and self.process.poll() is not 0

def run_commands(commands, stop = None, get_env = lambda: {}):
    """
    Execute a list of commands, waiting for all synchronous commands to complete. Will also
    exit if the stop event is set.

    Returns a list of exit codes for the command processes.
    """
    running_commands = []
    iterator_commands = (Command(**c) for c in commands)

    try:
        while not (stop and stop.isSet()):
            should_await = [c for c in running_commands if c.is_running() and c.is_sync()]
            should_abort = [c for c in running_commands if c.is_crashed() and c.is_guard()]

            if should_abort:
                break
            elif not should_await:
                if len(commands) == len(running_commands):
                    break
                else:
                    running_commands.append(next(iterator_commands).run(get_env()))
            else:
                time.sleep(1)
    finally:
        return [c.kill() for c in running_commands]

def run_launcher(store, stop):
    """
    Wait for a task to be created and then run its commands. Will also exit if the stop
    event is set.

    Updates the store with the commands' exit codes upon completion.
    """
    while not stop.isSet() and not store.all('task'):
        time.sleep(1)

    if not stop.isSet():
        id, task = store.all('task').popitem()

        store.merge('task', id, {
            'codes': run_commands(
                task['commands'],
                stop,
                lambda: store.get('task', id).get('env', {})
            )
        })
