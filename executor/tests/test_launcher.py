import os
import glob

from queue import Queue
from threading import Event, Thread

from cook.launcher import run_commands

def readlines(path):
    with open(path) as f:
        return [line.rstrip('\n') for line in f]

def remove_logs():
    for path in glob.glob("stdout*"):
        os.remove(path)
    for path in glob.glob("stderr*"):
        os.remove(path)

def setup_module(module):
    remove_logs()

def teardown_module(module):
    remove_logs()

def test_run_commands_simple():
    codes = run_commands([
        {'id': 0, 'value': 'echo 1'},
        {'id': 1, 'value': 'echo 2'}
    ])

    assert codes == [0, 0]

def test_run_commands_with_guard():
    """
    A failing guard should prevent the execution of additional commands
    """
    codes = run_commands([
        {'id': 0, 'value': '/bin/sh -c "exit 1"', 'guard': True},
        {'id': 1, 'value': 'echo 1'}
    ])

    assert codes == [1]

def test_run_commands_with_async():
    """
    An async command should be killed when all sync commands have finished
    """
    codes = run_commands([
        {'id': 0, 'value': 'sleep 10', 'async': True},
        {'id': 1, 'value': 'echo 1'}
    ])

    # -15 means killed by a signal
    assert codes == [-15, 0]

def test_run_commands_capture_output():
    """
    stdout/stderr should be redirected to files
    """
    run_commands([
        {'id': 0, 'value': 'echo 1'},
        {'id': 1, 'value': 'echo 2'}
    ])

    assert readlines('stdout0') == ['1']
    assert readlines('stdout1') == ['2']

def test_run_commands_with_env():
    """
    An async command should be killed when all sync commands have finished
    """
    codes = run_commands([
        {'id': 0, 'value': 'env'}
    ], None, lambda: {'HELLO': 'world'})

    assert 'HELLO=world' in readlines('stdout0')

def test_run_commands_in_thread_and_stop():
    """
    Setting the stop event should cause run_commands to stop
    """

    def thread_target(commands, stop, queue):
        queue.put(run_commands(commands, stop))

    event = Event()
    queue = Queue()
    commands = [
        {'id': 0, 'value': 'echo 1'}
    ]
    thread = Thread(target = thread_target, args=(commands, event, queue))

    event.set()
    thread.start()

    assert queue.get() == []
