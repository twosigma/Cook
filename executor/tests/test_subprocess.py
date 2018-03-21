import logging
import signal
import subprocess
import time
import unittest

import collections
import pytest

import cook.subprocess as cs
import tests.utils as tu


def find_process_ids_in_group(group_id):
    group_id_to_process_ids = collections.defaultdict(set)
    process_id_to_command = collections.defaultdict(lambda: '')

    p = subprocess.Popen('ps -eo pid,pgid,command',
                         close_fds=True, shell=True,
                         stderr=subprocess.STDOUT, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    ps_output = p.stdout.read().decode('utf8')

    for line in ps_output.splitlines():
        line_split = line.split()
        pid = line_split[0]
        pgid = line_split[1]
        command = str.join(' ', line_split[2:])
        group_id_to_process_ids[pgid].add(pid)
        process_id_to_command[pid] = command

    group_id_str = str(group_id)
    logging.info("group_id_to_process_ids[{}]: {}".format(group_id, group_id_to_process_ids[group_id_str]))
    for pid in group_id_to_process_ids[group_id_str]:
        logging.info("process (pid: {}) command is {}".format(pid, process_id_to_command[pid]))
    return group_id_to_process_ids[group_id_str]


class SubprocessTest(unittest.TestCase):
    # FIXME - remove the xfail mark once the issue with this test failing is resolved:
    # https://github.com/twosigma/Cook/issues/737
    @pytest.mark.xfail
    @unittest.skip('This test fails occasionally')
    def test_kill_task_terminate_with_sigterm(self):
        task_id = tu.get_random_task_id()

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            command = "bash -c 'function handle_term { echo GOT TERM; }; trap handle_term SIGTERM TERM; sleep 200'"
            process = cs.launch_process(command, {})
            shutdown_grace_period_ms = 1000

            group_id = cs.find_process_group(process.pid)
            self.assertGreater(len(find_process_ids_in_group(group_id)), 0)

            cs.kill_process(process, shutdown_grace_period_ms)

            # await process termination
            for i in range(1, 10 * shutdown_grace_period_ms):
                if process.poll() is None:
                    time.sleep(0.01)
            if process.poll() is None:
                process.kill()

            self.assertTrue(((-1 * signal.SIGTERM) == process.poll()) or ((128 + signal.SIGTERM) == process.poll()),
                            'Process exited with code {}'.format(process.poll()))
            self.assertEqual(0, len(find_process_ids_in_group(group_id)))

            with open(stdout_name) as f:
                file_contents = f.read()
                self.assertTrue('GOT TERM' in file_contents)

        finally:
            tu.cleanup_output(stdout_name, stderr_name)

    def test_kill_task_terminate_with_sigkill(self):
        task_id = tu.get_random_task_id()

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            command = "trap '' TERM SIGTERM; sleep 200"
            process = cs.launch_process(command, {})
            shutdown_grace_period_ms = 1000

            group_id = cs.find_process_group(process.pid)
            self.assertGreater(len(find_process_ids_in_group(group_id)), 0)

            cs.kill_process(process, shutdown_grace_period_ms)

            # await process termination
            for i in range(1, 10 * shutdown_grace_period_ms):
                if process.poll() is None:
                    time.sleep(0.01)
            if process.poll() is None:
                process.kill()

            self.assertTrue(((-1 * signal.SIGKILL) == process.poll()) or ((128 + signal.SIGKILL) == process.poll()),
                            'Process exited with code {}'.format(process.poll()))
            self.assertEqual(len(find_process_ids_in_group(group_id)), 0)

        finally:
            tu.cleanup_output(stdout_name, stderr_name)

    def process_launch_and_kill_helper(self, kill_fn):
        task_id = tu.get_random_task_id()

        stdout_name = tu.ensure_directory('build/stdout.{}'.format(task_id))
        stderr_name = tu.ensure_directory('build/stderr.{}'.format(task_id))

        tu.redirect_stdout_to_file(stdout_name)
        tu.redirect_stderr_to_file(stderr_name)

        try:
            start_time = time.time()

            command = 'echo "A.$(sleep 30)" & echo "B.$(sleep 30)" & echo "C.$(sleep 30)" &'
            environment = {}
            process = cs.launch_process(command, environment)

            group_id = cs.find_process_group(process.pid)
            self.assertGreater(group_id, 0)

            child_process_ids = tu.wait_for(lambda: find_process_ids_in_group(group_id),
                                            lambda data: len(data) >= 7,
                                            default_value=[])
            self.assertGreaterEqual(len(child_process_ids), 7)
            self.assertLessEqual(len(child_process_ids), 10)

            kill_fn(process.pid)

            child_process_ids = tu.wait_for(lambda: find_process_ids_in_group(group_id),
                                            lambda data: len(data) == 0,
                                            default_value=[])
            self.assertEqual(0, len(child_process_ids))

            # ensure the test ran in under 30 seconds
            self.assertLess(time.time() - start_time, 20)
        finally:
            tu.cleanup_output(stdout_name, stderr_name)

    def test_process_group_assignment_and_killing_send_process_tree_kill(self):
        self.process_launch_and_kill_helper(lambda pid: cs._send_signal_to_process_tree(pid, signal.SIGKILL))

    def test_process_group_assignment_and_killing_send_process_tree_term(self):
        self.process_launch_and_kill_helper(lambda pid: cs._send_signal_to_process_tree(pid, signal.SIGTERM))

    def test_process_group_assignment_and_killing_send_process_group_kill(self):
        self.process_launch_and_kill_helper(lambda pid: cs._send_signal_to_process_group(pid, signal.SIGKILL))

    def test_process_group_assignment_and_killing_send_process_group_term(self):
        self.process_launch_and_kill_helper(lambda pid: cs._send_signal_to_process_group(pid, signal.SIGTERM))

    def test_process_group_assignment_and_killing_send_signal_kill(self):
        self.process_launch_and_kill_helper(lambda pid: cs.send_signal(pid, signal.SIGKILL))

    def test_process_group_assignment_and_killing_send_signal_term(self):
        self.process_launch_and_kill_helper(lambda pid: cs.send_signal(pid, signal.SIGTERM))
