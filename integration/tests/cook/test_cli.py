import json
import logging
import os
import pytest
import subprocess
import time
import unittest
import uuid

from urllib.parse import urlparse

from tests.cook import cli, util


@pytest.mark.cli
@unittest.skipIf(util.http_basic_auth_enabled(), 'Cook CLI does not currently support HTTP Basic Auth')
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class CookCliTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retrieve_cook_url()
        util.init_cook_session(cls.cook_url)

    def current_name(self):
        """Returns the name of the currently running test function"""
        test_id = self.id()
        return test_id.split('.')[-1]

    def setUp(self):
        self.cook_url = type(self).cook_url
        self.logger = logging.getLogger(__name__)

    def test_basic_submit_and_wait(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('completed', jobs[0]['status'])

    def test_config_defaults_are_respected(self):
        # Submit job with defaults in config file
        config = {'defaults': {'submit': {'mem': 256,
                                          'cpus': 2,
                                          'priority': 16,
                                          'max-retries': 2,
                                          'max-runtime': 300}}}
        with cli.temp_config_file(config) as path:
            cp, uuids = cli.submit('ls', self.cook_url, '--config %s' % path)
            self.assertEqual(0, cp.returncode, cp.stderr)

        # Assert that the job was submitted with the defaults from the file
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        job = jobs[0]
        defaults = config['defaults']['submit']
        self.assertEqual(defaults['mem'], job['mem'])
        self.assertEqual(defaults['cpus'], job['cpus'])
        self.assertEqual(defaults['priority'], job['priority'])
        self.assertEqual(defaults['max-retries'], job['max_retries'])
        self.assertEqual(defaults['max-runtime'], job['max_runtime'])

    def test_submit_accepts_command_from_stdin(self):
        cp, uuids = cli.submit(cook_url=self.cook_url, stdin=cli.encode('ls'))
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(uuids), uuids)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        cp, uuids = cli.submit(cook_url=self.cook_url, stdin=cli.encode(''))
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('must specify at least one command', cli.decode(cp.stderr))

    def test_multiple_commands_submits_multiple_jobs(self):
        cp, uuids = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(uuids))
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(jobs))
        cp, uuids = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url, submit_flags='--uuid %s' % uuid.uuid4())
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('cannot specify multiple subcommands with a single UUID', cli.decode(cp.stderr))

    def test_wait_for_multiple_jobs(self):
        cp, uuids = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        returned_uuids = [j['uuid'] for j in jobs]
        self.assertEqual(uuids, returned_uuids)
        self.assertEqual('completed', jobs[0]['status'])
        self.assertEqual('completed', jobs[1]['status'])
        self.assertEqual('completed', jobs[2]['status'])

    def test_error_on_invalid_config_path(self):
        cp, uuids = cli.submit('ls', flags='--config /bogus/path/%s' % uuid.uuid4())
        self.assertEqual(1, cp.returncode, cp.stderr)

    def test_specifying_cluster_name_explicitly(self):
        cluster_name = 'foo'
        config = {'clusters': [{'name': cluster_name, 'url': self.cook_url}],
                  'defaults': {'submit': {'mem': 256, 'cpus': 2, 'max-retries': 2}}}
        with cli.temp_config_file(config) as path:
            flags = f'--config {path} --cluster {cluster_name}'
            cp, uuids = cli.submit('ls', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp, jobs = cli.show_jobs(uuids, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            # Cluster names are case insensitive
            uppercase_cluster_name = cluster_name.upper()
            self.assertNotEqual(cluster_name, uppercase_cluster_name)
            flags = f'--config {path} --cluster {uppercase_cluster_name}'
            cp, jobs = cli.show_jobs(uuids, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)

    def test_verbose_flag(self):
        cp, _ = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp_verbose, _ = cli.submit('ls', self.cook_url, '--verbose')
        self.assertEqual(0, cp_verbose.returncode, cp_verbose.stderr)
        self.assertTrue(len(cp_verbose.stderr) > len(cp.stderr))

    def test_usage_information(self):
        cp = cli.cli('')
        self.assertEqual(0, cp.returncode, cp.stderr)
        stdout = cli.stdout(cp)
        cp = cli.cli('', flags='--help')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(stdout, cli.stdout(cp))
        self.assertIn('usage:', stdout)
        self.assertIn('positional arguments:', stdout)
        self.assertIn('optional arguments:', stdout)

    def test_error_if_both_cluster_and_url_specified(self):
        cp, _ = cli.submit('ls', flags='--cluster foo --url bar')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('cannot specify both a cluster name and a cluster url', cli.decode(cp.stderr))

    def test_no_cluster(self):
        config = {'clusters': []}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp, uuids = cli.submit('ls', flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('must specify at least one cluster', cli.decode(cp.stderr))

    def test_submit_specify_fields(self):
        juuid = uuid.uuid4()
        name = 'foo'
        priority = 32
        max_retries = 12
        max_runtime = 34
        cpus = 0.1
        mem = 56
        submit_flags = '--uuid %s --name %s --priority %s --max-retries %s --max-runtime %s --cpus %s --mem %s' % \
                       (juuid, name, priority, max_retries, max_runtime, cpus, mem)
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=submit_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(str(juuid), uuids[0], uuids)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(name, jobs[0]['name'])
        self.assertEqual(priority, jobs[0]['priority'])
        self.assertEqual(max_retries, jobs[0]['max_retries'])
        self.assertEqual(max_runtime, jobs[0]['max_runtime'])
        self.assertEqual(cpus, jobs[0]['cpus'])
        self.assertEqual(mem, jobs[0]['mem'])

    def test_submit_raw(self):
        command = 'ls'
        juuid = uuid.uuid4()
        name = 'foo'
        priority = 32
        max_retries = 12
        max_runtime = 3456
        cpus = 0.1
        mem = 56
        raw_job = {'command': command,
                   'uuid': str(juuid),
                   'name': name,
                   'priority': priority,
                   'max-retries': max_retries,
                   'max-runtime': max_runtime,
                   'cpus': cpus,
                   'mem': mem}
        cp, uuids = cli.submit(stdin=cli.encode(json.dumps(raw_job)), cook_url=self.cook_url, submit_flags='--raw')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(str(juuid), uuids[0], uuids)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(f'{cli.command_prefix()}{command}', jobs[0]['command'])
        self.assertEqual(name, jobs[0]['name'])
        self.assertEqual(priority, jobs[0]['priority'])
        self.assertEqual(max_retries, jobs[0]['max_retries'])
        self.assertEqual(max_runtime, jobs[0]['max_runtime'])
        self.assertEqual(cpus, jobs[0]['cpus'])
        self.assertEqual(mem, jobs[0]['mem'])

    def test_submit_raw_multiple(self):
        command = 'ls'
        name = 'foo'
        priority = 32
        max_retries = 12
        max_runtime = 3456
        cpus = 0.1
        mem = 56
        raw_job = {'command': command,
                   'name': name,
                   'priority': priority,
                   'max-retries': max_retries,
                   'max-runtime': max_runtime,
                   'cpus': cpus,
                   'mem': mem}
        cp, uuids = cli.submit(stdin=cli.encode(json.dumps([raw_job, raw_job, raw_job])),
                               cook_url=self.cook_url, submit_flags='--raw')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(uuids), uuids)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(jobs), jobs)
        for job in jobs:
            self.assertEqual(f'{cli.command_prefix()}{command}', job['command'])
            self.assertEqual(name, job['name'])
            self.assertEqual(priority, job['priority'])
            self.assertEqual(max_retries, job['max_retries'])
            self.assertEqual(max_runtime, job['max_runtime'])
            self.assertEqual(cpus, job['cpus'])
            self.assertEqual(mem, job['mem'])

    def test_submit_raw_invalid(self):
        cp, _ = cli.submit(stdin=cli.encode('1'), cook_url=self.cook_url, submit_flags='--raw')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('malformed JSON for raw', cli.decode(cp.stderr))

    def test_name_default(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('%s_job' % os.environ['USER'], jobs[0]['name'])

    def test_wait_requires_at_least_one_uuid(self):
        cp = cli.wait([], self.cook_url, stdin='')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('You must specify at least one UUID', cli.decode(cp.stderr))

    def test_wait_specify_timeout_and_interval(self):
        cp, uuids = cli.submit('"sleep 60"', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        start_time = time.time()
        cp = cli.wait(uuids, self.cook_url, wait_flags='--timeout 1')
        elapsed_time = time.time() - start_time
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Timeout waiting', cli.decode(cp.stderr))
        self.assertLess(elapsed_time, 20)
        self.assertGreater(elapsed_time, 3)
        start_time = time.time()
        cp = cli.wait(uuids, self.cook_url, wait_flags='--timeout 1 --interval 1')
        elapsed_time_2 = time.time() - start_time
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Timeout waiting', cli.decode(cp.stderr))
        self.assertLess(elapsed_time_2, elapsed_time)

    def test_query_invalid_uuid(self):
        cp = cli.show([uuid.uuid4()], self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('No matching data found', cli.stdout(cp))
        cp = cli.wait([uuid.uuid4()], self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('No matching data found', cli.stdout(cp))

    def test_show_requires_at_least_one_uuid(self):
        cp = cli.show([], self.cook_url, stdin='')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('You must specify at least one UUID', cli.decode(cp.stderr))

    def test_assume_http_if_elided(self):
        url = urlparse(self.cook_url)
        url_sans_scheme = url.netloc
        cp, uuids = cli.submit('ls', url_sans_scheme)
        self.assertEqual(0, cp.returncode, cp.stderr)

    def test_double_dash_for_end_of_options(self):
        # Double-dash for 'end of options'
        cp, uuids = cli.submit('-- ls -al', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(f'{cli.command_prefix()}ls -al', jobs[0]['command'])
        # Double-dash along with other flags
        cp, uuids = cli.submit('-- ls -al', self.cook_url, submit_flags='--name foo --priority 12')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(f'{cli.command_prefix()}ls -al', jobs[0]['command'])
        self.assertEqual('foo', jobs[0]['name'])
        self.assertEqual(12, jobs[0]['priority'])

    def test_retries(self):
        # Default retries = 2
        cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose')
        stderr = cli.decode(cp.stderr)
        self.assertEqual(1, cp.returncode, stderr)
        self.assertIn('Retrying (Retry(total=1', stderr)
        self.assertIn('Retrying (Retry(total=0', stderr)
        self.assertNotIn('Retrying (Retry(total=2', stderr)
        # Set retries = 0
        config = {'http': {'retries': 0}}
        with cli.temp_config_file(config) as path:
            cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose --config %s' % path)
            stderr = cli.decode(cp.stderr)
            self.assertEqual(1, cp.returncode, stderr)
            self.assertNotIn('Retrying (Retry(total=0', stderr)
        # Set retries = 4
        config = {'http': {'retries': 4}}
        with cli.temp_config_file(config) as path:
            cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose --config %s' % path)
            stderr = cli.decode(cp.stderr)
            self.assertEqual(1, cp.returncode, stderr)
            self.assertIn('Retrying (Retry(total=3', stderr)
            self.assertIn('Retrying (Retry(total=2', stderr)
            self.assertIn('Retrying (Retry(total=1', stderr)
            self.assertIn('Retrying (Retry(total=0', stderr)
            self.assertNotIn('Retrying (Retry(total=4', stderr)

    def test_submit_priority(self):
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--priority 0')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(0, jobs[0]['priority'])
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--priority 100')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(100, jobs[0]['priority'])
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--priority -1')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('--priority/-p: invalid choice', cli.decode(cp.stderr))
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--priority 101')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('--priority/-p: invalid choice', cli.decode(cp.stderr))

    def test_submit_output_should_explain_what_happened(self):
        cp, _ = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn("succeeded", cli.stdout(cp))
        self.assertIn("Your job UUID is", cli.stdout(cp))
        cp, _ = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn("succeeded", cli.stdout(cp))
        self.assertIn("Your job UUIDs are", cli.stdout(cp))

    def test_submit_raw_should_error_if_command_is_given(self):
        cp, _ = cli.submit('ls', self.cook_url, submit_flags='--raw')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('cannot specify a command at the command line when using --raw/-r', cli.decode(cp.stderr))

    def test_show_running_job(self):
        cp, uuids = cli.submit('sleep 60', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'running')
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)

    def test_quoting(self):
        cp, uuids = cli.submit('echo "Hello; exit 1"', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('completed', jobs[0]['status'])
        self.assertEqual('success', jobs[0]['state'])

    def test_complex_commands(self):
        desired_command = '(foo -x \'def bar = "baz"\')'
        # Incorrectly submitted command
        command = '"(foo -x \'def bar = "baz"\')"'
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        expected_command = desired_command.replace('"', '')
        self.assertEqual(f'{cli.command_prefix()}{expected_command}', jobs[0]['command'])
        # Correctly submitted command
        command = '"(foo -x \'def bar = \\"baz\\"\')"'
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(f'{cli.command_prefix()}{desired_command}', jobs[0]['command'])

        desired_command = "export HOME=$MESOS_DIRECTORY; export LOGNAME=$(whoami); JAVA_OPTS='-Xmx15000m' foo"
        # Incorrectly submitted command
        command = "'export HOME=$MESOS_DIRECTORY; export LOGNAME=$(whoami); JAVA_OPTS='-Xmx15000m' foo'"
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        expected_command = desired_command.replace("'", '')
        self.assertEqual(f'{cli.command_prefix()}{expected_command}', jobs[0]['command'])
        # Correctly submitted command
        command = "'export HOME=$MESOS_DIRECTORY; export LOGNAME=$(whoami); JAVA_OPTS='\"'\"'-Xmx15000m'\"'\"' foo'"
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(f'{cli.command_prefix()}{desired_command}', jobs[0]['command'])

    def test_list_no_matching_jobs(self):
        cp = cli.jobs(self.cook_url, '--name %s' % uuid.uuid4())
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('No running jobs', cli.stdout(cp))
        self.assertIn(f'found in {self.cook_url}.', cli.stdout(cp))

    def list_jobs(self, name, user, *states):
        """Invokes the jobs subcommand with the given name, user, and state filters"""
        state_flags = ' '.join([f'--{state}' for state in states])
        cp, jobs = cli.jobs_json(self.cook_url, '--name %s --user %s %s' % (name, user, state_flags))
        return cp, jobs

    def test_list_by_state(self):
        name = f'{self.current_name()}_{uuid.uuid4()}'

        # Submit a job that will never run
        raw_job = {'command': 'ls', 'name': name, 'constraints': [['HOSTNAME', 'EQUALS', 'will not get scheduled']]}
        cp, uuids = cli.submit(stdin=cli.encode(json.dumps(raw_job)), cook_url=self.cook_url, submit_flags='--raw')
        self.assertEqual(0, cp.returncode, cp.stderr)
        user = util.get_user(self.cook_url, uuids[0])
        waiting_uuid = uuids[0]

        # Submit a long-running job
        cp, uuids = cli.submit('sleep 300', self.cook_url, submit_flags='--name %s' % name)
        self.assertEqual(0, cp.returncode, cp.stderr)
        running_uuid = uuids[0]

        try:
            # Submit a successful job
            cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--name %s' % name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            success_uuid = uuids[0]

            # Submit a failed job
            cp, uuids = cli.submit('exit 1', self.cook_url, submit_flags='--name %s' % name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            failed_uuid = uuids[0]

            # Wait for the desired states to be reached
            util.wait_for_job(self.cook_url, waiting_uuid, 'waiting')
            util.wait_for_job(self.cook_url, running_uuid, 'running')
            util.wait_for_job(self.cook_url, success_uuid, 'completed')
            util.wait_for_job(self.cook_url, failed_uuid, 'completed')

            # waiting
            cp, jobs = self.list_jobs(name, user, 'waiting')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(waiting_uuid, jobs[0]['uuid'])
            # running
            cp, jobs = self.list_jobs(name, user, 'running')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(running_uuid, jobs[0]['uuid'])
            # completed
            cp, jobs = self.list_jobs(name, user, 'completed')
            uuids = [j['uuid'] for j in jobs]
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(2, len(jobs))
            self.assertIn(success_uuid, uuids)
            self.assertIn(failed_uuid, uuids)
            # success
            cp, jobs = self.list_jobs(name, user, 'success')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(success_uuid, jobs[0]['uuid'])
            # failed
            cp, jobs = self.list_jobs(name, user, 'failed')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(failed_uuid, jobs[0]['uuid'])
            # all
            cp, jobs = self.list_jobs(name, user, 'all')
            uuids = [j['uuid'] for j in jobs]
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(4, len(jobs))
            self.assertIn(waiting_uuid, uuids)
            self.assertIn(running_uuid, uuids)
            self.assertIn(success_uuid, uuids)
            self.assertIn(failed_uuid, uuids)
            # waiting+running
            cp, jobs = self.list_jobs(name, user, 'waiting', 'running')
            uuids = [j['uuid'] for j in jobs]
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(2, len(jobs))
            self.assertIn(waiting_uuid, uuids)
            self.assertIn(running_uuid, uuids)
            # completed+waiting
            cp, jobs = self.list_jobs(name, user, 'completed', 'waiting')
            uuids = [j['uuid'] for j in jobs]
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(3, len(jobs), f'Expected 3 jobs, got: {jobs}')
            self.assertIn(waiting_uuid, uuids)
            self.assertIn(success_uuid, uuids)
            self.assertIn(failed_uuid, uuids)
        finally:
            util.kill_jobs(self.cook_url, jobs=[waiting_uuid, running_uuid])

    def test_list_invalid_state(self):
        cp = cli.jobs(self.cook_url, '--foo')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('error: unrecognized arguments: --foo', cli.decode(cp.stderr))

    def test_submit_with_application(self):
        # Specifying application name and version
        cp, uuids = cli.submit('ls', self.cook_url,
                               submit_flags='--application-name %s --application-version %s' % ('foo', '1.2.3'))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual('foo', jobs[0]['application']['name'])
        self.assertEqual('1.2.3', jobs[0]['application']['version'])
        # Default application name
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual('cook-scheduler-cli', jobs[0]['application']['name'])
        self.assertEqual(cli.version(), jobs[0]['application']['version'])
        # User-defined defaults
        config = {'defaults': {'submit': {'application-name': 'bar', 'application-version': '4.5.6'}}}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp, uuids = cli.submit('ls', self.cook_url, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp, jobs = cli.show_jobs(uuids, self.cook_url)
            self.assertEqual('bar', jobs[0]['application']['name'])
            self.assertEqual('4.5.6', jobs[0]['application']['version'])

    def test_list_invalid_flags(self):
        error_fragment = 'cannot specify both lookback hours and submitted after / before times'
        cp = cli.jobs(self.cook_url, '--lookback 1 --submitted-after "yesterday" --submitted-before "now"')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(error_fragment, cli.decode(cp.stderr))
        cp = cli.jobs(self.cook_url, '--lookback 1 --submitted-after "yesterday"')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(error_fragment, cli.decode(cp.stderr))
        cp = cli.jobs(self.cook_url, '--lookback 1 --submitted-before "now"')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(error_fragment, cli.decode(cp.stderr))
        cp = cli.jobs(self.cook_url, '--submitted-after ""')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('"" is not a valid date / time string', cli.decode(cp.stderr))
        cp = cli.jobs(self.cook_url, '--submitted-before ""')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('"" is not a valid date / time string', cli.decode(cp.stderr))

    def test_list_with_time_ranges(self):
        name = str(uuid.uuid4())
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--name {name}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        user = util.get_user(self.cook_url, uuids[0])
        # Time range that should be empty
        list_flags = f'--submitted-after "30 minutes ago" --submitted-before "15 minutes ago" ' \
                     f'--user {user} --all --name {name}'
        cp, jobs = cli.jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(0, len(jobs))
        # Time range that should contain our job (wait for job to complete to avoid racing with the "now" query)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        list_flags = f'--submitted-after "30 minutes ago" --submitted-before "now" --user {user} --all --name {name}'
        cp, jobs = cli.jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertIn(uuids[0], jobs[0]['uuid'])
        # --submitted-after is not required
        list_flags = f'--submitted-before "now" --user {user} --all --name {name}'
        cp, jobs = cli.jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertIn(uuids[0], jobs[0]['uuid'])
        # --submitted-before is not required
        list_flags = f'--submitted-after "15 minutes ago" --user {user} --all --name {name}'
        cp, jobs = cli.jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertIn(uuids[0], jobs[0]['uuid'])

    def test_jobs_exclude_custom_executor(self):
        # Unfortunately, there is no easy way to create a job with a custom executor.
        # Instead, we will check that we are making a request to the correct endpoint by
        # inspecting the --verbose output.
        cp, jobs = cli.jobs_json(self.cook_url, '--exclude-custom-executor', flags='--verbose')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('/list', cli.decode(cp.stderr))
        self.assertNotIn('/jobs', cli.decode(cp.stderr))
        cp, jobs = cli.jobs_json(self.cook_url, flags='--verbose')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('/jobs', cli.decode(cp.stderr))
        self.assertNotIn('/list', cli.decode(cp.stderr))

    def test_ssh_job_uuid(self):
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        instance = util.wait_for_output_url(self.cook_url, uuids[0])
        hostname = instance['hostname']
        env = os.environ
        env['CS_SSH'] = 'echo'
        cp = cli.ssh(uuids[0], self.cook_url, env=env)
        stdout = cli.stdout(cp)
        self.assertEqual(0, cp.returncode, cli.decode(cp.stderr))
        self.assertIn(f'Attempting ssh for job instance {instance["task_id"]}', stdout)
        self.assertIn('Executing ssh', stdout)
        self.assertIn(hostname, stdout)
        self.assertIn(f'-t {hostname} cd', stdout)
        self.assertIn('; bash', stdout)

    def test_ssh_no_instances(self):
        raw_job = {'command': 'ls', 'constraints': [['HOSTNAME', 'EQUALS', 'will not get scheduled']]}
        cp, uuids = cli.submit(stdin=cli.encode(json.dumps(raw_job)), cook_url=self.cook_url, submit_flags='--raw')
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            util.wait_for_job(self.cook_url, uuids[0], 'waiting')
            cp = cli.ssh(uuids[0], self.cook_url)
            self.assertEqual(1, cp.returncode, cp.stdout)
            self.assertIn('currently has no instances', cli.decode(cp.stderr))
        finally:
            util.kill_jobs(self.cook_url, uuids)

    def test_ssh_invalid_uuid(self):
        cp = cli.ssh(uuid.uuid4(), self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stdout)
        self.assertIn('No matching data found', cli.decode(cp.stderr))

    def test_ssh_duplicate_uuid(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        instance = util.wait_for_instance(self.cook_url, uuids[0])
        instance_uuid = instance['task_id']
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--uuid {instance_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.ssh(instance_uuid, self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stdout)
        self.assertIn('There is more than one match for the given uuid', cli.decode(cp.stderr))

    def test_ssh_group_uuid(self):
        group_uuid = uuid.uuid4()
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--group {group_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.ssh(group_uuid, self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stdout)
        self.assertIn('You provided a job group uuid', cli.decode(cp.stderr))

    def test_ssh_instance_uuid(self):
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        instance = util.wait_for_output_url(self.cook_url, uuids[0])
        hostname = instance['hostname']
        env = os.environ
        env['CS_SSH'] = 'echo'
        cp = cli.ssh(instance['task_id'], self.cook_url, env=env)
        stdout = cli.stdout(cp)
        self.assertEqual(0, cp.returncode, cli.decode(cp.stderr))
        self.assertIn('Executing ssh', stdout)
        self.assertIn(hostname, stdout)
        self.assertIn(f'-t {hostname} cd', stdout)
        self.assertIn('; bash', stdout)

    def test_tail_basic(self):
        cp, uuids = cli.submit('bash -c "for i in {1..100}; do echo $i >> foo; done"', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        # Ask for 0 lines
        cp = cli.tail(uuids[0], 'foo', self.cook_url, '--lines 0')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('0 is not a positive integer', cli.decode(cp.stderr))
        # Ask for 1 line
        cp = cli.tail_with_logging(uuids[0], 'foo', self.cook_url, 1)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('100\n', cli.decode(cp.stdout))
        # Ask for 10 lines
        cp = cli.tail_with_logging(uuids[0], 'foo', self.cook_url, 10)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('\n'.join([str(i) for i in range(91, 101)]) + '\n', cli.decode(cp.stdout))
        # Ask for 100 lines
        cp = cli.tail_with_logging(uuids[0], 'foo', self.cook_url, 100)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('\n'.join([str(i) for i in range(1, 101)]) + '\n', cli.decode(cp.stdout))
        # Ask for 1000 lines
        cp = cli.tail_with_logging(uuids[0], 'foo', self.cook_url, 1000)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('\n'.join([str(i) for i in range(1, 101)]) + '\n', cli.decode(cp.stdout))
        # Ask for a file that doesn't exist
        cp = cli.tail(uuids[0], uuid.uuid4(), self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('file was not found', cli.decode(cp.stderr))

    def test_tail_no_newlines(self):
        cp, uuids = cli.submit('bash -c \'for i in {1..100}; do printf "$i " >> foo; done\'', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.tail(uuids[0], 'foo', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(' '.join([str(i) for i in range(1, 101)]) + ' ', cli.decode(cp.stdout))

    def test_tail_large_file(self):
        iterations = 20
        cp, uuids = cli.submit('bash -c \'printf "hello\\nworld\\n" > file.txt; '
                               f'for i in {{1..{iterations}}}; do '
                               'cat file.txt file.txt > file2.txt && '
                               'mv file2.txt file.txt; done\'',
                               self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        lines_to_tail = pow(2, iterations - 1)
        cp = cli.tail(uuids[0], 'file.txt', self.cook_url, f'--lines {lines_to_tail}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('hello\nworld\n' * int(lines_to_tail / 2), cli.decode(cp.stdout))

    def test_tail_large_file_no_newlines(self):
        iterations = 18
        cp, uuids = cli.submit('bash -c \'printf "helloworld" > file.txt; '
                               f'for i in {{1..{iterations}}}; do '
                               'cat file.txt file.txt > file2.txt && '
                               'mv file2.txt file.txt; done\'',
                               self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.tail(uuids[0], 'file.txt', self.cook_url, f'--lines 1')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('helloworld' * pow(2, iterations), cli.decode(cp.stdout))

    def test_tail_follow(self):
        sleep_seconds_between_lines = 0.5
        cp, uuids = cli.submit(
            f'bash -c \'for i in {{1..300}}; do echo $i >> bar; sleep {sleep_seconds_between_lines}; done\'',
            self.cook_url, submit_flags=f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_instance(self.cook_url, uuids[0])
        util.wait_for_output_url(self.cook_url, uuids[0])
        proc = cli.tail(uuids[0], 'bar', self.cook_url,
                        f'--follow --sleep-interval {sleep_seconds_between_lines}',
                        wait_for_exit=False)
        try:
            def readlines():
                stdout_lines = proc.stdout.readlines()
                self.logger.info(f'stdout lines: {stdout_lines}')
                stderr_lines = proc.stderr.readlines()
                self.logger.info(f'stderr lines: {stderr_lines}')
                return stdout_lines

            # Wait until the tail prints some lines and then check the output
            lines = util.wait_until(readlines, lambda l: len(l) > 0)
            start = int(lines[0].decode().strip())
            for i, line in enumerate(lines):
                self.assertEqual(f'{start+i}\n', line.decode())

            # Wait until it prints some more lines and then check the (new) output
            lines = util.wait_until(readlines, lambda l: len(l) > 0)
            for j, line in enumerate(lines):
                self.assertEqual(f'{start+i+j+1}\n', line.decode())
        finally:
            proc.kill()
            util.kill_jobs(self.cook_url, jobs=uuids)

    def test_tail_zero_byte_file(self):
        cp, uuids = cli.submit('touch file.txt', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.tail(uuids[0], 'file.txt', self.cook_url, f'--lines 1')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('', cli.decode(cp.stdout))

    def test_tail_default_path(self):
        text = str(uuid.uuid4())
        cp, uuids = cli.submit(f'echo {text}', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'completed')
        cp = cli.tail(uuids[0], '', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn(text, cli.decode(cp.stdout))

    def test_ls(self):

        def entry(name):
            return cli.ls_entry_by_name(entries, name)

        cp, uuids = cli.submit('"mkdir foo; echo 123 > foo/bar; echo 45678 > baz; mkdir empty"',
                               self.cook_url, submit_flags=f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cli.wait_for_output_file(self.cook_url, uuids[0], 'empty')

        # Path that doesn't exist
        cp, entries = cli.ls(uuids[0], self.cook_url, 'qux', parse_json=False)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('no such file or directory', cli.decode(cp.stderr))

        # baz file
        cp, entries = cli.ls(uuids[0], self.cook_url, 'baz')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        baz = entry('baz')
        self.assertEqual('-rw-r--r--', baz['mode'])
        self.assertEqual(1, baz['nlink'])
        self.assertEqual(6, baz['size'])

        # empty directory
        cp, entries = cli.ls(uuids[0], self.cook_url, 'empty')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(0, len(entries))
        self.logger.debug(entries)

        # Root of the sandbox
        cp, entries = cli.ls(uuids[0], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertLessEqual(4, len(entries))
        self.logger.debug(entries)
        foo = entry('foo')
        self.assertEqual('drwxr-xr-x', foo['mode'])
        self.assertLessEqual(2, foo['nlink'])
        baz = entry('baz')
        self.assertEqual('-rw-r--r--', baz['mode'])
        self.assertEqual(1, baz['nlink'])
        self.assertEqual(6, baz['size'])
        stdout = entry('stdout')
        self.assertEqual('-rw-r--r--', stdout['mode'])
        self.assertEqual(1, stdout['nlink'])
        stderr = entry('stderr')
        self.assertEqual('-rw-r--r--', stderr['mode'])
        self.assertEqual(1, stderr['nlink'])

        # foo directory
        cp, entries = cli.ls(uuids[0], self.cook_url, 'foo')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        bar = entry('bar')
        self.assertEqual('-rw-r--r--', bar['mode'])
        self.assertEqual(1, bar['nlink'])
        self.assertEqual(4, bar['size'])

        # foo/bar file
        cp, entries = cli.ls(uuids[0], self.cook_url, 'foo/bar')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        bar = entry('bar')
        self.assertEqual('-rw-r--r--', bar['mode'])
        self.assertEqual(1, bar['nlink'])
        self.assertEqual(4, bar['size'])

    def test_ls_with_globbing_characters(self):

        def entry(name):
            return cli.ls_entry_by_name(entries, name)

        cp, uuids = cli.submit('"touch t?.sh; touch [ab]*; touch {b,c,est}; touch \'*\'; touch \'t*\'; touch done"',
                               self.cook_url, submit_flags=f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cli.wait_for_output_file(self.cook_url, uuids[0], 'done')

        path = 't?.sh'
        cp, _ = cli.ls(uuids[0], self.cook_url, path, parse_json=False)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('ls does not support globbing', cli.stdout(cp))
        cp, entries = cli.ls(uuids[0], self.cook_url, f'{path} --literal')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        bar = entry(path)
        self.assertEqual('-rw-r--r--', bar['mode'])
        self.assertEqual(1, bar['nlink'])
        self.assertEqual(0, bar['size'])

        path = '[ab]*'
        cp, _ = cli.ls(uuids[0], self.cook_url, path, parse_json=False)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('ls does not support globbing', cli.stdout(cp))
        cp, entries = cli.ls(uuids[0], self.cook_url, f'{path} --literal')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        bar = entry(path)
        self.assertEqual('-rw-r--r--', bar['mode'])
        self.assertEqual(1, bar['nlink'])
        self.assertEqual(0, bar['size'])

        path = '{b,c,est}'
        cp, _ = cli.ls(uuids[0], self.cook_url, path, parse_json=False)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('ls does not support globbing', cli.stdout(cp))
        cp, entries = cli.ls(uuids[0], self.cook_url, f'{path} --literal')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        bar = entry(path)
        self.assertEqual('-rw-r--r--', bar['mode'])
        self.assertEqual(1, bar['nlink'])
        self.assertEqual(0, bar['size'])

        path = '*'
        cp, _ = cli.ls(uuids[0], self.cook_url, path, parse_json=False)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('ls does not support globbing', cli.stdout(cp))
        cp, entries = cli.ls(uuids[0], self.cook_url, f'{path} --literal')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        bar = entry(path)
        self.assertEqual('-rw-r--r--', bar['mode'])
        self.assertEqual(1, bar['nlink'])
        self.assertEqual(0, bar['size'])

        path = 't*'
        cp, _ = cli.ls(uuids[0], self.cook_url, path, parse_json=False)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('ls does not support globbing', cli.stdout(cp))
        cp, entries = cli.ls(uuids[0], self.cook_url, f'{path} --literal')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(entries))
        self.logger.debug(entries)
        bar = entry(path)
        self.assertEqual('-rw-r--r--', bar['mode'])
        self.assertEqual(1, bar['nlink'])
        self.assertEqual(0, bar['size'])

    def test_ls_empty_root_directory(self):
        cp, uuids = cli.submit("'rm -r * && rm -r .*'", self.cook_url, submit_flags=f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'completed')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, entries = cli.ls(uuids[0], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(0, len(entries))

    def __wait_for_progress_message(self, uuids):
        return util.wait_until(lambda: cli.show_jobs(uuids, self.cook_url)[1][0]['instances'][0],
                               lambda i: 'progress' in i and 'progress_message' in i)

    def __wait_for_exit_code(self, uuids):
        return util.wait_until(lambda: cli.show_jobs(uuids, self.cook_url)[1][0]['instances'][0],
                               lambda i: 'exit_code' in i)

    def __wait_for_executor_completion_message(self, uuids):
        def query():
            cp = cli.tail(uuids[0], 'stdout', self.cook_url, '--lines 100')
            stdout = cli.decode(cp.stdout)
            self.logger.info(f'stdout = {stdout}')
            return stdout

        return util.wait_until(query, lambda out: 'Executor completed execution' in out)

    def test_show_progress_message(self):
        executor = util.get_job_executor_type(self.cook_url)
        line = util.progress_line(self.cook_url, 99, 'We are so close!')
        cp, uuids = cli.submit(f'echo "{line}"', self.cook_url, submit_flags=f'--executor {executor} '
                                                                             f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_instance(self.cook_url, uuids[0])
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(executor, jobs[0]['instances'][0]['executor'])
        if executor == 'cook':
            instance = self.__wait_for_exit_code(uuids)
            self.assertEqual(0, instance['exit_code'], sorted(instance))

            instance = self.__wait_for_progress_message(uuids)
            self.assertEqual(99, instance['progress'])
            self.assertEqual("We are so close!", instance['progress_message'], sorted(instance))

            cp = cli.show(uuids, self.cook_url)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn("99% We are so close!", cli.stdout(cp))

            stdout = self.__wait_for_executor_completion_message(uuids)
            self.logger.debug(f'Contents of stdout: {stdout}')
            self.assertIn("99 We are so close!", stdout)
            self.assertIn('Command exited with status 0', stdout)
            self.assertIn('Executor completed execution', stdout)

    def test_show_progress_message_custom_progress_file(self):
        executor = util.get_job_executor_type(self.cook_url)
        progress_file_env = util.retrieve_progress_file_env(self.cook_url)

        line = util.progress_line(self.cook_url, 99, 'We are so close!')
        cp, uuids = cli.submit('\'touch progress.txt && '
                               'echo "Hello World" >> progress.txt && '
                               f'echo "{line}" >> progress.txt && '
                               'echo "Done" >> progress.txt\'',
                               self.cook_url,
                               submit_flags=f'--executor {executor} '
                                            f'--env {progress_file_env}=progress.txt '
                                            f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_instance(self.cook_url, uuids[0])
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(executor, jobs[0]['instances'][0]['executor'])
        if executor == 'cook':
            instance = self.__wait_for_exit_code(uuids)
            self.assertEqual(0, instance['exit_code'], sorted(instance))

            instance = self.__wait_for_progress_message(uuids)
            self.assertEqual(99, instance['progress'])
            self.assertEqual("We are so close!", instance['progress_message'], sorted(instance))

            cp = cli.show(uuids, self.cook_url)
            self.assertEqual(0, cp.returncode, cp.stderr)

            stdout = self.__wait_for_executor_completion_message(uuids)
            self.logger.debug(f'Contents of stdout: {stdout}')
            self.assertIn('Command exited with status 0', stdout)
            self.assertIn('Executor completed execution', stdout)

    def test_submit_with_executor(self):
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--executor cook')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--executor mesos')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--executor bogus')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn("invalid choice: 'bogus'", cli.decode(cp.stderr))

    def test_kill_fails_with_duplicate_uuids(self):
        # Duplicate job and group uuid
        duplicate_uuid = uuid.uuid4()
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--uuid {duplicate_uuid} --group {duplicate_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.kill(uuids, self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Refusing to kill due to duplicate UUIDs', cli.decode(cp.stderr))
        self.assertIn('as a job', cli.decode(cp.stderr))
        self.assertIn('as a job group', cli.decode(cp.stderr))

        # Duplicate job and instance uuid
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        duplicate_uuid = jobs[0]['instances'][0]['task_id']
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--uuid {duplicate_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.kill(uuids, self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Refusing to kill due to duplicate UUIDs', cli.decode(cp.stderr))
        self.assertIn('as a job', cli.decode(cp.stderr))
        self.assertIn('as a job instance', cli.decode(cp.stderr))

        # Duplicate instance and group uuid
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        duplicate_uuid = jobs[0]['instances'][0]['task_id']
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--group {duplicate_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.kill([duplicate_uuid], self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Refusing to kill due to duplicate UUIDs', cli.decode(cp.stderr))
        self.assertIn('as a job group', cli.decode(cp.stderr))
        self.assertIn('as a job instance', cli.decode(cp.stderr))

        # Duplicate job, instance, and group uuid, with more precise check of the error message
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        instance_uuid = util.wait_for_instance(self.cook_url, uuids[0])['task_id']
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--uuid {instance_uuid} --group {instance_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.kill(uuids, self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        expected_stdout = \
            'Refusing to kill due to duplicate UUIDs.\n' \
            '\n' \
            f'{instance_uuid} is duplicated:\n' \
            f'- as a job on {self.cook_url}\n' \
            f'- as a job instance on {self.cook_url}\n' \
            f'- as a job group on {self.cook_url}\n' \
            '\n' \
            'You might need to explicitly set the cluster where you want to kill by using the --cluster flag.\n'
        self.assertEqual(expected_stdout, cli.decode(cp.stderr))

    def test_kill_job(self):
        cp, uuids = cli.submit('sleep 60', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)

        # Job should not be completed
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertNotEqual('completed', jobs[0]['status'])

        # Kill the job
        cp = cli.kill(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn(f'Killed job {uuids[0]}', cli.stdout(cp))

        # Job should now be completed
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('completed', jobs[0]['status'])

    def test_kill_instance(self):
        # Submit a job, allowing for a second try
        cp, uuids = cli.submit('sleep 60', self.cook_url, submit_flags='--max-retries 2')
        self.assertEqual(0, cp.returncode, cp.stderr)

        # Wait for an instance to appear, and kill it
        instance_uuid = util.wait_for_instance(self.cook_url, uuids[0])['task_id']
        cp = cli.kill([instance_uuid], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn(f'Killed job instance {instance_uuid}', cli.stdout(cp))

        # Wait for the second instance to appear and check their statuses
        job = util.wait_until(lambda: cli.show_jobs(uuids, self.cook_url)[1][0], lambda j: len(j['instances']) == 2)
        self.assertEqual('failed', next(i['status'] for i in job['instances'] if i['task_id'] == instance_uuid))
        self.assertIn(next(i['status'] for i in job['instances'] if i['task_id'] != instance_uuid),
                      ['running', 'unknown'])

    def test_kill_group(self):
        # Submit a group with one job
        group_uuid = uuid.uuid4()
        cp, uuids = cli.submit('sleep 60', self.cook_url, submit_flags=f'--group {group_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)

        # Job should not be completed
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertNotEqual('completed', jobs[0]['status'])

        # Kill the group
        cp = cli.kill([group_uuid], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn(f'Killed job group {group_uuid}', cli.stdout(cp))

        # Job should now be completed
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('completed', jobs[0]['status'])

    def test_kill_bogus_uuid(self):
        cp = cli.kill([uuid.uuid4()], self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(f'No matching data found', cli.stdout(cp))

    def test_kill_multiple(self):
        cp, uuids = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.kill(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn(f'Killed job {uuids[0]}', cli.stdout(cp))
        self.assertIn(f'Killed job {uuids[1]}', cli.stdout(cp))
        self.assertIn(f'Killed job {uuids[2]}', cli.stdout(cp))

    def test_submit_with_command_prefix(self):
        # Specifying command prefix
        cp, uuids = cli.submit('"exit ${FOO:-1}"', self.cook_url, submit_flags=f'--command-prefix "FOO=0; " '
                                                                               f'--name {self.current_name()}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_jobs(uuids, self.cook_url)
        self.assertEqual('FOO=0; exit ${FOO:-1}', jobs[0]['command'])
        self.assertEqual('success', jobs[0]['state'])

        # Default command prefix (empty)
        config = {'defaults': {'submit': {}}}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp, uuids = cli.submit('"exit ${FOO:-1}"', self.cook_url, flags=flags,
                                   submit_flags=f'--name {self.current_name()}')
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.wait(uuids, self.cook_url)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp, jobs = cli.show_jobs(uuids, self.cook_url)
            self.assertEqual('exit ${FOO:-1}', jobs[0]['command'])
            self.assertEqual('failed', jobs[0]['state'])

        # User-defined default
        config = {'defaults': {'submit': {'command-prefix': 'export FOO=0; '}}}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp, uuids = cli.submit('"exit ${FOO:-1}"', self.cook_url, flags=flags,
                                   submit_flags=f'--name {self.current_name()}')
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.wait(uuids, self.cook_url)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp, jobs = cli.show_jobs(uuids, self.cook_url)
            self.assertEqual('export FOO=0; exit ${FOO:-1}', jobs[0]['command'])
            self.assertEqual('success', jobs[0]['state'])

    def test_config_command_basics(self):
        config = {'defaults': {'submit': {'command-prefix': 'export FOO=0; '}}}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path

            # Get and set a valid entry
            cp = cli.config_get('defaults.submit.command-prefix', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual('export FOO=0; \n', cli.decode(cp.stdout))
            cp = cli.config_set('defaults.submit.command-prefix', '"export FOO=1; "', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.config_get('defaults.submit.command-prefix', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual('export FOO=1; \n', cli.decode(cp.stdout))

            # Get invalid entries
            cp = cli.config_get('this.is.not.present', flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Configuration entry this.is.not.present not found.', cli.decode(cp.stderr))
            cp = cli.config_get('defaults.submit.command-prefix.bogus', flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Configuration entry defaults.submit.command-prefix.bogus not found.', cli.decode(cp.stderr))
            cp = cli.config_get('defaults.submit', flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Unable to get value because defaults.submit is a configuration section.',
                          cli.decode(cp.stderr))

            # Set on a section (invalid)
            cp = cli.config_set('defaults.submit', 'bogus', flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Unable to set value because defaults.submit is a configuration section.',
                          cli.decode(cp.stderr))

    def test_config_command_advanced(self):
        config = {}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path

            # Set an entry that doesn't exist
            cp = cli.config_get('defaults.submit.command-prefix', flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Configuration entry defaults.submit.command-prefix not found.', cli.decode(cp.stderr))
            cp = cli.config_set('defaults.submit.command-prefix', '"export FOO=1; "', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.config_get('defaults.submit.command-prefix', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual('export FOO=1; \n', cli.decode(cp.stdout))

            # Set at the top level
            cp = cli.config_get('foo', flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Configuration entry foo not found.', cli.decode(cp.stderr))
            cp = cli.config_set('foo', 'bar', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.config_get('foo', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual('bar\n', cli.decode(cp.stdout))
            cp = cli.config_set('foo', 'baz', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.config_get('foo', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual('baz\n', cli.decode(cp.stdout))

            # Set non-string types
            cp = cli.config_set('int', '12345', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            with open(path) as json_file:
                self.assertEqual(12345, json.load(json_file)['int'])

            cp = cli.config_set('float', '67.89', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            with open(path) as json_file:
                self.assertEqual(67.89, json.load(json_file)['float'])

            cp = cli.config_set('bool', 'true', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            with open(path) as json_file:
                self.assertEqual(True, json.load(json_file)['bool'])

            cp = cli.config_set('bool', 'false', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            with open(path) as json_file:
                self.assertEqual(False, json.load(json_file)['bool'])

    def test_submit_with_group_name(self):
        # Group name, no group uuid
        cp, uuids = cli.submit_stdin(['ls', 'ls'], self.cook_url, submit_flags='--group-name foo')
        self.assertEqual(0, cp.returncode, cp.stderr)
        _, jobs = cli.show_jobs(uuids, self.cook_url)
        group_uuid = jobs[0]['groups'][0]
        self.assertEqual(group_uuid, jobs[1]['groups'][0])
        _, groups = cli.show_groups([group_uuid], self.cook_url)
        self.assertEqual('foo', groups[0]['name'])
        self.assertEqual(group_uuid, groups[0]['uuid'])
        self.assertEqual(sorted(uuids), sorted(groups[0]['jobs']))

        # Group name and group uuid
        group_uuid = str(uuid.uuid4())
        cp, uuids = cli.submit_stdin(['ls', 'ls'], self.cook_url, submit_flags=f'--group-name bar --group {group_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        _, groups = cli.show_groups([group_uuid], self.cook_url)
        self.assertEqual('bar', groups[0]['name'])
        self.assertEqual(group_uuid, groups[0]['uuid'])
        self.assertEqual(sorted(uuids), sorted(groups[0]['jobs']))

    def test_show_duplicate_uuid(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        _, jobs = cli.show_jobs(uuids + uuids, self.cook_url)
        self.assertEqual(1, len(jobs))
        self.assertEqual(uuids[0], jobs[0]['uuid'])

    def test_entity_refs_distinct_uuids(self):
        # Submit a job, then query using various entity ref strings
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--group-name foo')
        self.assertEqual(0, cp.returncode, cp.stderr)
        # cluster = *, type = job
        _, jobs = cli.show_jobs([f'{self.cook_url}/jobs/{uuids[0]}'], self.cook_url)
        group_uuid = jobs[0]['groups'][0]
        self.assertEqual(1, len(jobs))
        self.assertEqual(uuids[0], jobs[0]['uuid'])
        # cluster = *, type = instance
        _, instance_job_pairs = cli.show_instances([f'{self.cook_url}/instances/{uuids[0]}'], self.cook_url)
        self.assertEqual(0, len(instance_job_pairs))
        # cluster = *, type = group
        _, groups = cli.show_groups([f'{self.cook_url}/groups/{uuids[0]}'], self.cook_url)
        self.assertEqual(0, len(groups))

        # Wait for an instance to appear, then query using various entity ref strings
        instance_uuid = util.wait_for_instance(self.cook_url, uuids[0])['task_id']
        # cluster = *, type = job
        _, jobs = cli.show_jobs([f'{self.cook_url}/jobs/{instance_uuid}'], self.cook_url)
        self.assertEqual(0, len(jobs))
        # cluster = *, type = instance
        _, instance_job_pairs = cli.show_instances([f'{self.cook_url}/instances/{instance_uuid}'], self.cook_url)
        instance, _ = instance_job_pairs[0]
        self.assertEqual(1, len(instance_job_pairs))
        self.assertEqual(instance_uuid, instance['task_id'])
        # cluster = *, type = group
        _, groups = cli.show_groups([f'{self.cook_url}/groups/{instance_uuid}'], self.cook_url)
        self.assertEqual(0, len(groups))

        # Query for the job group using various entity ref strings
        # cluster = *, type = job
        _, jobs = cli.show_jobs([f'{self.cook_url}/jobs/{group_uuid}'], self.cook_url)
        self.assertEqual(0, len(jobs))
        # cluster = *, type = instance
        _, instance_job_pairs = cli.show_instances([f'{self.cook_url}/instances/{group_uuid}'], self.cook_url)
        self.assertEqual(0, len(instance_job_pairs))
        # cluster = *, type = group
        _, groups = cli.show_groups([f'{self.cook_url}/groups/{group_uuid}'], self.cook_url)
        self.assertEqual(1, len(groups))
        self.assertEqual(group_uuid, groups[0]['uuid'])

    def test_entity_refs_duplicate_uuid(self):
        # Create a job, instance, and group that share the same uuid
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        uuid = util.wait_for_instance(self.cook_url, uuids[0])['task_id']
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--uuid {uuid} --group {uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        # Query with just the uuid
        _, jobs, instance_job_pairs, groups = cli.show_all([uuid], self.cook_url)
        self.assertEqual(1, len(jobs))
        self.assertEqual(1, len(instance_job_pairs))
        self.assertEqual(1, len(groups))
        # cluster = *, type = job
        _, jobs, instance_job_pairs, groups = cli.show_all([f'{self.cook_url}/jobs/{uuid}'], self.cook_url)
        self.assertEqual(1, len(jobs))
        self.assertEqual(0, len(instance_job_pairs))
        self.assertEqual(0, len(groups))
        # cluster = *, type = instance
        _, jobs, instance_job_pairs, groups = cli.show_all([f'{self.cook_url}/instances/{uuid}'], self.cook_url)
        self.assertEqual(0, len(jobs))
        self.assertEqual(1, len(instance_job_pairs))
        self.assertEqual(0, len(groups))
        # cluster = *, type = group
        _, jobs, instance_job_pairs, groups = cli.show_all([f'{self.cook_url}/groups/{uuid}'], self.cook_url)
        self.assertEqual(0, len(jobs))
        self.assertEqual(0, len(instance_job_pairs))
        self.assertEqual(1, len(groups))

    def test_entity_refs_case_insensitive(self):
        config = {'clusters': [{'name': 'Foo', 'url': self.cook_url}]}
        with cli.temp_config_file(config) as path:
            flags = f'--config {path}'
            cp, uuids = cli.submit('ls', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            uuid = uuids[0]
            _, jobs = cli.show_jobs([f'{self.cook_url.lower()}/jobs/{uuid}'], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(uuid, jobs[0]['uuid'])
            _, jobs = cli.show_jobs([f'{self.cook_url.upper()}/jobs/{uuid}'], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(uuid, jobs[0]['uuid'])
            _, jobs = cli.show_jobs([f'{self.cook_url}/JOBS/{uuid}'], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(uuid, jobs[0]['uuid'])
            _, jobs = cli.show_jobs([f'{self.cook_url}/jobs/{uuid.upper()}'], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(uuid, jobs[0]['uuid'])

    def test_entity_refs_query_string_format(self):
        cp, uuids = cli.submit_stdin(['ls', 'ls'], self.cook_url, submit_flags='--group-name foo')
        self.assertEqual(0, cp.returncode, cp.stderr)
        _, jobs = cli.show_jobs(uuids, self.cook_url)
        group_uuid = jobs[0]['groups'][0]
        # Query for group with UUID
        _, groups = cli.show_groups([group_uuid], self.cook_url)
        self.assertEqual(sorted(uuids), sorted(groups[0]['jobs']))
        # Query for group with URL using query string argument
        _, groups = cli.show_groups([f'{self.cook_url}/groups?uuid={group_uuid}'], self.cook_url)
        self.assertEqual(sorted(uuids), sorted(groups[0]['jobs']))
        # Query for jobs with URL using query string argument
        _, jobs = cli.show_jobs([f'{self.cook_url}/jobs?uuid={uuids[0]}&uuid={uuids[1]}'], self.cook_url)
        self.assertEqual(sorted(uuids), sorted(j['uuid'] for j in jobs))
        # Query for instances with URL using query string argument
        instance_uuid_1 = util.wait_for_instance(self.cook_url, uuids[0])['task_id']
        instance_uuid_2 = util.wait_for_instance(self.cook_url, uuids[1])['task_id']
        instance_url = f'{self.cook_url}/instances?uuid={instance_uuid_1}&uuid={instance_uuid_2}'
        _, instance_job_pairs = cli.show_instances([instance_url], self.cook_url)
        self.assertIn(instance_uuid_1, (i['task_id'] for i, _ in instance_job_pairs))
        self.assertIn(instance_uuid_2, (i['task_id'] for i, _ in instance_job_pairs))

    def test_entity_refs_trailing_slash_on_cluster(self):
        config = {'clusters': [{'name': 'Foo', 'url': f'{self.cook_url}/'}]}
        with cli.temp_config_file(config) as path:
            flags = f'--config {path}'
            cp, uuids = cli.submit('ls', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            uuid = uuids[0]
            cp, jobs = cli.show_jobs([f'{self.cook_url}/jobs/{uuid}'], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            self.assertEqual(uuid, jobs[0]['uuid'])

    def test_entity_refs_as_arguments_and_stdin_not_allowed(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        stdin = cli.encode('\n'.join(uuids))
        cp = cli.kill(uuids, self.cook_url, stdin=stdin)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('You cannot supply entity references both as arguments and from stdin', cli.decode(cp.stderr))
        cp = cli.show(uuids, self.cook_url, stdin=stdin)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('You cannot supply entity references both as arguments and from stdin', cli.decode(cp.stderr))
        cp = cli.wait(uuids, self.cook_url, stdin=stdin)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('You cannot supply entity references both as arguments and from stdin', cli.decode(cp.stderr))

    def test_jobs_one_per_line(self):
        name = uuid.uuid4()
        cp, uuids = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url, submit_flags=f'--name {name}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        user = util.get_user(self.cook_url, uuids[0])
        cp = cli.jobs(self.cook_url, f'--user {user} --name {name} --all -1')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(sorted([f'{self.cook_url}/jobs/{u}' for u in uuids]), sorted(cli.stdout(cp).split('\n')))

    def test_jobs_json_and_one_per_line_mutually_exclusive(self):
        cp = cli.jobs(self.cook_url, f'--json -1')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('not allowed with argument', cli.decode(cp.stderr))

    def test_piping_from_jobs_to_kill_show_wait(self):
        name = uuid.uuid4()
        num_jobs = 101

        # Submit a batch of jobs
        cp, uuids = cli.submit_stdin(['ls'] * num_jobs, self.cook_url,
                                     submit_flags=f'--name {name} --cpus 0.01 --mem 16')
        self.assertEqual(0, cp.returncode, cp.stderr)

        # List the jobs
        user = util.get_user(self.cook_url, uuids[0])
        jobs_flags = f'--user {user} --name {name} --all --limit {num_jobs}'
        cp, jobs = cli.jobs_json(self.cook_url, jobs_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(num_jobs, len(jobs))

        # Pipe from jobs to kill
        cs = f'{cli.command()} --url {self.cook_url}'
        command = f'{cs} jobs {jobs_flags} -1 | {cs} kill'
        self.logger.info(command)
        cp = subprocess.run(command, shell=True, stdout=subprocess.PIPE)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn(f'Successful: {num_jobs}, Failed: 0', cli.stdout(cp))

        # All jobs should now be completed
        cp, jobs = cli.jobs_json(self.cook_url, f'--user {user} --name {name} --completed --limit {num_jobs}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(num_jobs, len(jobs))

        # Pipe from jobs to show
        cs = f'{cli.command()} --url {self.cook_url}'
        command = f'{cs} jobs {jobs_flags} -1 | {cs} show --json'
        self.logger.info(command)
        cp = subprocess.run(command, shell=True, stdout=subprocess.PIPE)
        self.assertEqual(0, cp.returncode, cp.stderr)
        jobs = json.loads(cli.stdout(cp))['clusters'][self.cook_url]['jobs']
        self.assertEqual(num_jobs, len(jobs))
        self.assertEqual(sorted(uuids), sorted([j['uuid'] for j in jobs]))

        # Pipe from jobs to wait
        command = f'{cs} jobs {jobs_flags} -1 | {cs} wait'
        self.logger.info(command)
        cp = subprocess.run(command, shell=True, stdout=subprocess.PIPE)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('Waiting for 100 jobs', cli.stdout(cp))
        self.assertIn(f'Waiting for {num_jobs-100} job', cli.stdout(cp))

    def test_show_non_v4_uuid(self):
        cp = cli.show(['019c34c3-13b3-b370-01a5-d1ecc9071249'], self.cook_url)
        if cp.returncode == 1:
            self.assertIn('No matching data found', cli.stdout(cp), cp.stderr)
        else:
            self.assertEqual(0, cp.returncode, cp.stderr)

    def test_cat_basic(self):
        cp, uuids = cli.submit('bash -c "for i in {1..10}; do echo $i >> foo; done"', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        # Ask for the file we wrote to
        cp = cli.cat_with_logging(uuids[0], 'foo', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('\n'.join([str(i) for i in range(1, 11)]) + '\n', cli.decode(cp.stdout))
        # Ask for a file that doesn't exist
        cp = cli.cat(uuids[0], uuid.uuid4(), self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('file was not found', cli.decode(cp.stderr))

    def test_cat_no_newlines(self):
        cp, uuids = cli.submit('bash -c \'for i in {1..100}; do printf "$i " >> foo; done\'', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.cat(uuids[0], 'foo', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(' '.join([str(i) for i in range(1, 101)]) + ' ', cli.decode(cp.stdout))

    def test_cat_large_file(self):
        iterations = 20
        cp, uuids = cli.submit('bash -c \'printf "hello\\nworld\\n" > file.txt; '
                               f'for i in {{1..{iterations}}}; do '
                               'cat file.txt file.txt > file2.txt && '
                               'mv file2.txt file.txt; done\'',
                               self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.cat(uuids[0], 'file.txt', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('hello\nworld\n' * pow(2, iterations), cli.decode(cp.stdout))

    def test_cat_large_file_no_newlines(self):
        iterations = 18
        cp, uuids = cli.submit('bash -c \'printf "helloworld" > file.txt; '
                               f'for i in {{1..{iterations}}}; do '
                               'cat file.txt file.txt > file2.txt && '
                               'mv file2.txt file.txt; done\'',
                               self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'completed')
        cp = cli.cat(uuids[0], 'file.txt', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('helloworld' * pow(2, iterations), cli.decode(cp.stdout))

    def test_cat_zero_byte_file(self):
        cp, uuids = cli.submit('touch file.txt', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.cat(uuids[0], 'file.txt', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('', cli.decode(cp.stdout))

    def test_cat_invalid_entity_ref_string(self):
        cp = cli.cat('foo', 'file.txt', self.cook_url)
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('error: argument target-entity', cli.decode(cp.stderr))

    def test_cat_empty_path(self):
        cp = cli.cat(uuid.uuid4(), '""', self.cook_url)
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('error: argument path', cli.decode(cp.stderr))

    def test_cat_group_uuid(self):
        group_uuid = uuid.uuid4()
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--group {group_uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.cat(group_uuid, 'stdout', self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stdout)
        self.assertIn('You provided a job group uuid', cli.decode(cp.stderr))

    def test_cat_bogus_uuid(self):
        bogus_uuid = uuid.uuid4()
        cp = cli.cat(bogus_uuid, 'stdout', self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stdout)
        self.assertIn('No matching data found', cli.decode(cp.stderr))

    def test_cat_job_with_no_instances(self):
        raw_job = {'command': 'ls', 'constraints': [['HOSTNAME', 'EQUALS', 'will not get scheduled']]}
        cp, uuids = cli.submit(stdin=cli.encode(json.dumps(raw_job)), cook_url=self.cook_url, submit_flags='--raw')
        self.assertEqual(0, cp.returncode, cp.stderr)
        waiting_uuid = uuids[0]
        try:
            cp = cli.cat(waiting_uuid, 'stdout', self.cook_url)
            self.assertEqual(1, cp.returncode, cp.stdout)
            self.assertIn('currently has no instances', cli.decode(cp.stderr))
        finally:
            util.kill_jobs(self.cook_url, jobs=[waiting_uuid])

    def test_cat_with_broken_pipe(self):
        iterations = 20
        cp, uuids = cli.submit('bash -c \'printf "hello\\nworld\\n" > file.txt; '
                               f'for i in {{1..{iterations}}}; do '
                               'cat file.txt file.txt > file2.txt && '
                               'mv file2.txt file.txt; done\'',
                               self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cs = f'{cli.command()} --url {self.cook_url}'
        command = f'{cs} cat {uuids[0]} file.txt | head'
        self.logger.info(command)
        cp = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('hello\nworld\n' * 5, cli.decode(cp.stdout))
        self.assertEqual('', cli.decode(cp.stderr))

    def test_cat_binary_file(self):
        cp, uuids = cli.submit('bash -c \''
                               'for i in {0..255}; do num=$(printf "%x" $i); echo -n -e "\\x$num"; done > file.bin'
                               '\'',
                               self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.cat(uuids[0], 'file.bin', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(bytes(i for i in range(0, 256)), cp.stdout)

    def test_usage(self):
        command = 'sleep 300'

        # Submit un-grouped jobs
        cp, uuids = cli.submit(command, self.cook_url, submit_flags='--cpus 0.1 --mem 16')
        self.assertEqual(0, cp.returncode, cp.stderr)
        uuid_1 = uuids[0]
        cp, uuids = cli.submit(command, self.cook_url, submit_flags='--cpus 0.1 --mem 16')
        self.assertEqual(0, cp.returncode, cp.stderr)
        uuid_2 = uuids[0]
        cp, uuids = cli.submit(command, self.cook_url, submit_flags='--cpus 0.1 --mem 16')
        self.assertEqual(0, cp.returncode, cp.stderr)
        uuid_3 = uuids[0]

        # Submit grouped jobs
        guuid_1 = uuid.uuid4()
        cp, uuids = cli.submit_stdin([command, command, command], self.cook_url,
                                     submit_flags=f'--group-name foo --group {guuid_1} --cpus 0.1 --mem 16')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(uuids))
        uuid_4, uuid_5, uuid_6 = uuids

        # Submit grouped jobs with a custom application
        guuid_2 = uuid.uuid4()
        custom_application = str(uuid.uuid4())
        cp, uuids = cli.submit_stdin([command, command, command], self.cook_url,
                                     submit_flags='--group-name qux '
                                                  f'--group {guuid_2} '
                                                  f'--application-name {custom_application} '
                                                  '--application-version does-not-matter '
                                                  '--cpus 0.1 '
                                                  '--mem 16')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(uuids))
        uuid_7, uuid_8, uuid_9 = uuids
        all_uuids = [uuid_1, uuid_2, uuid_3, uuid_4, uuid_5, uuid_6, uuid_7, uuid_8, uuid_9]
        try:
            # Wait for all jobs to be running
            util.wait_for_jobs(self.cook_url, all_uuids, 'running')

            # Invoke cs usage
            user = util.get_user(self.cook_url, uuids[0])
            cp, usage = cli.usage(user, self.cook_url)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.logger.info(f'Usage map: {json.dumps(usage, indent=2)}')
            cluster_usage = usage['clusters'][self.cook_url]
            total_usage = cluster_usage['usage']
            share = cluster_usage['share']
            applications = cluster_usage['applications']
            cs_usage = applications['cook-scheduler-cli']['usage']
            ungrouped_usage = applications['cook-scheduler-cli']['groups']['null']['usage']
            ungrouped_jobs = applications['cook-scheduler-cli']['groups']['null']['jobs']
            group_1 = f'foo ({guuid_1})'
            grouped_usage = applications['cook-scheduler-cli']['groups'][group_1]['usage']
            grouped_jobs = applications['cook-scheduler-cli']['groups'][group_1]['jobs']
            custom_application_usage = applications[custom_application]['usage']
            group_2 = f'qux ({guuid_2})'
            custom_application_grouped_usage = applications[custom_application]['groups'][group_2]['usage']
            custom_application_grouped_jobs = applications[custom_application]['groups'][group_2]['jobs']

            # Check the output data
            self.assertLessEqual(round(0.1 * 9, 1), round(total_usage['cpus'], 1))
            self.assertLessEqual(round(16 * 9, 1), round(total_usage['mem'], 1))
            self.assertLessEqual(0, total_usage['gpus'])
            self.assertLessEqual(9, usage['count'])
            self.assertLessEqual(0, share['cpus'])
            self.assertLessEqual(0, share['mem'])
            self.assertLessEqual(0, share['gpus'])
            self.assertLessEqual(round(0.1 * 6, 1), round(cs_usage['cpus'], 1))
            self.assertLessEqual(round(16 * 6, 1), round(cs_usage['mem'], 1))
            self.assertLessEqual(0, cs_usage['gpus'])
            self.assertLessEqual(round(0.1 * 3, 1), round(ungrouped_usage['cpus'], 1))
            self.assertLessEqual(round(16 * 3, 1), round(ungrouped_usage['mem'], 1))
            self.assertLessEqual(0, ungrouped_usage['gpus'])
            self.assertIn(uuid_1, ungrouped_jobs)
            self.assertIn(uuid_2, ungrouped_jobs)
            self.assertIn(uuid_3, ungrouped_jobs)
            self.assertEqual(round(0.1 * 3, 1), round(grouped_usage['cpus'], 1))
            self.assertEqual(round(16 * 3, 1), round(grouped_usage['mem'], 1))
            self.assertEqual(0, grouped_usage['gpus'])
            self.assertEqual(3, len(grouped_jobs))
            self.assertIn(uuid_4, grouped_jobs)
            self.assertIn(uuid_5, grouped_jobs)
            self.assertIn(uuid_6, grouped_jobs)
            self.assertEqual(round(0.1 * 3, 1), round(custom_application_usage['cpus'], 1))
            self.assertEqual(round(16 * 3, 1), round(custom_application_usage['mem'], 1))
            self.assertEqual(0, custom_application_usage['gpus'])
            self.assertEqual(round(0.1 * 3, 1), round(custom_application_grouped_usage['cpus'], 1))
            self.assertEqual(round(16 * 3, 1), round(custom_application_grouped_usage['mem'], 1))
            self.assertEqual(0, custom_application_grouped_usage['gpus'])
            self.assertEqual(3, len(custom_application_grouped_jobs))
            self.assertIn(uuid_7, custom_application_grouped_jobs)
            self.assertIn(uuid_8, custom_application_grouped_jobs)
            self.assertIn(uuid_9, custom_application_grouped_jobs)
        finally:
            util.kill_jobs(self.cook_url, jobs=all_uuids)

    def test_avoid_exit_on_connection_error(self):
        name = uuid.uuid4()
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--name {name}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        user = util.get_user(self.cook_url, uuids[0])
        config = {'clusters': [{'name': 'foo', 'url': self.cook_url},
                               {'name': 'bar', 'url': 'http://localhost:99999'}]}
        with cli.temp_config_file(config) as path:
            flags = f'--config {path}'
            cp, jobs = cli.jobs_json(flags=flags, jobs_flags=f'--name {name} --all --user {user}')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(uuids[0], jobs[0]['uuid'])
            self.assertIn('Encountered connection error with bar', cli.decode(cp.stderr))

    def test_submit_with_gpus(self):
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--gpus 1')
        if util.settings(self.cook_url)['mesos-gpu-enabled']:
            self.assertEqual(0, cp.returncode, cp.stderr)
        else:
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('GPU support is not enabled', cli.stdout(cp))

        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--gpus 0')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('submit: error: argument --gpus: 0 is not a positive integer', cli.decode(cp.stderr))

    def test_submit_with_pool(self):
        pools, _ = util.all_pools(self.cook_url)
        if len(pools) == 0:
            self.logger.info('There are no pools to submit jobs to')
        for pool in pools:
            self.logger.info(f'Submitting to {pool}')
            pool_name = pool['name']
            cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--pool {pool_name}')
            if pool['state'] == 'active':
                self.assertEqual(0, cp.returncode, cp.stderr)
                cp, jobs = cli.show_jobs(uuids, self.cook_url)
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertEqual(1, len(jobs))
                self.assertEqual(pool_name, jobs[0]['pool'])
            else:
                self.assertEqual(1, cp.returncode, cp.stderr)
                self.assertIn(f'{pool_name} is not accepting job submissions', cli.stdout(cp))
        # Try submitting to a pool that doesn't exist
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--pool {uuid.uuid4()}')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('is not a valid pool name', cli.stdout(cp))
