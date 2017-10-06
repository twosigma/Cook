import json
import logging
import os
import time
import unittest
import uuid
from urllib.parse import urlparse

from nose.plugins.attrib import attr

from tests.cook import cli, util


@attr(cli=True)
class CookCliTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        self.cook_url = util.retrieve_cook_url()
        self.logger = logging.getLogger(__name__)

    def test_basic_submit_and_wait(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        cp, uuids = cli.submit(cook_url=self.cook_url, stdin=cli.encode(''))
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('must specify at least one command', cli.decode(cp.stderr))

    def test_multiple_commands_submits_multiple_jobs(self):
        cp, uuids = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(uuids))
        cp, jobs = cli.show_json(uuids, self.cook_url)
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(uuids[0], jobs[0]['uuid'])
        self.assertEqual(uuids[1], jobs[1]['uuid'])
        self.assertEqual(uuids[2], jobs[2]['uuid'])
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
            cp, jobs = cli.show_json(uuids, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            # Cluster names are case insensitive
            uppercase_cluster_name = cluster_name.upper()
            self.assertNotEqual(cluster_name, uppercase_cluster_name)
            flags = f'--config {path} --cluster {uppercase_cluster_name}'
            cp, jobs = cli.show_json(uuids, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)

    def test_verbose_flag(self):
        cp, _ = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp_verbose, _ = cli.submit('ls', self.cook_url, '--verbose')
        self.assertEqual(0, cp_verbose.returncode, cp_verbose.stderr)
        self.assertTrue(len(cp_verbose.stderr) > len(cp.stderr))

    def test_usage(self):
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(command, jobs[0]['command'])
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(3, len(jobs), jobs)
        for job in jobs:
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(command, job['command'])
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('%s_job' % os.environ['USER'], jobs[0]['name'])

    def test_wait_requires_at_least_one_uuid(self):
        cp = cli.wait([], self.cook_url)
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('the following arguments are required: uuid', cli.decode(cp.stderr))

    def test_wait_specify_timeout_and_interval(self):
        cp, uuids = cli.submit('"sleep 60"', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        start_time = time.time()
        cp = cli.wait(uuids, self.cook_url, wait_flags='--timeout 1')
        elapsed_time = time.time() - start_time
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Timeout waiting', cli.decode(cp.stderr))
        self.assertLess(elapsed_time, 15)
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
        cp = cli.show([], self.cook_url)
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('the following arguments are required: uuid', cli.decode(cp.stderr))

    def test_assume_http_if_elided(self):
        url = urlparse(self.cook_url)
        url_sans_scheme = url.netloc
        cp, uuids = cli.submit('ls', url_sans_scheme)
        self.assertEqual(0, cp.returncode, cp.stderr)

    def test_double_dash_for_end_of_options(self):
        # Double-dash for 'end of options'
        cp, uuids = cli.submit('-- ls -al', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('ls -al', jobs[0]['command'])
        # Double-dash along with other flags
        cp, uuids = cli.submit('-- ls -al', self.cook_url, submit_flags='--name foo --priority 12')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('ls -al', jobs[0]['command'])
        self.assertEqual('foo', jobs[0]['name'])
        self.assertEqual(12, jobs[0]['priority'])

    def test_retries(self):
        # Default retries = 2
        cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose')
        stderr = cli.decode(cp.stderr)
        self.assertEqual(1, cp.returncode, stderr)
        self.assertIn('Starting new HTTP connection (1)', stderr)
        self.assertIn('Starting new HTTP connection (2)', stderr)
        self.assertIn('Starting new HTTP connection (3)', stderr)
        self.assertNotIn('Starting new HTTP connection (4)', stderr)
        # Set retries = 0
        config = {'http': {'retries': 0}}
        with cli.temp_config_file(config) as path:
            cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose --config %s' % path)
            stderr = cli.decode(cp.stderr)
            self.assertEqual(1, cp.returncode, stderr)
            self.assertIn('Starting new HTTP connection (1)', stderr)
            self.assertNotIn('Starting new HTTP connection (2)', stderr)
        # Set retries = 4
        config = {'http': {'retries': 4}}
        with cli.temp_config_file(config) as path:
            cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose --config %s' % path)
            stderr = cli.decode(cp.stderr)
            self.assertEqual(1, cp.returncode, stderr)
            self.assertIn('Starting new HTTP connection (1)', stderr)
            self.assertIn('Starting new HTTP connection (2)', stderr)
            self.assertIn('Starting new HTTP connection (3)', stderr)
            self.assertIn('Starting new HTTP connection (4)', stderr)
            self.assertIn('Starting new HTTP connection (5)', stderr)
            self.assertNotIn('Starting new HTTP connection (6)', stderr)

    def test_submit_priority(self):
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--priority 0')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(0, jobs[0]['priority'])
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--priority 100')
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
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
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('completed', jobs[0]['status'])
        self.assertEqual('success', jobs[0]['state'])

    def test_complex_commands(self):
        desired_command = '(foo -x \'def bar = "baz"\')'
        # Incorrectly submitted command
        command = '"(foo -x \'def bar = "baz"\')"'
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(desired_command.replace('"', ''), jobs[0]['command'])
        # Correctly submitted command
        command = '"(foo -x \'def bar = \\"baz\\"\')"'
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(desired_command, jobs[0]['command'])

        desired_command = "export HOME=$MESOS_DIRECTORY; export LOGNAME=$(whoami); JAVA_OPTS='-Xmx15000m' foo"
        # Incorrectly submitted command
        command = "'export HOME=$MESOS_DIRECTORY; export LOGNAME=$(whoami); JAVA_OPTS='-Xmx15000m' foo'"
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(desired_command.replace("'", ''), jobs[0]['command'])
        # Correctly submitted command
        command = "'export HOME=$MESOS_DIRECTORY; export LOGNAME=$(whoami); JAVA_OPTS='\"'\"'-Xmx15000m'\"'\"' foo'"
        cp, uuids = cli.submit(command, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(desired_command, jobs[0]['command'])

    def test_list_no_matching_jobs(self):
        cp = cli.list_jobs(self.cook_url, '--name %s' % uuid.uuid4())
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual('No jobs found in %s.' % self.cook_url, cli.stdout(cp))

    def list_jobs(self, name, user, *states):
        """Invokes the list subcommand with the given name, user, and state filters"""
        state_flags = ' '.join([f'--{state}' for state in states])
        cp, jobs = cli.list_jobs_json(self.cook_url, '--name %s --user %s %s' % (name, user, state_flags))
        return cp, jobs

    def test_list_by_state(self):
        name = str(uuid.uuid4())
        # waiting
        raw_job = {'command': 'ls', 'name': name, 'constraints': [['HOSTNAME', 'EQUALS', 'will not get scheduled']]}
        cp, uuids = cli.submit(stdin=cli.encode(json.dumps(raw_job)), cook_url=self.cook_url, submit_flags='--raw')
        user = util.get_user(self.cook_url, uuids[0])
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'waiting')
        cp, jobs = self.list_jobs(name, user, 'waiting')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertEqual(uuids[0], jobs[0]['uuid'])
        waiting_uuid = uuids[0]
        # running
        cp, uuids = cli.submit('sleep 120', self.cook_url, submit_flags='--name %s' % name)
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'running')
        cp, jobs = self.list_jobs(name, user, 'running')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertEqual(uuids[0], jobs[0]['uuid'])
        running_uuid = uuids[0]
        # completed
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags='--name %s' % name)
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'completed')
        cp, jobs = self.list_jobs(name, user, 'completed')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertEqual(uuids[0], jobs[0]['uuid'])
        # success
        cp, jobs = self.list_jobs(name, user, 'success')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertEqual(uuids[0], jobs[0]['uuid'])
        success_uuid = uuids[0]
        # failed
        cp, uuids = cli.submit('exit 1', self.cook_url, submit_flags='--name %s' % name)
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'completed')
        cp, jobs = self.list_jobs(name, user, 'failed')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertEqual(uuids[0], jobs[0]['uuid'])
        failed_uuid = uuids[0]
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

    def test_list_invalid_state(self):
        cp = cli.list_jobs(self.cook_url, '--foo')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('error: unrecognized arguments: --foo', cli.decode(cp.stderr))

    def test_submit_with_application(self):
        # Specifying application name and version
        cp, uuids = cli.submit('ls', self.cook_url,
                               submit_flags='--application-name %s --application-version %s' % ('foo', '1.2.3'))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual('foo', jobs[0]['application']['name'])
        self.assertEqual('1.2.3', jobs[0]['application']['version'])
        # Default application name
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual('cook-scheduler-cli', jobs[0]['application']['name'])

    def test_read_timeout_messaging(self):
        config = {'http': {'read-timeout': 0.0001}}
        with cli.temp_config_file(config) as path:
            cp = cli.list_jobs(self.cook_url, flags=f'--config {path} --verbose')
            self.assertEqual(1, cp.returncode, cli.stdout(cp))
            self.assertIn('Encountered connection error', cli.decode(cp.stderr))
            cp, uuids = cli.submit('ls', self.cook_url, flags=f'--config {path} --verbose')
            self.assertEqual(1, cp.returncode)
            self.assertIn('Encountered read timeout', cli.stdout(cp))
            self.assertIn('Your submission may have completed', cli.stdout(cp))

    def test_list_invalid_flags(self):
        error_fragment = 'cannot specify both lookback hours and submitted after / before times'
        cp = cli.list_jobs(self.cook_url, '--lookback 1 --submitted-after "yesterday" --submitted-before "now"')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(error_fragment, cli.decode(cp.stderr))
        cp = cli.list_jobs(self.cook_url, '--lookback 1 --submitted-after "yesterday"')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(error_fragment, cli.decode(cp.stderr))
        cp = cli.list_jobs(self.cook_url, '--lookback 1 --submitted-before "now"')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(error_fragment, cli.decode(cp.stderr))

    def test_list_with_time_ranges(self):
        name = str(uuid.uuid4())
        cp, uuids = cli.submit('ls', self.cook_url, submit_flags=f'--name {name}')
        self.assertEqual(0, cp.returncode, cp.stderr)
        user = util.get_user(self.cook_url, uuids[0])
        # Time range that should be empty
        list_flags = f'--submitted-after "30 minutes ago" --submitted-before "15 minutes ago" ' \
                     f'--user {user} --all --name {name}'
        cp, jobs = cli.list_jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(0, len(jobs))
        # Time range that should contain our job (wait for job to complete to avoid racing with the "now" query)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        list_flags = f'--submitted-after "30 minutes ago" --submitted-before "now" --user {user} --all --name {name}'
        cp, jobs = cli.list_jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertIn(uuids[0], jobs[0]['uuid'])
        # --submitted-after is not required
        list_flags = f'--submitted-before "now" --user {user} --all --name {name}'
        cp, jobs = cli.list_jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertIn(uuids[0], jobs[0]['uuid'])
        # --submitted-before is not required
        list_flags = f'--submitted-after "15 minutes ago" --user {user} --all --name {name}'
        cp, jobs = cli.list_jobs_json(self.cook_url, list_flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(1, len(jobs))
        self.assertIn(uuids[0], jobs[0]['uuid'])
