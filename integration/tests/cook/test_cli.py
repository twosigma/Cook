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
        self.assertEqual('', cli.stdout(cp))
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
        self.assertIn('cannot specify multiple commands with a single UUID', cli.decode(cp.stderr))

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
        config = {'clusters': [{'name': 'foo', 'url': self.cook_url}],
                  'defaults': {'submit': {'mem': 256, 'cpus': 2, 'max-retries': 2}}}
        with cli.temp_config_file(config) as path:
            flags = '--config %s --cluster foo' % path
            cp, uuids = cli.submit('ls', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp, jobs = cli.show_json(uuids, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.wait(uuids, flags=flags)
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

    def test_default_cluster(self):
        config = {'clusters': [{'name': 'foo', 'url': self.cook_url}],
                  'defaults': {'submit': {'mem': 256, 'cpus': 2, 'max-retries': 2},
                               'cluster': 'foo'}}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp, uuids = cli.submit('ls', flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp, jobs = cli.show_json(uuids, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            cp = cli.wait(uuids, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)

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
        name = jobs[0]['name']
        name_parts = name.split('_')
        self.assertEqual(os.environ['USER'], name_parts[0], name)
        self.assertTrue(util.is_valid_uuid(name_parts[1]), name)

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
        self.assertIn('Timeout waiting for jobs', cli.decode(cp.stderr))
        self.assertLess(elapsed_time, 15)
        self.assertGreater(elapsed_time, 3)
        start_time = time.time()
        cp = cli.wait(uuids, self.cook_url, wait_flags='--timeout 1 --interval 1')
        elapsed_time_2 = time.time() - start_time
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Timeout waiting for jobs', cli.decode(cp.stderr))
        self.assertLess(elapsed_time_2, 3)

    def test_query_invalid_uuid(self):
        cp = cli.show([uuid.uuid4()], self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Unable to query job(s)', cli.decode(cp.stderr))
        cp = cli.wait([uuid.uuid4()], self.cook_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Unable to query job(s)', cli.decode(cp.stderr))

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
        cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose --retries 0')
        stderr = cli.decode(cp.stderr)
        self.assertEqual(1, cp.returncode, stderr)
        self.assertIn('Starting new HTTP connection (1)', stderr)
        self.assertNotIn('Starting new HTTP connection (2)', stderr)
        # Set retries = 4
        cp, uuids = cli.submit('-- ls -al', 'localhost:99999', '--verbose --retries 4')
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

    def test_default_max_runtime_displays_as_none(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'max_runtime\s+none')

    def test_submit_output_should_explain_what_happened(self):
        cp, _ = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn("Job submitted successfully", cli.stdout(cp))
        self.assertIn("Your job's UUID is", cli.stdout(cp))
        cp, _ = cli.submit_stdin(['ls', 'ls', 'ls'], self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn("Jobs submitted successfully", cli.stdout(cp))
        self.assertIn("Your jobs' UUIDs are", cli.stdout(cp))

    def test_submit_raw_should_error_if_command_is_given(self):
        cp, _ = cli.submit('ls', self.cook_url, submit_flags='--raw')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('cannot specify a command at the command line when using --raw/-r', cli.decode(cp.stderr))

    def test_env_should_be_nicely_formatted(self):
        # Empty environment
        raw_job = {'command': 'ls', 'env': {}}
        cp, uuids = cli.submit(cook_url=self.cook_url, submit_flags='--raw', stdin=cli.encode(json.dumps(raw_job)))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'env\s+\(empty\)')
        # Non-empty environment
        raw_job = {'command': 'ls', 'env': {'FOO': '1', 'BAR': '2'}}
        cp, uuids = cli.submit(cook_url=self.cook_url, submit_flags='--raw', stdin=cli.encode(json.dumps(raw_job)))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'env\s+BAR=2 FOO=1')

    def test_labels_should_be_nicely_formatted(self):
        # Empty labels
        raw_job = {'command': 'ls', 'labels': {}}
        cp, uuids = cli.submit(cook_url=self.cook_url, submit_flags='--raw', stdin=cli.encode(json.dumps(raw_job)))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'labels\s+\(empty\)')
        # Non-empty labels
        raw_job = {'command': 'ls', 'labels': {'FOO': '1', 'BAR': '2'}}
        cp, uuids = cli.submit(cook_url=self.cook_url, submit_flags='--raw', stdin=cli.encode(json.dumps(raw_job)))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'labels\s+BAR=2 FOO=1')

    def test_uris_should_be_nicely_formatted(self):
        # Empty uris
        raw_job = {'command': 'ls', 'uris': []}
        cp, uuids = cli.submit(cook_url=self.cook_url, submit_flags='--raw', stdin=cli.encode(json.dumps(raw_job)))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'uris\s+\(empty\)')
        # Non-empty uris
        raw_job = {'command': 'ls', 'uris': [{'value': 'foo'}, {'value': 'bar'}, {'value': 'baz'}]}
        cp, uuids = cli.submit(cook_url=self.cook_url, submit_flags='--raw', stdin=cli.encode(json.dumps(raw_job)))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'uris\s+.*cache=False executable=False extract=False value=foo')
        self.assertRegex(cli.stdout(cp), 'uris\s+.*cache=False executable=False extract=False value=bar')
        self.assertRegex(cli.stdout(cp), 'uris\s+.*cache=False executable=False extract=False value=baz')

    def test_show_instances_flag(self):
        cp, uuids = cli.submit('ls', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.wait(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'Instances:\\n\\ntask_id\s+status\s+start_time\s+end_time\s*\\n')
        cp = cli.show(uuids, self.cook_url, show_flags='--instances')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertRegex(cli.stdout(cp), 'Instances:\\n\\ntask_id\s+status\s+start_time\s+end_time\s+mesos_start_time'
                                         '\s+slave_id\s+executor_id\s+hostname\s+ports\s+backfilled\s+preempted\s*\\n')

    def test_show_running_job(self):
        cp, uuids = cli.submit('sleep 60', self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        util.wait_for_job(self.cook_url, uuids[0], 'running')
        cp = cli.show(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)

    def test_quoting(self):
        command = 'printf "Surname: %s\nName: %s\n" "$SURNAME" "$FIRSTNAME"'
        raw_job = {'command': command, 'env': {'SURNAME': 'Bar', 'FIRSTNAME': 'Foo'}}
        cp, uuids = cli.submit(cook_url=self.cook_url, submit_flags='--raw', stdin=cli.encode(json.dumps(raw_job)))
        self.assertEqual(0, cp.returncode, cp.stderr)
        cp, jobs = cli.show_json(uuids, self.cook_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(command, jobs[0]['command'])
