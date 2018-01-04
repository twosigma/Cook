import logging
import os
import pytest
import subprocess
import unittest
import uuid

from urllib.parse import urlparse

from tests.cook import cli, util


@pytest.mark.cli
@unittest.skipUnless(util.multi_cluster_tests_enabled(),
                     'Requires setting the COOK_MULTI_CLUSTER environment variable')
@pytest.mark.timeout(600)  # no individual test exceeds 10 minutes
class MultiCookCliTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        self.cook_url_1 = util.retrieve_cook_url()
        self.cook_url_2 = util.retrieve_cook_url('COOK_SCHEDULER_URL_2', 'http://localhost:22321')
        self.logger = logging.getLogger(__name__)
        util.wait_for_cook(self.cook_url_1)
        util.wait_for_cook(self.cook_url_2)

    def __two_cluster_config(self):
        return {'clusters': [{'name': 'cook1', 'url': self.cook_url_1},
                             {'name': 'cook2', 'url': self.cook_url_2}]}

    def test_federated_show(self):
        # Submit to cluster #1
        cp, uuids = cli.submit('ls', self.cook_url_1)
        self.assertEqual(0, cp.returncode, cp.stderr)
        uuid_1 = uuids[0]

        # Submit to cluster #2
        cp, uuids = cli.submit('ls', self.cook_url_2)
        self.assertEqual(0, cp.returncode, cp.stderr)
        uuid_2 = uuids[0]

        # Single query for both jobs, federated across clusters
        config = self.__two_cluster_config()
        with cli.temp_config_file(config) as path:
            cp, jobs = cli.show_jobs([uuid_1, uuid_2], flags='--config %s' % path)
            uuids = [job['uuid'] for job in jobs]
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(2, len(jobs), jobs)
            self.assertIn(str(uuid_1), uuids)
            self.assertIn(str(uuid_2), uuids)

    def test_ssh(self):
        # Submit to cluster #2
        cp, uuids = cli.submit('ls', self.cook_url_2)
        self.assertEqual(0, cp.returncode, cp.stderr)
        instance = util.wait_for_instance(self.cook_url_2, uuids[0])

        # Run ssh for the submitted job, with both clusters configured
        config = self.__two_cluster_config()
        with cli.temp_config_file(config) as path:
            hostname = instance['hostname']
            env = os.environ
            env['CS_SSH'] = 'echo'
            cp = cli.ssh(uuids[0], env=env, flags=f'--config {path}')
            stdout = cli.stdout(cp)
            self.assertEqual(0, cp.returncode, cli.output(cp))
            self.assertIn(f'Attempting ssh for job instance {instance["task_id"]}', stdout)
            self.assertIn('Executing ssh', stdout)
            self.assertIn(hostname, stdout)
            self.assertIn(f'-t {hostname} cd', stdout)
            self.assertIn('; bash', stdout)

    def test_entity_ref_support(self):
        # Submit to cluster #1
        cp, uuids = cli.submit('job1', self.cook_url_1)
        self.assertEqual(0, cp.returncode, cp.stderr)
        uuid = uuids[0]

        # Submit to cluster #2 with the same uuid
        cp, uuids = cli.submit('job2', self.cook_url_2, submit_flags=f'--uuid {uuid}')
        self.assertEqual(0, cp.returncode, cp.stderr)

        config = self.__two_cluster_config()
        with cli.temp_config_file(config) as path:
            flags = f'--config {path}'
            # Query for both jobs with uuid
            cp, jobs = cli.show_jobs([uuid], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(2, len(jobs), jobs)
            self.assertEqual(uuid, jobs[0]['uuid'])
            self.assertEqual(uuid, jobs[1]['uuid'])
            # Query cook1 with entity ref
            cp, jobs = cli.show_jobs([f'{self.cook_url_1}/jobs/{uuid}'], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs), jobs)
            self.assertEqual(uuid, jobs[0]['uuid'])
            self.assertEqual('job1', jobs[0]['command'])
            # Query cook2 with entity ref
            cp, jobs = cli.show_jobs([f'{self.cook_url_2}/jobs/{uuid}'], flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs), jobs)
            self.assertEqual(uuid, jobs[0]['uuid'])
            self.assertEqual('job2', jobs[0]['command'])

    def test_no_matching_data_error_shows_only_cluster_of_interest(self):
        name = uuid.uuid4()
        config = {'clusters': [{'name': 'FOO', 'url': f'{self.cook_url_1}'},
                               {'name': 'BAR', 'url': f'{self.cook_url_2}'}]}
        with cli.temp_config_file(config) as path:
            flags = f'--config {path}'
            cp, uuids = cli.submit('ls', flags=flags, submit_flags=f'--name {name}')
            self.assertEqual(0, cp.returncode, cp.stderr)
            user = util.get_user(self.cook_url_1, uuids[0])
            jobs_flags = f'--user {user} --name {name} --all'
            cp, jobs = cli.jobs_json(self.cook_url_1, jobs_flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(jobs))
            cs = f'{cli.command()} {flags}'
            netloc_1 = urlparse(self.cook_url_1).netloc
            netloc_2 = urlparse(self.cook_url_2).netloc
            command = f'{cs} jobs {jobs_flags} -1 | sed "s/{netloc_1}/{netloc_2}/" | {cs} show'
            self.logger.info(command)
            cp = subprocess.run(command, shell=True, stdout=subprocess.PIPE)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('No matching data found in BAR.\nDo you need to add another cluster', cli.stdout(cp))
