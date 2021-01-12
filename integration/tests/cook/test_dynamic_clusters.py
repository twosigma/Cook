import logging
import os
import time
import unittest

import pytest

from tests.cook import util


@pytest.mark.prodskip
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class TestDynamicClusters(util.CookTest):

    @classmethod
    def setUpClass(cls):
        cls.cook_url = util.retrieve_cook_url()
        util.init_cook_session(cls.cook_url)

    def setUp(self):
        self.cook_url = type(self).cook_url
        self.logger = logging.getLogger(__name__)
        self.user_factory = util.UserFactory(self)

    @unittest.skipUnless(util.using_kubernetes(), 'Test requires kubernetes')
    @unittest.skipUnless(os.getenv('COOK_TEST_DYNAMIC_CLUSTERS') is not None,
                         'Requires setting the COOK_TEST_DYNAMIC_CLUSTERS environment variable')
    @pytest.mark.serial
    def test_dynamic_clusters(self):
        """
        Test that dynamic cluster configuration functionality is working.
        """
        docker_image = util.docker_image()
        container = {'type': 'docker',
                     'docker': {'image': docker_image}}
        admin = self.user_factory.admin()
        # Force all clusters to have state = deleted via the API
        clusters = [cluster for cluster in util.compute_clusters(self.cook_url)['db-configs'] if cluster["state"] == "running"]
        with admin:
            self.logger.info(f'Clusters {clusters}')
            # First state = draining
            for cluster in clusters:
                cluster["state"] = "draining"
                cluster["state-locked?"] = True
                self.logger.info(f'Trying to update cluster {cluster}')
                data, resp = util.update_compute_cluster(self.cook_url, cluster)
                self.assertEqual(201, resp.status_code, resp.content)
            # Then state = deleted
            for cluster in clusters:
                cluster["state"] = "deleted"
                util.wait_until(lambda: util.update_compute_cluster(self.cook_url, cluster),
                                lambda x: 201 == x[1].status_code,
                                300000, 5000)
            # Create at least one new cluster with a unique test name (using one of the existing cluster's IP and cert)
            test_cluster_name = f'test_cluster_{round(time.time() * 1000)}'
            test_cluster = {
                "name": test_cluster_name,
                "state": "running",
                "base-path": clusters[0]["base-path"],
                "ca-cert": clusters[0]["ca-cert"],
                "template": clusters[0]["template"]
            }
            data, resp = util.create_compute_cluster(self.cook_url, test_cluster)
            self.assertEqual(201, resp.status_code, resp.content)
            # Test create cluster with duplicate name
            data, resp = util.create_compute_cluster(self.cook_url, test_cluster)
            self.assertEqual(422, resp.status_code, resp.content)
            self.assertEqual(f'Compute cluster with name {test_cluster_name} already exists',
                             data['error']['message'],
                             resp.content)

        # Check that a job schedules successfully
        command = "true"
        job_uuid, resp = util.submit_job(self.cook_url, command=command, container=container)
        self.assertEqual(201, resp.status_code, resp.content)
        instance = util.wait_for_instance(self.cook_url, job_uuid)
        message = repr(instance)
        self.assertIsNotNone(instance['compute-cluster'], message)
        instance_compute_cluster_name = instance['compute-cluster']['name']
        self.assertEqual(test_cluster["name"], instance_compute_cluster_name, instance['compute-cluster'])
        util.wait_for_instance(self.cook_url, job_uuid, status='success')
        running_clusters = [cluster for cluster in util.compute_clusters(self.cook_url)['db-configs'] if cluster["state"] == "running"]
        self.assertEqual(1, len(running_clusters), running_clusters)
        self.assertEqual(test_cluster["name"], running_clusters[0]["name"], running_clusters)


        with admin:
            # Delete test cluster
            # First state = draining
            test_cluster["state"] = "draining"
            data, resp = util.update_compute_cluster(self.cook_url, test_cluster)
            self.assertEqual(201, resp.status_code, resp.content)
            # Then state = deleted
            test_cluster["state"] = "deleted"
            util.wait_until(lambda: util.update_compute_cluster(self.cook_url, test_cluster),
                            lambda x: 201 == x[1].status_code,
                            300000, 5000)
            # Hard-delete the original non-test clusters
            for cluster in clusters:
                self.logger.info(f'Trying to delete cluster {cluster}')
                resp = util.delete_compute_cluster(self.cook_url, cluster)
                self.assertEqual(204, resp.status_code, resp.content)
            # Force give up leadership
            resp = util.shutdown_leader(self.cook_url, "test_dynamic_clusters")
            self.assertEqual(b'Accepted', resp)

        # Old clusters should be re-created
        # wait for cook to come up
        util.wait_until(lambda: [cluster for cluster in util.compute_clusters(self.cook_url)['db-configs'] if cluster["state"] == "running"],
                        lambda x: len(x) == len(clusters),
                        420000, 5000)
        # Check that a job schedules successfully
        command = "true"
        job_uuid, resp = util.submit_job(self.cook_url, command=command, container=container)
        self.assertEqual(201, resp.status_code, resp.content)
        util.wait_for_instance(self.cook_url, job_uuid, status='success')

        with admin:
            # Hard-delete test cluster
            resp = util.delete_compute_cluster(self.cook_url, test_cluster)
            self.assertEqual(204, resp.status_code, resp.content)

    @unittest.skipUnless(util.using_kubernetes(), 'Test requires kubernetes')
    def test_checkpoint_locality(self):
        """
        Test that restored instances run in the same location as their checkpointed instances.
        """
        # Get the set of clusters that correspond to the pool under test and are running
        pool = util.default_submit_pool()
        clusters = util.compute_clusters(self.cook_url)
        running_clusters = [c for c in clusters['in-mem-configs']
                            if pool in c['cluster-definition']['config']['synthetic-pods']['pools']
                            and c['state'] == 'running']
        self.logger.info(f'Running clusters for pool {pool}: {running_clusters}')
        if len(running_clusters) == 0:
            self.skipTest(f'Requires at least 1 running compute cluster for pool {pool}')

        # Submit an initial canary job
        job_uuid, resp = util.submit_job(self.cook_url, pool=pool, command='true')
        self.assertEqual(201, resp.status_code, resp.content)
        util.wait_for_instance(self.cook_url, job_uuid, status='success', indent=None)

        # Submit a long-running job with checkpointing
        checkpoint_job_uuid, resp = util.submit_job(
            self.cook_url,
            pool=pool,
            command=f'sleep {util.DEFAULT_TEST_TIMEOUT_SECS}',
            max_retries=5,
            checkpoint={'mode': 'auto'})
        self.assertEqual(201, resp.status_code, resp.content)

        try:
            # Wait for the job to be running
            checkpoint_instance = util.wait_for_instance(
                self.cook_url,
                checkpoint_job_uuid,
                status='running',
                indent=None)
            checkpoint_instance_uuid = checkpoint_instance['task_id']
            checkpoint_location = next(c['location'] for c in running_clusters
                                       if c['name'] == checkpoint_instance['compute-cluster']['name'])

            admin = self.user_factory.admin()
            try:
                # Force all clusters in the instance's location to have state = draining
                with admin:
                    for cluster in running_clusters:
                        if cluster['location'] == checkpoint_location:
                            cluster_update = dict(cluster)
                            # Set state = draining
                            cluster_update['state'] = 'draining'
                            cluster_update['state-locked?'] = True
                            # The location and cluster-definition fields cannot be sent in the update
                            cluster_update.pop('location', None)
                            cluster_update.pop('cluster-definition', None)
                            self.logger.info(f'Trying to update cluster to draining: {cluster_update}')
                            util.wait_until(
                                lambda: util.update_compute_cluster(self.cook_url, cluster_update)[1],
                                lambda response: response.status_code == 201 and len(response.json()) > 0)
                        else:
                            self.logger.info(f'Not updating cluster - not in location {checkpoint_location}: {cluster}')

                # Kill the running checkpoint job instance
                util.kill_instance(self.cook_url, checkpoint_instance_uuid)

                # Submit another canary job
                job_uuid, resp = util.submit_job(self.cook_url, pool=pool, command='true')
                self.assertEqual(201, resp.status_code, resp.content)

                cluster_locations = set(c['location'] for c in running_clusters)
                if len(cluster_locations) > 1:
                    # The canary job should run in the non-draining location
                    self.logger.info(f'There are > 1 cluster locations under test: {cluster_locations}')
                    util.wait_for_instance(self.cook_url, job_uuid, status='success', indent=None)
                else:
                    self.logger.info(f'There is only 1 cluster location under test: {cluster_locations}')

                # The checkpoint job should be waiting
                util.wait_for_instance(self.cook_url, checkpoint_job_uuid, status='failed', indent=None)
                util.wait_for_job_in_statuses(self.cook_url, checkpoint_job_uuid, ['waiting'])
            finally:
                # Revert all clusters in the instance's location to state = running
                with admin:
                    for cluster in running_clusters:
                        if cluster['location'] == checkpoint_location:
                            cluster_update = dict(cluster)
                            # Set state = running
                            cluster_update['state'] = 'running'
                            cluster_update['state-locked?'] = False
                            # The location and cluster-definition fields cannot be sent in the update
                            cluster_update.pop('location', None)
                            cluster_update.pop('cluster-definition', None)
                            self.logger.info(f'Trying to update cluster to running: {cluster_update}')
                            util.wait_until(
                                lambda: util.update_compute_cluster(self.cook_url, cluster_update)[1],
                                lambda response: response.status_code == 201 and len(response.json()) > 0)
                        else:
                            self.logger.info(f'Not updating cluster - not in location {checkpoint_location}: {cluster}')

                # Wait for the checkpoint job to be running again, in the same location as before
                checkpoint_instance = util.wait_for_instance(
                    self.cook_url,
                    checkpoint_job_uuid,
                    status='running',
                    indent=None)
                self.assertEqual(checkpoint_location,
                                 next(c['location'] for c in running_clusters
                                      if c['name'] == checkpoint_instance['compute-cluster']['name']))
        finally:
            # Kill the checkpoint job to not leave it running
            util.kill_jobs(self.cook_url, [checkpoint_job_uuid])
