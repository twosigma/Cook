import logging
import os
import subprocess
import time
import unittest

import pytest

from tests.cook import util


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
