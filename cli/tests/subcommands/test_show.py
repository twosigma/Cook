import logging
import unittest
import uuid

from nose.plugins.attrib import attr

from cook.subcommands.show import query_cluster


@attr(cli=True)
class CookCliTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        self.logger = logging.getLogger(__name__)

    def test_query_cluster(self):
        cluster = {}
        uuids = [uuid.uuid4()]

        def make_job_request(_, __):
            return {}

        query_cluster(cluster, uuids, None, None, None, make_job_request, 'job')
