import datetime
import logging
import unittest

import pytz
from dateutil.tz import tzlocal, tz
from freezegun import freeze_time
from nose.plugins.attrib import attr

from cook import dateparser


@attr(cli=True)
class CookCliTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        self.logger = logging.getLogger(__name__)

    def test_parse(self):
        time_zone = pytz.utc
        now = datetime.datetime.now(time_zone)
        with freeze_time(now):
            self.assertEqual(now, dateparser.parse('now', time_zone))
            self.assertEqual(now, dateparser.parse('NOW', time_zone))
            self.assertEqual(now, dateparser.parse('today', time_zone))
            self.assertEqual(now, dateparser.parse('TODAY', time_zone))
            self.assertEqual(now - datetime.timedelta(1), dateparser.parse('yesterday', time_zone))
            self.assertEqual(now - datetime.timedelta(1), dateparser.parse('YESTERDAY', time_zone))
            self.assertEqual(now - datetime.timedelta(seconds=1), dateparser.parse('1 sec ago', time_zone))
            self.assertEqual(now - datetime.timedelta(seconds=1), dateparser.parse('1 second ago', time_zone))
            self.assertEqual(now - datetime.timedelta(seconds=2), dateparser.parse('2 seconds ago', time_zone))
            self.assertEqual(now - datetime.timedelta(minutes=1), dateparser.parse('1 min ago', time_zone))
            self.assertEqual(now - datetime.timedelta(minutes=1), dateparser.parse('1 minute ago', time_zone))
            self.assertEqual(now - datetime.timedelta(minutes=2), dateparser.parse('2 minutes ago', time_zone))
            self.assertEqual(now - datetime.timedelta(hours=1), dateparser.parse('1 hour ago', time_zone))
            self.assertEqual(now - datetime.timedelta(hours=2), dateparser.parse('2 hours ago', time_zone))
            self.assertEqual(now - datetime.timedelta(days=1), dateparser.parse('1 day ago', time_zone))
            self.assertEqual(now - datetime.timedelta(days=2), dateparser.parse('2 days ago', time_zone))
            self.assertEqual(now - datetime.timedelta(weeks=1), dateparser.parse('1 week ago', time_zone))
            self.assertEqual(now - datetime.timedelta(weeks=2), dateparser.parse('2 weeks ago', time_zone))
            self.assertEqual(datetime.datetime(2017, 10, 5, tzinfo=time_zone),
                             dateparser.parse('2017-10-05', time_zone))
            self.assertEqual(datetime.datetime(2017, 10, 5, 16, 36, tzinfo=time_zone),
                             dateparser.parse('2017-10-05 16:36', time_zone))
            self.assertEqual(datetime.datetime(2017, 10, 6, 16, 37, 59, tzinfo=tzlocal()),
                             dateparser.parse('Fri Oct  6 16:37:59 CDT 2017', time_zone))
            self.assertEqual(datetime.datetime(2003, 9, 25, 10, 49, 41,
                                               tzinfo=tz.tzoffset(None, datetime.timedelta(hours=-3))),
                             dateparser.parse('Thu, 25 Sep 2003 10:49:41 -0300', time_zone))
