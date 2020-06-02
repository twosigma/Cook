# Copyright (c) Two Sigma Open Source, LLC
#
# Licensed under the Apache license, Version 2.0 (the "License");
# you may not use this file ecept in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import TestCase

from cook.scheduler.client.util import Dataset, FetchableUri

URI_DICT_NO_OPTIONALS = {'value': 'localhost/resource1'}

URI_DICT_WITH_OPTIONALS = {
    'value': 'localhost/resource2',
    'cache': True,
    'extract': False,
    'executable': True
}

URI_EXAMPLE = FetchableUri(
    'localhost/resource2',
    cache=True,
    extract=False,
    executable=True
)

DATASET_DICT_NO_OPTIONALS = {
    'dataset': {
        'key': 'value',
        'key2': 'value'
    }
}

DATASET_DICT_WITH_OPTIONALS = {**DATASET_DICT_NO_OPTIONALS, **{
    'partitions': [
        {
            'from': '20190201',
            'to': '20190301'
        },
        {
            'from': '20200201',
            'to': '20200301'
        },
    ]
}}

DATASET_EXAMPLE = Dataset(
    dataset={
        'key': 'value',
        'key2': 'value'
    },
    partitions=[
        {
            'from': '20190201',
            'to': '20190301'
        },
        {
            'from': '20200201',
            'to': '20200301'
        },
    ]
)


class FetchableUriTest(TestCase):
    def _check_required_fields(self, uri: FetchableUri, uridict: dict):
        self.assertEqual(uri.value, uridict['value'])

    def _check_optional_fields(self, uri: FetchableUri, uridict: dict):
        self.assertEqual(uri.cache, uridict['cache'])
        self.assertEqual(uri.extract, uridict['extract'])
        self.assertEqual(uri.executable, uridict['executable'])

    def test_dict_parse_required(self):
        uridict = URI_DICT_NO_OPTIONALS
        uri = FetchableUri.from_dict(uridict)
        self._check_required_fields(uri, uridict)

    def test_dict_parse_optional(self):
        uridict = URI_DICT_WITH_OPTIONALS
        uri = FetchableUri.from_dict(uridict)
        self._check_optional_fields(uri, uridict)

    def test_dict_output(self):
        uri = URI_EXAMPLE
        uridict = uri.to_dict()
        self._check_required_fields(uri, uridict)
        self._check_optional_fields(uri, uridict)


class DatasetTest(TestCase):
    def _check_required_fields(self, ds: Dataset, dsdict: dict):
        self.assertEqual(ds.dataset, dsdict['dataset'])

    def _check_optional_fields(self, ds: Dataset, dsdict: dict):
        self.assertEqual(ds.partitions, dsdict['partitions'])

    def test_dict_parse_required(self):
        dsdict = DATASET_DICT_NO_OPTIONALS
        ds = Dataset.from_dict(dsdict)
        self._check_required_fields(ds, dsdict)

    def test_dict_parse_optional(self):
        dsdict = DATASET_DICT_WITH_OPTIONALS
        ds = Dataset.from_dict(dsdict)
        self._check_optional_fields(ds, dsdict)

    def test_dict_output(self):
        ds = DATASET_EXAMPLE
        dsdict = ds.to_dict()
        self._check_required_fields(ds, dsdict)
        self._check_optional_fields(ds, dsdict)
