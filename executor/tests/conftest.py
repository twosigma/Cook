# This file is automatically loaded and run by pytest during its setup process,
# meaning it happens before any of the tests in this directory are run.
# See the pytest documentation on conftest files for more information:
# https://docs.pytest.org/en/2.7.3/plugins.html#conftest-py-plugins

# Please see: https://github.com/twosigma/Cook/issues/749

import pymesos as pm

pm.encode_data((str({'foo': 'bar'}).encode('utf8')))
