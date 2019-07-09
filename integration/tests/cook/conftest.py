# This file is automatically loaded and run by pytest during its setup process,
# meaning it happens before any of the tests in this directory are run.
# See the pytest documentation on conftest files for more information:
# https://docs.pytest.org/en/2.7.3/plugins.html#conftest-py-plugins
import datetime
import getpass
import json
import logging
import os
import socket
import subprocess
import threading
import time
import uuid

import pytest
from pygit2 import Repository

from tests.cook import util


def _sudo_check(user):
    """	
    Check if the current user can sudo as a test user.	
    This is necessary to obtain Kerberos auth headers for multi-user tests.	
    """
    sudo_ok = (0 == subprocess.call(f'sudo -nu {user} echo CACHED SUDO', shell=True))
    assert sudo_ok, "You need to pre-cache your sudo credentials. (Run a simple sudo command as a test user.)"


def _sudo_checker_task(user):
    """Periodically check sudo ability to ensure the credentials stay cached."""
    while True:
        _sudo_check(user)
        time.sleep(60)


def _ssh_check(user):
    """
    Check if the current user can ssh as a test user.
    This is necessary to obtain Kerberos auth headers for multi-user tests.
    """
    hostname = socket.gethostname()
    logging.info(f'Checking ssh as {user} to {hostname}')
    ssh_ok = (0 == subprocess.call(f'ssh {user}@{hostname} echo SSH', shell=True))
    assert ssh_ok, f'Unable to ssh as {user} to {hostname}'


if util.kerberos_enabled() and os.getenv('COOK_MAX_TEST_USERS'):
    switch_user_mode = os.getenv('COOK_SWITCH_USER_MODE', 'sudo')
    if switch_user_mode == 'sudo':
        username = next(util._test_user_names())
        _sudo_check(username)
        threading.Thread(target=_sudo_checker_task, args=[username], daemon=True).start()
    elif switch_user_mode == 'ssh':
        for username in util._test_user_names():
            _ssh_check(username)
    else:
        assert False, f'{switch_user_mode} is not a valid value for COOK_SWITCH_USER_MODE'


@pytest.fixture()
def cleandir(request):
    if 'TEST_METRICS_ES_URL' in os.environ:
        print('\n~~~~~ hello from fixture')
        yield
        print('\n~~~~~ goodbye from fixture')
        elastic_search_url = os.getenv('TEST_METRICS_ES_URL')
        now = datetime.datetime.utcnow()
        index = f'cook-tests-{now.strftime("%Y%m%d")}'
        test_namespace = '::'.join(request.node._nodeid.split('::')[:-1]).replace('/', '.')
        test_name = request.node.name
        doc_id = f'{test_namespace}-{test_name}-{now.strftime("%s")}-{uuid.uuid4()}'
        repository_path = os.path.abspath(f'{os.path.dirname(os.path.abspath(__file__))}/../../..')
        repo = Repository(repository_path)
        head = repo.head
        commit = repo.revparse_single('HEAD')
        metrics = {
            'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S'),
            'project': 'cook',
            'test-namespace': test_namespace,
            'test-name': test_name,
            'git-branch': head.name.replace('refs/heads/', ''),
            'git-commit-hash': commit.hex,
            'git-branch-under-test': os.getenv('TEST_METRICS_BRANCH_UNDER_TEST', None),
            'git-commit-hash-under-test': os.getenv('TEST_METRICS_COMMIT_HASH_UNDER_TEST', None),
            'host': socket.gethostname(),
            'user': getpass.getuser(),
            'build-id': os.getenv('TEST_METRICS_BUILD_ID', None),
            'result': 
        }
        logging.info(f'Updating test metrics: {metrics}')
        resp = util.session.post(f'{elastic_search_url}/{index}/test/{doc_id}', json=metrics)
        logging.info(f'Response from updating test metrics: {json.dumps(resp.json(), indent=2)}')
    else:
        yield
