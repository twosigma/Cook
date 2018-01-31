# This file is automatically loaded and run by pytest during its setup process,
# meaning it happens before any of the tests in this directory are run.
# See the pytest documentation on conftest files for more information:
# https://docs.pytest.org/en/2.7.3/plugins.html#conftest-py-plugins

import os
import subprocess
import threading
import time

from tests.cook import util

def _sudo_check(username):
    """
    Check if the current user can sudo as a test user.
    This is necessary to obtain Kerberos auth headers for multi-user tests.
    """
    sudo_ok = (0 == subprocess.call(f'sudo -nu {username} echo CACHED SUDO', shell=True))
    assert sudo_ok, "You need to pre-cache your sudo credentials. (Run a simple sudo command as a test user.)"

def _sudo_checker_task(username):
    """Periodically check sudo ability to ensure the credentials stay cached."""
    while True:
        _sudo_check(username)
        time.sleep(60)

if util.kerberos_enabled() and os.getenv('COOK_MAX_TEST_USERS'):
    username = next(util._test_user_names())
    _sudo_check(username)
    threading.Thread(target=_sudo_checker_task, args=[username], daemon=True).start()
