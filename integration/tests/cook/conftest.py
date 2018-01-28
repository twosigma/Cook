import os
import subprocess
import threading
import time

from tests.cook import util

def sudo_check(username):
    sudo_ok = (0 == subprocess.call(f'sudo -nu {username} echo CACHED SUDO', shell=True))
    assert sudo_ok, "You need to pre-cache your sudo credentials. (Run a simple sudo command as a test user.)"

def _sudo_checker_task(username):
    while True:
        sudo_check(username)
        time.sleep(60)

if util.kerberos_enabled() and os.getenv('COOK_MAX_TEST_USERS'):
    username = next(util._test_user_names())
    sudo_check(username)
    threading.Thread(target=_sudo_checker_task, args=[username], daemon=True).start()
