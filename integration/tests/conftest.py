import logging
import os

import pytest

logging.info('Checking if test-metric recording needs to be enabled')
if 'TEST_METRICS_URL' in os.environ:
    import datetime
    import getpass
    import json
    import socket
    from timeit import default_timer as timer

    from pygit2 import Repository

    from tests.cook import util

    repository_path = os.path.abspath(f'{os.path.dirname(os.path.abspath(__file__))}/../..')
    repo = Repository(repository_path)
    head = repo.head
    commit = repo.revparse_single('HEAD')
    git_branch = head.name.replace('refs/heads/', '')
    git_commit_hex = commit.hex
    elastic_search_url = os.getenv('TEST_METRICS_URL').rstrip('/')
    logging.info(f'Sending test metrics to {elastic_search_url}')


    @pytest.fixture()
    def record_test_metric(request):
        start = timer()
        yield
        try:
            end = timer()
            now = datetime.datetime.utcnow()
            index = f'cook-tests-{now.strftime("%Y%m%d")}'
            request_node = request.node
            xfail_mark = request_node._evalxfail._mark
            expected_to_fail = xfail_mark is not None and xfail_mark.name == 'xfail'
            test_namespace = '.'.join(request_node._nodeid.split('::')[:-1]).replace('/', '.').replace('.py', '')
            test_name = request_node.name

            # We can't assume that request_node will always have a rep_setup attribue
            if hasattr(request_node, 'rep_setup'):
                setup = request_node.rep_setup
                setup_passed = setup.passed
                setup_failed = setup.failed
            else:
                logging.warning(f'Unable to determine setup result (assuming it succeeded): {request_node}')
                setup_passed = True
                setup_failed = False

            if hasattr(request_node, 'rep_call'):
                call = request_node.rep_call
                if setup_failed or call.failed:
                    result = 'failed'
                elif setup_passed and call.passed:
                    result = 'passed'
                elif call.skipped:
                    # Unfortunately, tests marked as xfail that fail
                    # have a call outcome of 'skipped'. So, we have to
                    # workaround this by checking the _skipped_by_mark
                    # attribute here to see if it was actually skipped.
                    if request_node._skipped_by_mark:
                        result = 'skipped'
                    elif expected_to_fail:
                        result = 'failed'
                    else:
                        logging.warning('Unable to determine test result for call skipped')
                        result = 'unknown'
                else:
                    logging.warning('Unable to determine test result')
                    result = 'unknown'
            else:
                logging.warning('Test does not appear to have completed')
                result = 'uncompleted'
            metrics = {
                'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S'),
                'project': 'cook',
                'test-namespace': test_namespace,
                'test-name': test_name,
                'git-branch': git_branch,
                'git-commit-hash': git_commit_hex,
                'git-branch-under-test': os.getenv('TEST_METRICS_BRANCH_UNDER_TEST', None),
                'git-commit-hash-under-test': os.getenv('TEST_METRICS_COMMIT_HASH_UNDER_TEST', None),
                'host': socket.gethostname(),
                'user': getpass.getuser(),
                'run-id': os.getenv('TEST_METRICS_RUN_ID', None),
                'run-description': os.getenv('TEST_METRICS_RUN_DESCRIPTION', 'open source integration tests'),
                'build-id': os.getenv('TEST_METRICS_BUILD_ID', None),
                'result': result,
                'runtime-milliseconds': (end - start) * 1000,
                'expected-to-fail': expected_to_fail
            }
            timeout = os.getenv('TEST_METRICS_POST_TIMEOUT_SECONDS', 10)
            logging.info(f'Updating test metrics (timeout = {timeout} seconds): {json.dumps(metrics, indent=2)}')
            resp = util.session.post(f'{elastic_search_url}/{index}/test-result', json=metrics, timeout=timeout)
            logging.info(f'Response from updating test metrics: {resp.text}')
        except:
            logging.exception('Encountered exception while recording test metrics')


    @pytest.hookimpl(tryfirst=True, hookwrapper=True)
    def pytest_runtest_makereport(item, call):
        # execute all other hooks to obtain the report object
        outcome = yield
        rep = outcome.get_result()

        # set a report attribute for each phase of a call, which can
        # be "setup", "call", "teardown"
        setattr(item, "rep_" + rep.when, rep)
else:
    logging.info('Test-metric recording is not getting enabled')


    @pytest.fixture()
    def record_test_metric():
        pass
