import pytest
import sys
import importlib
import tempfile
from multiprocessing import Process
from tests.cook import util

cook = importlib.import_module('cook.cli')

from cook.plugins import SubCommandPlugin
from cook.util import print_info
from cook.__main__ import main as cli

class DummySubCommandPlugin(SubCommandPlugin):
    def register(self, add_parser, add_defaults):
        parser = add_parser('int_test', help='int_test help menu. This command takes a digit a factor and returns the mult.')
        parser.add_argument('--digit', '-d', dest='digit', type=int, help='some int')
        parser.add_argument('--factor', '-f', dest='factor', type=int, help='some int', required=True)
        add_defaults('int_test', {'digit': 10})

    def run(self, clusters, args, _):
        print(args.get('digit') * args.get('factor'))

    def name(self):
        return 'int_test'

class SecondSubCommandPlugin(SubCommandPlugin):
    """Another subcommand for testing purposes.

    name() intentionally returns the same value
    as the previous subcommand to test error handling.
    """
    def register(self, add_parser, add_defaults):
        pass

    def run(self, cluster, args, _):
        pass

    def name(self):
        return 'int_test'

@pytest.mark.cli
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class CookCliSubCommandPluginTest(util.CookTest):

    def helper(self, args, plugins={'test': DummySubCommandPlugin()}):
        with tempfile.NamedTemporaryFile(delete=True) as temp:
            def fn():
                sys.stderr = open(temp.name, 'w')
                sys.stdout = open(temp.name, 'w')
                cli(args=args, plugins=plugins)

            p = Process(target=fn)
            p.start()
            p.join()
            temp.flush()
            return open(temp.name, 'r').read()

    def test_help_menu(self):
        output = self.helper([])
        self.assertTrue('int_test' in output)

    def test_subcommand_help_menu(self):
        output = self.helper(['int_test'])
        self.assertTrue('--digit' in output)

    def test_subcommand_output(self):
        output = self.helper(['int_test', '--digit', '10', '--factor', '10'])
        self.assertTrue('100' in output)

    def test_failed_loading(self):
        output = self.helper(
                ['int_test', '--digit', '10', '--factor', '10'],
                plugins={
                    'test1': DummySubCommandPlugin(),
                    'test2': SecondSubCommandPlugin()
                })
        self.assertTrue('Failed to load SubCommandPlugin' in output)
