import argparse
import json
import logging
import os
from urllib.parse import urlparse

from cook import util, http, colors, metrics
from cook.subcommands import submit, show, wait, list
from cook.util import deep_merge, load_first_json_file

# Default locations to check for configuration files if one isn't given on the command line
DEFAULT_CONFIG_PATHS = ['.cs.json',
                        os.path.expanduser('~/.cs.json')]

DEFAULT_CONFIG = {'defaults': {},
                  'http': {'retries': 2,
                           'connect-timeout': 3.05,
                           'read-timeout': 5,
                           'modules': {'session-module': 'requests',
                                       'adapters-module': 'requests.adapters'}},
                  'metrics': {'disabled': True}}


def add_defaults(action, defaults):
    """Adds default arguments for the given action to the DEFAULT_CONFIG map"""
    DEFAULT_CONFIG['defaults'][action] = defaults


parser = argparse.ArgumentParser(description='cs is the Cook Scheduler CLI')
parser.add_argument('--cluster', '-c', help='the name of the Cook scheduler cluster to use')
parser.add_argument('--url', '-u', help='the url of the Cook scheduler cluster to use')
parser.add_argument('--config', '-C', help='the configuration file to use')
parser.add_argument('--silent', '-s', help='silent mode', dest='silent', action='store_true')
parser.add_argument('--verbose', '-v', help='be more verbose/talkative (useful for debugging)',
                    dest='verbose', action='store_true')

subparsers = parser.add_subparsers(dest='action')

actions = {'submit': submit.register(subparsers.add_parser, add_defaults),
           'show': show.register(subparsers.add_parser, add_defaults),
           'wait': wait.register(subparsers.add_parser, add_defaults),
           'list': list.register(subparsers.add_parser, add_defaults)}


def load_target_clusters(config, url=None, cluster=None):
    """Given the config and (optional) url and cluster flags, returns the list of clusters to target"""
    if cluster and url:
        raise Exception('You cannot specify both a cluster name and a cluster url at the same time')

    clusters = None
    config_clusters = config.get('clusters')
    if url:
        if urlparse(url).scheme == '':
            url = 'http://%s' % url
        clusters = [{'name': url, 'url': url}]
    elif config_clusters:
        if cluster:
            clusters = [c for c in config_clusters if c.get('name') == cluster]
        else:
            clusters = [c for c in config_clusters if 'disabled' not in c or not c['disabled']]

    if not clusters:
        raise Exception('%s\nYour current configuration is:\n%s' %
                        (colors.failed('You must specify at least one cluster.'), json.dumps(config, indent=2)))

    return clusters


def load_config(config_path=None):
    """Loads the configuration map to use"""
    if config_path:
        if os.path.isfile(config_path):
            with open(config_path) as json_file:
                config = json.load(json_file)
        else:
            raise Exception('The configuration path specified (%s) is not valid' % config_path)
    else:
        config = load_first_json_file(DEFAULT_CONFIG_PATHS) or {}
    config = deep_merge(DEFAULT_CONFIG, config)
    logging.debug('using configuration: %s' % config)
    return config


def run(args):
    """
    Main entrypoint to the cook scheduler CLI. Loads configuration files, 
    processes global command line arguments, and calls other command line 
    sub-commands (actions) if necessary.
    """
    args = vars(parser.parse_args(args))
    util.silent = args.pop('silent')
    verbose = args.pop('verbose') and not util.silent

    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    if verbose:
        logging.getLogger('').handlers = []
        logging.basicConfig(format=log_format, level=logging.DEBUG)
    else:
        logging.disable(logging.FATAL)

    logging.debug('args: %s' % args)

    action = args.pop('action')
    config_path = args.pop('config')
    cluster = args.pop('cluster')
    url = args.pop('url')

    if action is None:
        parser.print_help()
    else:
        config = load_config(config_path)
        try:
            metrics.initialize(config)
            metrics.inc('command.%s.runs' % action)
            clusters = load_target_clusters(config, url, cluster)
            http.configure(config)
            args = {k: v for k, v in args.items() if v is not None}
            defaults = config.get('defaults')
            action_defaults = (defaults.get(action) if defaults else None) or {}
            result = actions[action](clusters, deep_merge(action_defaults, args))
            logging.debug('result: %s' % result)
            return result
        finally:
            metrics.close()

    return None
