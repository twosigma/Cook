import argparse
import logging
from urllib.parse import urlparse

from cook import util, http, metrics, version, configuration
from cook.subcommands import submit, show, wait, jobs, ssh, ls, tail, kill, config, cat, usage
from cook.util import deep_merge

parser = argparse.ArgumentParser(description='cs is the Cook Scheduler CLI')
parser.add_argument('--cluster', '-c', help='the name of the Cook scheduler cluster to use')
parser.add_argument('--url', '-u', help='the url of the Cook scheduler cluster to use')
parser.add_argument('--config', '-C', help='the configuration file to use')
parser.add_argument('--silent', '-s', help='silent mode', dest='silent', action='store_true')
parser.add_argument('--verbose', '-v', help='be more verbose/talkative (useful for debugging)',
                    dest='verbose', action='store_true')
parser.add_argument('--version', help='output version information and exit',
                    version=f'%(prog)s version {version.VERSION}', action='version')

subparsers = parser.add_subparsers(dest='action')

actions = {
    'cat': cat.register(subparsers.add_parser, configuration.add_defaults),
    'config': config.register(subparsers.add_parser, configuration.add_defaults),
    'jobs': jobs.register(subparsers.add_parser, configuration.add_defaults),
    'kill': kill.register(subparsers.add_parser, configuration.add_defaults),
    'ls': ls.register(subparsers.add_parser, configuration.add_defaults),
    'show': show.register(subparsers.add_parser, configuration.add_defaults),
    'ssh': ssh.register(subparsers.add_parser, configuration.add_defaults),
    'submit': submit.register(subparsers.add_parser, configuration.add_defaults),
    'tail': tail.register(subparsers.add_parser, configuration.add_defaults),
    'usage': usage.register(subparsers.add_parser, configuration.add_defaults),
    'wait': wait.register(subparsers.add_parser, configuration.add_defaults)
}


def load_target_clusters(config_map, url=None, cluster=None):
    """Given the config and (optional) url and cluster flags, returns the list of clusters to target"""
    if cluster and url:
        raise Exception('You cannot specify both a cluster name and a cluster url at the same time')

    clusters = None
    config_clusters = config_map.get('clusters')
    if url:
        if urlparse(url).scheme == '':
            url = 'http://%s' % url
        clusters = [{'name': url, 'url': url}]
    elif config_clusters:
        if cluster:
            clusters = [c for c in config_clusters if c.get('name').lower() == cluster.lower()]
        else:
            clusters = [c for c in config_clusters if 'disabled' not in c or not c['disabled']]

    return clusters


def run(args):
    """
    Main entrypoint to the cook scheduler CLI. Loads configuration files, 
    processes global command line arguments, and calls other command line 
    sub-commands (actions) if necessary.
    """
    args = vars(parser.parse_args(args))

    util.silent = args.pop('silent')
    verbose = args.pop('verbose') and not util.silent

    log_format = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
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
        config_map = configuration.load_config_with_defaults(config_path)
        try:
            metrics.initialize(config_map)
            metrics.inc('command.%s.runs' % action)
            clusters = load_target_clusters(config_map, url, cluster)
            http.configure(config_map)
            args = {k: v for k, v in args.items() if v is not None}
            defaults = config_map.get('defaults')
            action_defaults = (defaults.get(action) if defaults else None) or {}
            result = actions[action](clusters, deep_merge(action_defaults, args), config_path)
            logging.debug('result: %s' % result)
            return result
        finally:
            metrics.close()

    return None
