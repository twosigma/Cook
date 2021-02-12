import sys
import argparse
import logging

from cook import util, http, metrics, version, configuration
from cook.subcommands import admin, cat, config, jobs, kill, ls, show, ssh, submit, tail, usage, wait
from cook.util import deep_merge, load_target_clusters
from cook.plugins import SubCommandPlugin
import cook.plugins

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
    'admin': admin.register(subparsers.add_parser, configuration.add_defaults),
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


def run(args, plugins):
    """
    Main entrypoint to the cook scheduler CLI. Loads configuration files, 
    processes global command line arguments, and calls other command line 
    sub-commands (actions) if necessary.

    plugins is a map from plugin-name -> function or Class.SubCommandPlugin
    """

    # This has to happen before we parse the args, otherwise we might
    # get subcommand not found.
    for name, instance in plugins.items():
        if isinstance(instance, SubCommandPlugin):
            logging.debug('Adding SubCommandPlugin %s' % name)
            try:
                instance.register(subparsers.add_parser, configuration.add_defaults)
                logging.debug('Done adding SubCommandPlugin %s' % name)
                name = instance.name()
                if name in actions:
                    raise Exception('SubCommandPlugin %s clashes with an existing subcommand.' % name)
                actions[name] = instance.run
            except Exception as e:
                print('Failed to load SubCommandPlugin %s: %s' % (name, e), file=sys.stderr)

    args = vars(parser.parse_args(args))

    util.silent = args.pop('silent')
    verbose = args.pop('verbose') and not util.silent

    log_format = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
    if verbose:
        logging.getLogger('').handlers = []
        logging.basicConfig(format=log_format, level=logging.DEBUG)
    else:
        logging.disable(logging.FATAL)

    logging.debug('args: %s', args)

    action = args.pop('action')
    config_path = args.pop('config')
    cluster = args.pop('cluster')
    url = args.pop('url')

    if action is None:
        parser.print_help()
    else:
        _, config_map = configuration.load_config_with_defaults(config_path)
        try:
            metrics.initialize(config_map)
            metrics.inc('command.%s.runs' % action)
            clusters = load_target_clusters(config_map, url, cluster)
            http.configure(config_map, plugins)
            cook.plugins.configure(plugins)
            args = {k: v for k, v in args.items() if v is not None}
            defaults = config_map.get('defaults')
            action_defaults = (defaults.get(action) if defaults else None) or {}
            logging.debug('going to execute % action' % action)
            result = actions[action](clusters, deep_merge(action_defaults, args), config_path)
            logging.debug('result: %s' % result)
            return result
        finally:
            metrics.close()

    return None
