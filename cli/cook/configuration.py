import json
import logging
import os

from cook.util import deep_merge

# Default locations to check for configuration files if one isn't given on the command line
DEFAULT_CONFIG_PATHS = ['.cs.json',
                        os.path.expanduser('~/.cs.json')]

DEFAULT_CONFIG = {'defaults': {},
                  'http': {'retries': 2,
                           'connect-timeout': 3.05,
                           'read-timeout': 20},
                  'metrics': {'disabled': True,
                              'timeout': 0.15,
                              'max-retries': 2}}


def __load_json_file(path):
    """Decode a JSON formatted file."""
    content = None

    if os.path.isfile(path):
        with open(path) as json_file:
            try:
                logging.debug(f'attempting to load json configuration from {path}')
                content = json.load(json_file)
            except Exception:
                pass
    else:
        logging.info(f'{path} is not a file')

    return content


def __load_first_json_file(paths):
    """Returns the contents of the first parseable JSON file in a list of paths."""
    if paths is None:
        paths = []
    contents = ((os.path.abspath(p), __load_json_file(os.path.abspath(p))) for p in paths if p)
    return next(((p, c) for p, c in contents if c), (None, None))


def load_config(config_path):
    """
    Loads the configuration map, using the provided config_path if not None,
    otherwise, searching the default config paths for a valid JSON config file
    """
    if config_path:
        if os.path.isfile(config_path):
            with open(config_path) as json_file:
                config = json.load(json_file)
        else:
            raise Exception(f'The configuration path specified ({config_path}) is not valid.')
    else:
        config_path, config = __load_first_json_file(DEFAULT_CONFIG_PATHS)

    return config_path, config


def load_config_with_defaults(config_path=None):
    """Loads the configuration map to use, merging in the defaults"""
    _, config = load_config(config_path)
    config = config or {}
    config = deep_merge(DEFAULT_CONFIG, config)
    logging.debug(f'using configuration: {config}')
    return config


def add_defaults(action, defaults):
    """Adds default arguments for the given action to the DEFAULT_CONFIG map"""
    DEFAULT_CONFIG['defaults'][action] = defaults


def save_config(config_path, config_map):
    """Saves the provided config_map to the provided config_path"""
    with open(config_path, 'w') as outfile:
        json.dump(config_map, outfile, indent=2)
