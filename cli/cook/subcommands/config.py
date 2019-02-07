from cook.util import print_info

from cook import configuration, terminal
from cook.configuration import load_config_with_defaults


def get_in(dct, keys):
    """Gets the value in dct at the nested path indicated by keys"""
    for key in keys:
        if not isinstance(dct, dict):
            return None

        if key in dct:
            dct = dct[key]
        else:
            return None
    return dct


def set_in(dct, keys, value):
    """Sets the value in dct at the nested path indicated by keys"""
    for key in keys[:-1]:
        if key not in dct:
            dct[key] = {}
        dct = dct[key]
    leaf_key = keys[-1]
    if leaf_key in dct and isinstance(dct[leaf_key], dict):
        raise Exception(f'Unable to set value because {".".join(keys)} is a configuration section.')
    else:
        dct[leaf_key] = value


def is_int(s):
    """Returns true if s represents an integer"""
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_float(s):
    """Returns true if s represents a float"""
    try:
        float(s)
        return True
    except ValueError:
        return False


def get_config_value(config_map, keys):
    """Attempts to print the config value at the location specified by keys"""
    value = get_in(config_map, keys)

    if value is None:
        raise Exception(f'Configuration entry {".".join(keys)} not found.')

    if isinstance(value, dict):
        raise Exception(f'Unable to get value because {".".join(keys)} is a configuration section.')

    print(value)
    return 0


def set_config_value(config_map, keys, value, config_path):
    """Attempts to set the config entry at the location specified by keys to value"""
    if is_int(value):
        value = int(value)
    elif is_float(value):
        value = float(value)
    elif value.lower() == 'true':
        value = True
    elif value.lower() == 'false':
        value = False

    set_in(config_map, keys, value)
    print_info(f'Updating configuration in {terminal.bold(config_path)}.')
    configuration.save_config(config_path, config_map)
    return 0


def config(_, args, config_path):
    """
    Gets or sets the value for a given configuration key, where
    the key is specified using a dot-separated path, e.g.:

        $ cs config metrics.disabled false

    In the example above, we will set the disabled key in the
    metrics map to false.
    """
    get = args.get('get')
    key = args.get('key')
    value = args.get('value')

    if len(key) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single key.')

    keys = key[0].split('.')
    config_path, config_map = load_config_with_defaults(config_path)

    if get:
        return get_config_value(config_map, keys)
    else:
        if not config_path:
            raise Exception(f'Unable to locate configuration file.')
        return set_config_value(config_map, keys, value, config_path)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('config', help='get and set configuration options')
    parser.add_argument('--get', help='get the value for a given key; returns error code 1 if the key was not found',
                        action='store_true')
    parser.add_argument('key', nargs=1)
    parser.add_argument('value', nargs='?')
    return config


