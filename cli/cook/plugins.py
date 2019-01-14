import importlib
import importlib.util
import logging

plugins = {}


def configure(config):
    """Configures the plugins map from the given config map"""
    global plugins
    if 'plugins' in config:
        plugins = config['plugins']
    else:
        logging.debug('no plugins section found in config')

    logging.debug(f'plugins: {plugins}')


def get_fn(plugin_name, default_fn):
    """Returns the plugin function corresponding to the given plugin name if found, otherwise, default_fn"""
    if plugin_name in plugins:
        try:
            plugin = plugins[plugin_name]
            plugin_path = plugin['path']
            logging.debug(f'using plugin path {plugin_path}')
            spec = importlib.util.spec_from_file_location(plugin['module-name'], plugin_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            method_to_call = getattr(module, plugin['function-name'])
            return method_to_call
        except Exception as e:
            logging.exception(e)

    return default_fn
