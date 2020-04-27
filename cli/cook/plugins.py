import logging

__plugins = {}


def configure(plugins):
    """Configures global plugins to the plugins map"""
    global __plugins
    __plugins = plugins
    logging.debug('plugins: %s', __plugins)


def get_fn(plugin_name, default_fn):
    """Returns the plugin function corresponding to the given plugin name if found, otherwise, default_fn"""
    return __plugins.get(plugin_name, default_fn)
