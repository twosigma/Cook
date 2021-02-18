import logging

__plugins = {}

class SubCommandPlugin:
    """Base class to implement custom subcommands."""

    def __init__(self):
        pass

    def register(self, add_parser, add_defaults):
        """Register this subcommand with argparse.

        Must be implemented by the subclass extending SubCommandPlugin.
        """
        raise NotImplementedError

    def run(self, clusters, args, config_path):
        """Run the subcommand.

        Must be implemented by the subclass extending SubCommandPlugin.
        """
        raise NotImplementedError

    def name():
        """Return the shortname of the subcommand.

        This shortname is used to register this subcommand in the list
        of supported actions. It cannot clash with an existing core
        subcommand or other plugin based subcommands.

        Must be implemented by the subclass extended SubCommandPlugin.
        """
        raise NotImplementedError

def configure(plugins):
    """Configures global plugins to the plugins map"""
    global __plugins
    __plugins = plugins
    logging.debug('plugins: %s', __plugins)


def get_fn(plugin_name, default_fn):
    """Returns the plugin function corresponding to the given plugin name if found, otherwise, default_fn"""
    return __plugins.get(plugin_name, default_fn)
