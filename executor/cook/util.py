import logging
import resource
import sys

__rusage_denom_mb = 1024.0
if sys.platform == 'darwin':
    # in OSX the output is in different units
    __rusage_denom_mb = __rusage_denom_mb * 1024


def print_memory_usage():
    """Logs the memory usage of the executor."""
    try:
        max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logging.info('Executor Memory usage: {} MB'.format(max_rss / __rusage_denom_mb))
    except Exception:
        logging.exception('Error in logging memory usage')
