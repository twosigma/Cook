import logging
import socket

from cook.util import current_user

__line_formats = None
__conn = None
__host = socket.gethostname()
__user = current_user()
__disabled = True


def initialize(config):
    """
    Initializes the metrics module using the given
    config; note that metrics can be completely
    disabled in which case this is essentially a no-op
    """
    global __disabled
    metrics_config = config.get('metrics')
    __disabled = metrics_config.get('disabled')
    if __disabled:
        return

    global __conn
    global __line_formats
    __conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    __conn.connect((metrics_config.get('host'), metrics_config.get('port')))
    __line_formats = metrics_config.get('line-formats')


def close():
    """Closes the metrics module (unless disabled)"""
    if __disabled:
        return
    __conn.close()


def __send(metric):
    """Sends the given metric using the configured line format"""
    line_format = __line_formats[metric['type']]
    try:
        metric_line = line_format.format(**metric)
        logging.info('sending metric %s' % metric_line)
        __conn.send(('%s\n' % metric_line).encode())
    except:
        logging.exception('exception when sending metric %s using line format %s' % (metric, line_format))


def inc(metric_name):
    """Increments a counter with the given metric_name"""
    if __disabled:
        return
    metric = {'name': metric_name,
              'value': 1,
              'host': __host,
              'user': __user,
              'type': 'count'}
    __send(metric)
