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
    try:
        metrics_config = config.get('metrics')
        __disabled = metrics_config.get('disabled')
        if __disabled:
            return

        global __conn
        global __line_formats
        __conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        __conn.connect((metrics_config.get('host'), metrics_config.get('port')))
        __line_formats = metrics_config.get('line-formats')
    except:
        __disabled = True
        logging.exception('exception when initializing metrics')


def close():
    """Closes the metrics module (unless disabled)"""
    global __disabled
    if __disabled:
        return
    try:
        __conn.close()
    except:
        __disabled = True
        logging.exception('exception when closing metrics socket')


def __send(metric):
    """Sends the given metric using the configured line format"""
    global __disabled
    try:
        line_format = __line_formats[metric['type']]
        metric_line = line_format.format(**metric)
        logging.info('sending metric %s' % metric_line)
        __conn.send(('%s\n' % metric_line).encode())
    except:
        __disabled = True
        logging.exception('exception when sending metric %s' % metric)


def inc(metric_name, count=1):
    """Increments a counter with the given metric_name by count"""
    if __disabled:
        return
    metric = {'namespace': 'cs',
              'name': metric_name,
              'value': count,
              'host': __host,
              'user': __user,
              'type': 'count'}
    __send(metric)
