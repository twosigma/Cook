import logging

import requests

session = requests.Session()
timeouts = None


def configure(config):
    """Configures HTTP timeouts and retries to be used"""
    global timeouts
    http_config = config.get('http')
    connect_timeout = http_config.get('connect-timeout')
    read_timeout = http_config.get('read-timeout')
    timeouts = (connect_timeout, read_timeout)
    logging.debug('using http timeouts: %s', timeouts)
    retries = http_config.get('retries')
    http_adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    session.mount('http://', http_adapter)


def post(url, json):
    """Sends a POST with the json payload to the given url"""
    return session.post(url, json=json, timeout=timeouts)


def get(url, params=None):
    """Sends a GET with params to the given url"""
    return session.get(url, params=params, timeout=timeouts)
