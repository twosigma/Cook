import importlib
import json
import logging
from urllib.parse import urljoin

import requests

import cook

session = None
timeouts = None


def configure(config):
    """Configures HTTP timeouts and retries to be used"""
    global session
    global timeouts
    http_config = config.get('http')
    modules_config = http_config.get('modules')
    session_module_name = modules_config.get('session-module')
    adapters_module_name = modules_config.get('adapters-module')
    session_module = importlib.import_module(session_module_name)
    adapters_module = importlib.import_module(adapters_module_name)
    logging.getLogger(session_module_name).setLevel(logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.DEBUG)
    connect_timeout = http_config.get('connect-timeout')
    read_timeout = http_config.get('read-timeout')
    timeouts = (connect_timeout, read_timeout)
    logging.debug('using http timeouts: %s', timeouts)
    retries = http_config.get('retries')
    http_adapter = adapters_module.HTTPAdapter(max_retries=retries)
    session = session_module.Session()
    session.mount('http://', http_adapter)
    session.headers['User-Agent'] = f"cs/{cook.version.VERSION} ({session.headers['User-Agent']})"


def __post(url, json_body):
    """Sends a POST with the json payload to the given url"""
    logging.info(f'POST {url} with body {json_body}')
    return session.post(url, json=json_body, timeout=timeouts)


def __get(url, params=None):
    """Sends a GET with params to the given url"""
    logging.info(f'GET {url} with params {params}')
    return session.get(url, params=params, timeout=timeouts)


def __delete(url, params=None):
    """Sends a DELETE with params to the given url"""
    logging.info(f'DELETE {url} with params {params}')
    return session.delete(url, params=params, timeout=timeouts)


def __make_url(cluster, endpoint):
    """Given a cluster and an endpoint, returns the corresponding full URL"""
    return urljoin(cluster['url'], endpoint)


def post(cluster, endpoint, json_body):
    """POSTs data to cluster at /endpoint"""
    url = __make_url(cluster, endpoint)
    resp = __post(url, json_body)
    logging.info(f'POST response: {resp.text}')
    return resp


def get(cluster, endpoint, params):
    """GETs data corresponding to the given params from cluster at /endpoint"""
    url = __make_url(cluster, endpoint)
    resp = __get(url, params)
    logging.info(f'GET response: {resp.text}')
    return resp


def delete(cluster, endpoint, params):
    """DELETEs data corresponding to the given params on cluster at /endpoint"""
    url = __make_url(cluster, endpoint)
    resp = __delete(url, params)
    logging.info(f'DELETE response: {resp.text}')
    return resp


def make_data_request(cluster, make_request_fn):
    """
    Makes a request (using make_request_fn), parsing the
    assumed-to-be-JSON response and handling common errors
    """
    try:
        resp = make_request_fn()
        if resp.status_code == 200:
            return resp.json()
        else:
            return []
    except requests.exceptions.ConnectionError as ce:
        logging.exception(ce)
        raise Exception(f'Encountered connection error with {cluster["name"]} ({cluster["url"]}).')
    except requests.exceptions.ReadTimeout as rt:
        logging.exception(rt)
        raise Exception(f'Encountered read timeout with {cluster["name"]} ({cluster["url"]}).')
    except IOError as ioe:
        logging.exception(ioe)
        return []
    except json.decoder.JSONDecodeError as jde:
        logging.exception(jde)
        return []
