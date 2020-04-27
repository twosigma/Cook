import json
import logging
import requests
import requests.adapters
from urllib.parse import urljoin

import cook.version
from cook.util import print_error

session = None
timeouts = None


def configure(config, plugins):
    """Configures HTTP timeouts and retries to be used"""
    global session
    global timeouts
    http_config = config.get('http', {})
    modules_config = http_config.get('modules')
    adapter_factory = plugins.get('http-adapter-factory', requests.adapters.HTTPAdapter)
    session_factory = plugins.get('http-session-factory', requests.Session)
    logging.getLogger('urllib3').setLevel(logging.DEBUG) # logger.disable in cli.py may override
    connect_timeout = http_config.get('connect-timeout')
    read_timeout = http_config.get('read-timeout')
    timeouts = (connect_timeout, read_timeout)
    logging.debug('using http timeouts: %s', timeouts)
    retries = http_config.get('retries')
    http_adapter = adapter_factory(max_retries=retries)
    session = session_factory()
    session.mount('http://', http_adapter)
    session.mount('https://', http_adapter)
    session.headers['User-Agent'] = f"cs/{cook.version.VERSION} ({session.headers['User-Agent']})"
    auth_config = http_config.get('auth', None)
    if auth_config:
        auth_type = auth_config.get('type')
        if auth_type == 'basic':
            basic_auth_config = auth_config.get('basic')
            user = basic_auth_config.get('user')
            session.auth = (user, basic_auth_config.get('pass'))
            logging.debug(f'using http basic auth with user {user}')
        else:
            raise Exception(f'Encountered unsupported authentication type "{auth_type}".')


def __head(url, params=None, **kwargs):
    """Sends a HEAD with params to the given url"""
    logging.info(f'HEAD {url} with {params}')
    return session.head(url, params=params, timeout=timeouts, **kwargs)


def __post(url, json_body):
    """Sends a POST with the json payload to the given url"""
    logging.info(f'POST {url} with body {json_body}')
    return session.post(url, json=json_body, timeout=timeouts)


def __get(url, params=None, **kwargs):
    """Sends a GET with params to the given url"""
    logging.info(f'GET {url} with params {params}')
    return session.get(url, params=params, timeout=timeouts, **kwargs)


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
        elif resp.status_code == 401:
            print_error(f'Authentication failed on {cluster["name"]} ({cluster["url"]}).')
            return []
        elif resp.status_code == 500:
            print_error(f'Encountered server error while querying {cluster["name"]}.')
            # fall through to logging call below

        logging.warn(f'Unexpected response code {resp.status_code} for data request. Response body: {resp.text}')
    except requests.exceptions.ConnectionError as ce:
        logging.exception(ce)
        print_error(f'Encountered connection error with {cluster["name"]} ({cluster["url"]}).')
    except requests.exceptions.ReadTimeout as rt:
        logging.exception(rt)
        print_error(f'Encountered read timeout with {cluster["name"]} ({cluster["url"]}).')
    except IOError as ioe:
        logging.exception(ioe)
    except json.decoder.JSONDecodeError as jde:
        logging.exception(jde)
    return []
