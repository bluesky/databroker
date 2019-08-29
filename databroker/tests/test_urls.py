import os
import requests
import ujson
import uuid
from time import time
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get('INCLUDE_V0_SERVICE_TESTS') != '1',
    reason="deprecated")


@pytest.mark.parametrize('k,v',
                         [('run_start', 'find_run_starts'),
                          ('run_stop', 'find_run_stops'),
                          ('event', 'find_events'),
                          ('event_descriptor', 'find_descriptors')])

def test_get_urls(k, v, md_server_url):
    base_url = md_server_url
    url = base_url + k
    message = dict(query={}, signature=v)
    r = requests.get(url, params=ujson.dumps(message))
    r.raise_for_status()

def test_put_rs(md_server_url):
    base_url = md_server_url
    rs = dict(time=time(),
              scan_id=1,
              beamline_id='example',
              uid=str(uuid.uuid4()))
    url = base_url + 'run_start'
    payload = dict(data=rs, signature='insert_run_start')
    r = requests.post(url, data=ujson.dumps(payload))
    kipif.raise_for_status()
