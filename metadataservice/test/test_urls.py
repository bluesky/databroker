import requests
from .utils import mds_setup, mds_teardown, testing_config
import ujson
from time import sleep

def setup():
    mds_setup()

def test_smoke_urls():
    host='localhost'
    port=testing_config['serviceport']
    test_dict = {'run_start': 'find_run_starts',
                 'run_stop': 'find_run_stops',
                 'event': 'find_events',
                 'event_descriptor': 'find_descriptors'}
    base_url = 'http://{}:{}/'.format(host, port)
    trans_dict = dict(query={}, signature='find_run_start')
    for k, v in test_dict.items():
        url = base_url + k
        message = dict(query={}, signature=v)
        r = requests.get(url, params=ujson.dumps(message))
        r.raise_for_status()


def teardown():
    mds_teardown()
