import requests
from .utils import mds_setup, mds_teardown, testing_config
import ujson
import uuid
from time import time


class TestUrls:
    def setup_class(self):
        mds_setup()
        print('Started server for test')
        self.url_dict = {'run_start': 'find_run_starts',
                         'run_stop': 'find_run_stops',
                         'event': 'find_events',
                         'event_descriptor': 'find_descriptors'}
        self.host = 'localhost'
        self.port = testing_config['serviceport']
        self.base_url = 'http://{}:{}/'.format(self.host, self.port)

    def test_run_start(self):
        rs = dict(time=time(),
                  scan_id=1,
                  beamline_id='example',
                  uid=str(uuid.uuid4()))
        url = self.base_url + 'run_start'
        payload = dict(data=rs, signature='insert_run_start')
        r = requests.post(url, data=ujson.dumps(payload))
        r.raise_for_status()
        for k, v in self.url_dict.items():
            url = self.base_url + k
            message = dict(query={}, signature=v)
            r = requests.get(url, params=ujson.dumps(message))
            r.raise_for_status()

    def teardown_class(self):
        mds_teardown()
        print('\nKilled server after test')
