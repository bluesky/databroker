from datetime import datetime as dt


class DummyEventDescriptor(object):
    def __init__(self):
        self.keys = {'temp': {'source': 'PV:blah'},
                     'picture': {'source': 'CCD:blah',
                                 'external': 'FILESTORE!!!!'}}


class DummyEvent(object):
    def __init__(self):
        self.ev_desc = DummyEventDescriptor()
        self.data = {'temp': {'value': 273, 'timestamp': None},
                     'picture': {'value': 'np.ones((10, 10))',
                                 'timestamp': None}}
        self.time = dt(2014, 01, 01, 1, 2, 3)


def find(header_id=None, scan_id=None, owner=None, start_time=None,
         beamline_id=None, end_time=None):
    return 3 * [DummyEvent()]
