from __future__ import print_function

from metadatastore.api import (insert_run_start, insert_beamline_config,
                               insert_event, insert_event_descriptor,
                               insert_run_stop)
from metadatastore.api import find_last, find_events
import time
import numpy as np

b_config = insert_beamline_config(config_params={'my_beamline': 'my_value'},
                                  time=time.time())

data_keys = {'linear_motor': {'source': 'PV:pv1',
                              'shape': None,
                              'dtype': 'number'},
             'scalar_detector': {'source': 'PV:pv2',
                                 'shape': None,
                                 'dtype': 'number'},
             'Tsam': {'source': 'PV:pv3',
                      'dtype': 'number',
                      'shape': None}
             }

try:
    last_hdr = next(find_last())
    scan_id = int(last_hdr.scan_id)+1
except (IndexError, TypeError):
    scan_id = 1

custom = {'plotx': 'linear_motor', 'ploty': 'scalar_detector'}
# Create a BeginRunEvent that serves as entry point for a run
rs = insert_run_start(scan_id=scan_id, beamline_id='csx', time=time.time(),
                      beamline_config=b_config, custom=custom)

# Create an EventDescriptor that indicates the data
# keys and serves as header for set of Event(s)
e_desc = insert_event_descriptor(data_keys=data_keys, time=time.time(),
                                 run_start=rs)
func = np.cos
num = 1000
start = 0
stop = 10
sleep_time = .1
for idx, i in enumerate(np.linspace(start, stop, num)):
    data = {'linear_motor': [i, time.time()],
            'Tsam': [i + 5, time.time()],
            'scalar_detector': [func(i) + np.random.randn() / 100,
                                time.time()]}
    e = insert_event(event_descriptor=e_desc, seq_num=idx,
                     time=time.time(),
                     data=data)
last_run = next(find_last())
try:
    if str(last_run.id) != str(rs.id):
        print("find_last() is broken")
except AttributeError as ae:
    print(ae)
res_2 = find_events(descriptor=e_desc)
if not res_2:
    print("find_events() is broken")
