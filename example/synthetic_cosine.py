from __future__ import print_function

from metadatastore.api import (insert_run_start, insert_event,
                               insert_descriptor, insert_run_stop)

from metadatastore.api import find_last, find_events
import time
import numpy as np
import uuid

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

custom = {}
# Create a BeginRunEvent that serves as entry point for a run
run_start = insert_run_start(scan_id=scan_id, beamline_id='csx',
                           time=time.time(), custom=custom,
                           uid=str(uuid.uuid4()))

# Create an EventDescriptor that indicates the data
# keys and serves as header for set of Event(s)
descriptor = insert_descriptor(data_keys=data_keys, time=time.time(),
                               run_start=run_start, uid=str(uuid.uuid4()))
func = np.cos
num = 1000
start = 0
stop = 10
sleep_time = .1
for idx, i in enumerate(np.linspace(start, stop, num)):
    data = {'linear_motor': i,
            'Tsam': i + 5,
            'scalar_detector': func(i) + np.random.randn() / 100}

    ts = {k: time.time() for k in data}

    e = insert_event(descriptor=descriptor, seq_num=idx,
                     time=time.time(),
                     timestamps=ts,
                     data=data,
                     uid=str(uuid.uuid4()))
insert_run_stop(run_start, time=time.time(), uid=str(uuid.uuid4()))
last_run = next(find_last())
try:
    if str(last_run.uid) != str(run_start):
        print("find_last() is broken")
except AttributeError as ae:
    print(ae)
res_2 = find_events(descriptor=descriptor)
if not res_2:
    print("find_events() is broken")
