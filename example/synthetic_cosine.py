from __builtin__ import type
from metadataStore.api.collection import (insert_begin_run, insert_beamline_config,
                                          insert_event, insert_event_descriptor)
from metadataStore.api.analysis import find_last
import random
import time
import string
import numpy as np

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


b_config = insert_beamline_config(config_params={'my_beamline': 'my_value'})


data_keys = {'linear_motor': 'PV1', 'scalar_detector': 'PV2',
             'Tsam': 'PV3', 'some.dotted_field': 'PV4'}

try:
    last_hdr = find_last()
    scan_id = last_hdr.scan_id+1
except IndexError:
    scan_id = 1


# Create a BeginRunEvent that serves as entry point for a run
bre = insert_begin_run(scan_id=scan_id, beamline_id='csx', time=time.time(), beamline_config=b_config)

# Create an EventDescriptor that indicates the data keys and serves as header for set of Event(s)
e_desc = insert_event_descriptor(data_keys=data_keys, time=time.time(),
                                 begin_run_event=bre)

func = np.cos
num = 1000
start = 0
stop = 10
sleep_time = .05

for idx, i in enumerate(np.linspace(start, stop, num)):
    data = {'linear_motor': i,
            'Tsam': i + 5,
            'scalar_detector': func(i)}
    e = insert_event(event_descriptor=e_desc, seq_no=idx,
                     time=time.time(),
                     data=data)
last_run = find_last()

if last_run.id != bre.id:
    print("Either Arman or Eric broke find_last(). File a complaint, we will get back at you")