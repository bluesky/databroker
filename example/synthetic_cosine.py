from __builtin__ import type

from metadataStore.api.collection import (insert_begin_run, insert_beamline_config,
                                          insert_event, insert_event_descriptor)
from metadataStore.api.analysis import find_last, find_event, fetch_events
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
    last_hdr = find_last()[0]
    scan_id = int(last_hdr.scan_id)+1
except (IndexError, TypeError):
    scan_id = 1


# cast to string
scan_id = str(scan_id)


# Create a BeginRunEvent that serves as entry point for a run
bre = insert_begin_run(scan_id=scan_id, beamline_id='csx', time=time.time(),
                       beamline_config=b_config)

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

try:
    if last_run.id != bre.id:
        print("Either Arman or Eric broke find_last().")
except AttributeError as ae:
    print ae
res_2 = find_event(begin_run_event=bre)
if not res_2:
    print("Either Arman or Eric broke find_event().")
else:
    for event in res_2:
        for idx, i in enumerate(np.linspace(start, stop, num)):
            print event[idx].data, event[idx].seq_no

if not fetch_events(descriptor=e_desc):
    print("Either Arman or Eric broke find_event().")



{
    "uid": "4609e51f-cf38-4c2a-a6ea-483edc461e43",
    "seq_num": 42,
    "descriptor": "f05338e0-ed07-4e15-8d7b-06a60dcebaff",
    data = {
        "chan1": [3.14, 1422940467.3101866],
        "chan2": [3.14, 1422940467.3101866],
        "chan3": [3.14, 1422940467.3101866],
        "chan4": [3.14, 1422940467.3101866],
        "chan5": [3.14, 1422940467.3101866],
        "chan6": [3.14, 1422940467.3101866],
        "chan7": [3.14, 1422940467.3101866],
        "chan8": [3.14, 1422940467.3101866],
        "pimte": ["8cad7f02-c3e1-4e76-a823-94a2a7d23f6b","1422940467.3101866"]}