from metadataStore.api.collection import (save_begin_run, save_beamline_config,
                                          save_event, save_event_descriptor)
from metadataStore.api.analysis import find_last
import random
import time
import string
import numpy as np

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


b_config = save_beamline_config(config_params={'my_beamline': 'my_value'})


data_keys = {'linear_motor': 'PV1', 'scalar_detector': 'PV2', 'Tsam': 'PV3'}

try:
    last_hdr = find_last()
    scan_id = last_hdr.scan_id+1
except IndexError:
    scan_id = 1

bre = save_begin_run(scan_id=scan_id, beamline_id='csx', time=time.time(), beamline_config=b_config)

e_desc = save_event_descriptor(data_keys=data_keys,
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
    e = save_event(event_descriptor=e_desc, seq_no=idx,
                   time=time.time(),
                   data=data)
    # time.sleep(sleep_time)

print find_last()