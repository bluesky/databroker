from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from metadataservice.client.client import *
import time
from collections import deque
import uuid
import numpy as np

 
 
data = {'uid': 'c5ae4c83-89dd-4d-bb61-09faaba9a07', 'project': '', 'group': '', 'owner': 'xf23id1' , 'beamline_id': 'xf23id', 'time': 1435547475.537353, 'time_as_datetime': datetime.datetime(2015, 6, 28, 23, 11, 15, 537000), 'scan_id': 11271, 'sample': {}}
 
   
conf.connection_config['host'] = 'localhost'
  
conf.connection_config['port'] = 7771
rs = find_run_starts(owner='xf23id1')
# next(rs)
print('done')
# print(event_desc_given_uid(event_descriptor='7667c81e-c159-4104-9866-6bbe6eaa0b4a'))


custom = {}
def setup_syn():

    data_keys = {k:  {'source': k,
                          'dtype': 'number',
                          'shape': None} for k in 'ABCEDEFHIJKL'
                 }
    scan_id = 1

    # Create a BeginRunEvent that serves as entry point for a run
    rs = insert_run_start(scan_id=scan_id, beamline_id='testing', time=time.time(),
                          custom=custom, uid=str(uuid.uuid4()))
    
    # Create an EventDescriptor that indicates the data
    # keys and serves as header for set of Event(s)
    
    print('rs', rs)
    
    e_desc = insert_event_descriptor(data_keys=data_keys, time=time.time(),
                                     run_start=rs, uid=str(uuid.uuid4()))
    return rs, e_desc, data_keys

def syn_data(data_keys, count):
    all_data = deque()
    for seq_num in range(count):
        data = {k: float(seq_num) for k in data_keys}
        timestamps = {k: time.time() for k in data_keys}

        _time = time.time()
        uid = str(uuid.uuid4())
        all_data.append({'data': data, 'timestamps': timestamps,
                         'seq_num': seq_num, 'time':_time,
                         'uid': uid})
    return all_data

func = np.cos
num = 10000
start = 0
stop = 10



rs, e_desc, data_keys = setup_syn()
all_data = syn_data(data_keys, num)



start_time = time.time()
insert_event(descriptor=e_desc, events=all_data)
stop_time = time.time()
print('insert time: {}'.format(stop_time - start_time))

print(next(find_run_starts(owner='xf23id1')))

start_time = time.time()
print(e_desc)
ret = find_events(descriptor=e_desc)
stop_time = time.time()

print('retrieve time: {}'.format(stop_time - start_time))
