from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from metadataclient.commands import *
import metadataclient
import time
from collections import deque
import uuid
import numpy as np
from uuid import uuid4


conf.connection_config['host'] = 'localhost'
conf.connection_config['port'] = 7770
custom = {}


def setup_syn():

    data_keys = {k:  {'source': k,
                      'dtype': 'number',
                      'shape': None} for k in 'ABCEDEFHIJKL'
                 }
    scan_id = 1
    cfg = {'beamline_id': 'testing',
            'custom': {},
            'group': 'test',
            'owner': 'test',
            'project': 'test',
            'scan_id': 1,
            'time': 1441727144.985973,
            'uid': str(uuid.uuid4())}
    # Create a BeginRunEvent that serves as entry point for a run
    rs = insert_run_start(scan_id=scan_id, beamline_id='testing', time=time.time(),
                          custom=custom, uid=str(uuid.uuid4()), config=cfg, project='test',
                          owner='test', group='test')
    
    # Create an EventDescriptor that indicates the data
    # keys and serves as header for set of Event(s)

    e_desc = insert_descriptor(data_keys=data_keys, time=time.time(),
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
num = 65000
start = 0
stop = 10


cfg = {'beamline_id': 'testing',
            'custom': {},
            'group': 'test',
            'owner': 'test',
            'project': 'test',
            'scan_id': 1,
            'time': 1441727144.985973,
            'uid': str(uuid.uuid4())}
my_uid = str(uuid.uuid4())
rs = insert_run_start(scan_id=1, beamline_id='testing', time=time.time(),
                      custom=custom, uid=my_uid, config=cfg, project='test',
                    owner='test', group='test')

inserted_rs = utils.Document('RunStart', rs)

retrieved_rs = next(find_run_starts(range_floor=0, range_ceil=100, uid=inserted_rs.uid))


print(inserted_rs)
print(retrieved_rs)

if inserted_rs.uid != retrieved_rs.uid:
    raise Exception('Inserted run_start is not the same as retrieved run_start')

data_keys = {k:  {'source': k,
                      'dtype': 'number',
                      'shape': None} for k in 'ABCEDEFHIJKL'
                 }
 
desc_uid = str(uuid.uuid4())

e_desc = insert_descriptor(data_keys=data_keys, time=time.time(),
                           run_start=retrieved_rs, uid=desc_uid)
inserted_desc = utils.Document('EventDescriptor', e_desc)
retrieved_desc = next(find_descriptors(range_floor=0, range_ceil=100, run_start=inserted_rs))

print(inserted_desc)
print(retrieved_desc)

if inserted_desc.uid != retrieved_desc.uid:
    print('Inserted descriptor is not the same as retrieved descriptor')


stop_uid = str(uuid.uuid4())

stop = insert_run_stop(retrieved_rs, 
                       time=time.time(), 
                       uid=stop_uid, 
                       exit_status='success', reason='')

inserted_stop = utils.Document('RunStop', stop)
retrieved_stop = next(find_run_stops(range_floor=0, range_ceil=100, run_start=retrieved_rs.uid))

if inserted_stop != retrieved_stop:
    print('Inserted stop is not the same as retrieved stop')





retrieved_stop = stop_by_start(run_start=inserted_rs)

if inserted_stop != retrieved_stop:
    print('Inserted stop is not the same as retrieved stop')









metadataclient.commands._insert2cappedstart(scan_id=1, beamline_id='testing', time=time.time(),
                      custom=custom, uid=my_uid, config=cfg, project='test',
                    owner='test', group='test')
 
# print(next(monitor_run_start(callback=None)))
res1 = find_run_starts(range_floor=0, range_ceil=100, owner='test')
# for _ in res1:
#     print(_)
my_uid2 = str(uuid.uuid4())
 
print("here is runstart uid", my_uid)
  
  
# print(next(monitor_run_start()))
 
 

