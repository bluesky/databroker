from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from metadataservice.client.client import *
 
from bson import json_util
 
 
 
data = {'uid': 'c5ae4c83-89dd-4d-bb61-09faaba9a07', 'project': '', 'group': '', 'owner': 'xf23id1' , 'beamline_id': 'xf23id', 'time': 1435547475.537353, 'time_as_datetime': datetime.datetime(2015, 6, 28, 23, 11, 15, 537000), 'scan_id': 11271, 'sample': {}}
 
   
conf.connection_config['host'] = 'localhost'
  
conf.connection_config['port'] = 7770
rs = find_run_starts(owner='xf23id1')
# next(rs)
print('done')


print(event_desc_given_uid(event_descriptor='7667c81e-c159-4104-9866-6bbe6eaa0b4a'))