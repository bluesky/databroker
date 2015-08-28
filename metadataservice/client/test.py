from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from metadataservice.client.client import *

from bson import json_util



data = {'uid': 'c5ae4c83-89dd-4d-bb61-09faaba9a07', 'project': '', 'group': '', 'owner': 'xf23id1' , 'beamline_id': 'xf23id', 'time': 1435547475.537353, 'time_as_datetime': datetime.datetime(2015, 6, 28, 23, 11, 15, 537000), 'scan_id': 11271, 'sample': {}}

# d=json_util.dumps(data)
# print(d)
# r = requests.post("http://127.0.0.1:7770/run_start",data = d) 
# if r.status_code == 200:
#     print("Success")
# else:
#     print("Tanked", r.status_code, r.text)
#   
conf.connection_config['host'] = 'localhost'
 
conf.connection_config['port'] = 7770
rs = find_run_starts(owner='xf23id1')
next(rs)
print('done')

insert_beamline_config(time=1252, scan_id=0, uid='r', custom={'arman':1})