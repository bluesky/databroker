from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from metadataservice.client.client import *

__author__ = 'arkilic'



data_dump = json_util.dumps({'arman': 'val'})
r = requests.post("http://127.0.0.1:7770/run_start", data=data_dump)
if r.status_code == 200:
    print("Success")
else:
    print("Tanked", r.status_code, r.text)
 
# conf.connection_config['host'] = 'localhost'
# 
# rs = find_run_starts(owner='xf23id1')
# for r in rs:
#     print(r)
# print('done')
# 
# 
# # status = insert_run_start(1235353.6353, scan_id=0, 
# #                           uid='agad353agad3531', custom={'trial': 1})
# # print(status)