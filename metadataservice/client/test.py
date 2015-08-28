from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from metadataservice.client.client import *

__author__ = 'arkilic'

# data_dump = simplejson.dumps({'key':"arman"})
# r = requests.post("http://127.0.0.1:7777/run_start", data=data_dump)
# if r.status_code == 200:
#     print("Success")
# else:
#     print("Tanked", r.status_code, r.text)

conf.connection_config['host'] = 'localhost'

rs = find_run_starts(owner='xf23id1')
for r in rs:
    print(r)
print('done')

ev = find_events()
# print(next(ev))