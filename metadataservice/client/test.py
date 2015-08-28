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

for i in find_run_starts(owner='xf231'):
    print(i)

for j in find_run_stops(uid="15972b46-721f-4e21-bd28-b25b77de343c"):
    print(j)