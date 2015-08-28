__author__ = 'arkilic'
from metadataservice.client.client import *

# data_dump = simplejson.dumps({'key':"arman"})
# r = requests.post("http://127.0.0.1:7777/run_start", data=data_dump)
# if r.status_code == 200:
#     print("Success")
# else:
#     print("Tanked", r.status_code, r.text)


for i in find_run_starts(owner='xf231'):
    print(i)

for j in find_run_stops(uid="15972b46-721f-4e21-bd28-b25b77de343c"):
    print(j)