__author__ = 'arkilic'

from metadataStore.api.analysis import find, find_last


print find(scan_id=8878179)
print find(start_time={'start': 142109013, 'end': 1421090219.127196})

import time

start = time.time()
find(scan_id=8878179, start_time={'start': 142109013, 'end': 1421090219.127196})
end = time.time()

time_elapsed = (end-start)*1000


print 'Query time..: ', time_elapsed, ' milliseconds'

start = time.time()
find(start_time={'start': 142109013, 'end': 1421090219.127196})
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds'


# start = time.time()
hdrs, bcfgs,event_descs, evs = find(owner='arkilic')


hdrs2, bcfgs2, event_descs2, evs2 = find(owner='arkilic', limit=10)


for hdr in hdrs2:
    print 'Header: ', '_id: ', hdr.id, ' owner: ', hdr.owner, ' start_time: ', hdr.start_time

print 'Here is the last scan recorded', find_last().id


