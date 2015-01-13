__author__ = 'arkilic'

from metadataStore.api.analysis import find


print find(scan_id=8878179)
print find(start_time={'start': 142109013, 'end': 1421090219.127196})

import time

start = time.time()
find(scan_id=8878179, start_time={'start': 142109013, 'end': 1421090219.127196})[0].id
end = time.time()

time_elapsed = (end-start)*1000


print 'Query time..: ', time_elapsed, ' milliseconds'

start = time.time()
find(start_time={'start': 142109013, 'end': 1421090219.127196})[0].id
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds'


start = time.time()
q_res_3 = find(owner='arkilic')
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds returning ', len(q_res_3), ' headers'

for entry in q_res_3:
    print entry.id, entry.start_time
