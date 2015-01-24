__author__ = 'arkilic'

from metadataStore.api.analysis import find, find_last, find_event_descriptor, find_header, find_event
import time

#################################
# Run example/collection.py first ##
# Otherwise, this will not run ##
#################################
start = time.time()
res_1 = find(scan_id=3, create_time={'start': 142109013, 'end': 1521090219.127196})
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds'
header_1 = res_1[0][0]
print header_1.create_time


start = time.time()
find(create_time={'start': 142109000, 'end': start})
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds'


# hdrs, bcfgs,event_descs, evs = find(owner='arkilic')
#
#
# hdrs2, bcfgs2, event_descs2, evs2 = find(owner='arkilic', limit=10)
#
#
# for hdr in hdrs2:
#     print 'Header: ', '_id: ', hdr.id, ' owner: ', hdr.owner, ' start_time: ', hdr.start_time
#
# print 'Here is the last scan recorded', find_last().id
#
#
# hdr = find_last()
#
# print find_event_descriptor(hdr)[0].data_keys
#
# print hdr.id
#
# print find_event(hdr)[0].data
#
# #Given an object, get other objects it hold a foreignkey
# #i.e. Event_object.event_descriptor, returns evet descriptor an event refers to
#
# print 'Here is a neat feature, behold...:', find_event_descriptor(hdr)[0].header, 'I know, cool right...'
#
# #TODO: Sample collection code of the old master should be working with this new version