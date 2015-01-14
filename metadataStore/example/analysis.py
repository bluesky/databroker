__author__ = 'arkilic'

from metadataStore.api.analysis import find, find_last, find_event_descriptor, find_header, find_event
import time

start = time.time()
find(scan_id=8878179, start_time={'start': 142109013, 'end': 1421090219.127196})
end = time.time()

time_elapsed = (end-start)*1000


print 'Query time..: ', time_elapsed, ' milliseconds'

start = time.time()
find(start_time={'start': 142109013, 'end': 142117687})
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds'


hdrs, bcfgs,event_descs, evs = find(owner='arkilic')


hdrs2, bcfgs2, event_descs2, evs2 = find(owner='arkilic', limit=10)


for hdr in hdrs2:
    print 'Header: ', '_id: ', hdr.id, ' owner: ', hdr.owner, ' start_time: ', hdr.start_time

print 'Here is the last scan recorded', find_last().id


hdr = find_last()

print find_event_descriptor(hdr)[0].data_keys

print hdr.id

print find_event(hdr)[0].data

#Given an object, get other objects it hold a foreignkey
#i.e. Event_object.event_descriptor, returns evet descriptor an event refers to

print 'Here is a neat feature, behold...:', find_event_descriptor(hdr)[0].header, 'I know, cool right...'

#TODO: Sample collection code of the old master should be working with this new version