__author__ = 'arkilic'

from metadataStore.api.analysis import find, find_last, find_event_descriptor, find_header, find_event
import time

#################################
# Run example/collection.py first ##
# Otherwise, this will not make sense unless you have a scan_id=3 within that time range ##
#################################
start = time.time()
res_1 = find(scan_id=3, create_time={'start': 142109013, 'end': 1521090219.127196})
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds'
header_1 = res_1[0][0]
print header_1.create_time

############################################################################################################
# Yes, start_time and end_time are now deprecated. We have a new convention for doing this w/o updates using
# EventDescriptor types. Talk to Stuart if you're confused or have no idea what I am talking about.
# This notation works quite well with synchronous and asynchronous events
############################################################################################################

start = time.time()
headers, events = find(create_time={'start': 142109000, 'end': start})
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time..: ', time_elapsed, ' milliseconds'

start = time.time()
hdr, events = find(owner='arkilic')
end = time.time()
time_elapsed = (end-start)*1000
print 'Query time given owner..: ', time_elapsed, ' milliseconds'


print 'This is how you access header EventDescriptor(s) ', hdr[0].event_descriptor
print hdr[0].event_descriptor[0].id
print events