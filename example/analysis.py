__author__ = 'arkilic'

from filestore.api.analysis import find, find_last, find_nugget, find_resoure_attributes
import time

start = time.time()
base = find_last()
end = time.time()

print "Query time to get the last Resource insert...: ", str((end-start)*1000) , ' milliseconds'

print base.id

#Note: The event_id below is identical to file_link created under example/collection.py.
# Note:This is not a real id inside metadataStore.No need to blame Arman for referring to non-existent documents
start2 = time.time()
base2 = find_nugget(event_id='54b59cf5fa44833081ba8282')
end2 = time.time()
print "Query time to get the FileLink insert...: ", str((end2-start2)*1000) , ' milliseconds'

print base2[0].resource.id

start3 = time.time()
base3, attributes3, event_links3  = find(spec='some spec')
end3 = time.time()
print "Query time to get the a file and all its properties...: ", str((end3-start3)*1000) , ' milliseconds'
