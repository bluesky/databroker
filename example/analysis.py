__author__ = 'arkilic'

from fileStore.api.analysis import find, find_last, find_file_event_link
import time

start = time.time()
base = find_last()
end = time.time()

print "Query time to get the last FileBase insert...: ", str((end-start)*1000) , ' milliseconds'

print base.id

start2 = time.time()
base2 = find_file_event_link(event_id='54b59cf5fa44833081ba8282')
end2 = time.time()
print "Query time to get the FileLink insert...: ", str((end2-start2)*1000) , ' milliseconds'

print base2[0].file_base.id

